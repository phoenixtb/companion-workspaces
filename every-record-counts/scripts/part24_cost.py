"""Part 24: the bill always comes. Meter what the platform does (bytes stored, bytes scanned, CPU, requests), price
it with list prices as inputs, find the biggest lines, and measure three optimisations before and after.

uv run python -m scripts.part24_cost storage              # the lake's bytes: current, history, orphans, owner-less
uv run python -m scripts.part24_cost daily --day D        # one daily run (ingest, dbt build, checks, publish), metered
uv run python -m scripts.part24_cost reads                # the read workload in sql/part24/workload.sql, metered
uv run python -m scripts.part24_cost model [--after]      # units x volume x prices -> a month, unit costs, FOCUS rows
uv run python -m scripts.part24_cost pruning              # optimisation 1: the store dashboard's date filter
uv run python -m scripts.part24_cost incremental --day D  # optimisation 2: the daily report, rebuilt vs incremental
uv run python -m scripts.part24_cost shapes               # which ways of writing a date filter let Trino skip files
uv run python -m scripts.part24_cost sweep [--apply]      # optimisation 3: table folders no table owns

Needs `make up-trino` (Trino as user `lab`, which OPA lets through) and SeaweedFS's metrics port (compose: 9327).
Measurements land in data/lab/part24/*.json; `model` reads them with cost/prices.json and cost/assumptions.json, and
storage from cost/measured/ (written by `sweep --apply`: the bytes it deleted can't be measured again).
"""

import argparse
import json
import re
import statistics
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from scripts import part09_catalog as cat9
from scripts import part17_dbt as p17
from scripts.part18_dbt import cpu, size

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "data/lab/part24"
SQL = ROOT / "sql/part24"
COST = ROOT / "cost"
MEASURED = COST / "measured"  # storage on both sides of the last sweep: the deleted bytes can't be measured again
METRICS = "http://localhost:9327/metrics"
PREFIX = ""  # tests set "test-" so they never overwrite the post's measurements
BUCKETS = ("larkspur-lake", "larkspur-raw")

# ---------- meters: Trino's query statistics and the object store's request counters ----------


def s3_requests() -> Counter:
    """SeaweedFS's S3 request counters for the lab's buckets, summed over response codes, by request type."""
    out = Counter()
    for line in urllib.request.urlopen(METRICS).read().decode().splitlines():
        m = re.match(r'SeaweedFS_s3_request_total\{bucket="([^"]*)",code="\d+",type="(\w+)"\} (\S+)', line)
        if m and m.group(1) in BUCKETS:
            out[m.group(2)] += int(float(m.group(3)))
    return out


def query_stats(q: dict) -> dict:
    s = q["queryStats"]
    return {
        "elapsed_s": cpu(s["elapsedTime"]),
        "cpu_s": cpu(s["totalCpuTime"]),
        "read_bytes": round(size(s["physicalInputDataSize"]) * 2**20),
        "written_bytes": round(size(s["physicalWrittenDataSize"]) * 2**20),
        "peak_mem_bytes": round(size(s["peakUserMemoryReservation"]) * 2**20),
    }


def trino(sql: str, user: str = "lab", session: dict[str, str] | None = None) -> tuple[list, dict]:
    """One statement as `user`: its rows, and what it cost (Trino's statistics + the S3 requests it caused)."""
    before = s3_requests()
    headers = {"X-Trino-User": user}
    if session:
        headers["X-Trino-Session"] = ",".join(f"{k}={v}" for k, v in session.items())
    req = urllib.request.Request(f"{cat9.TRINO}/v1/statement", data=sql.encode(), headers=headers)
    page, rows = json.load(urllib.request.urlopen(req)), []
    qid = page["id"]
    while True:
        rows += page.get("data", [])
        if "error" in page:
            raise SystemExit(f"Trino: {cat9.short(page['error']['message'])}\n  {sql[:300]}")
        if "nextUri" not in page:
            break
        page = json.load(urllib.request.urlopen(page["nextUri"]))
    req = urllib.request.Request(f"{cat9.TRINO}/v1/query/{qid}", headers={"X-Trino-User": user})
    stats = query_stats(json.load(urllib.request.urlopen(req)))
    stats["requests"] = dict(s3_requests() - before)
    return rows, stats


class QueryLog:
    """Collects every query Trino finishes while it runs. Trino keeps finished queries for a short while only
    (query.min-expire-age=2m in the lab, and at most query.max-history), so a long dbt build is polled."""

    def __init__(self) -> None:
        self.seen: dict[str, dict] = {}
        self.since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.poll, daemon=True)

    def collect(self) -> None:
        req = urllib.request.Request(f"{cat9.TRINO}/v1/query", headers={"X-Trino-User": "lab"})
        for q in json.load(urllib.request.urlopen(req)):
            if q["queryStats"]["createTime"] >= self.since and q["state"] in ("FINISHED", "FAILED"):
                self.seen[q["queryId"]] = q

    def poll(self) -> None:
        while not self.stop.wait(10):
            self.collect()

    def __enter__(self) -> "QueryLog":
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop.set()
        self.thread.join()
        self.collect()


def total(stats: list[dict]) -> dict:
    out = {k: sum(s[k] for s in stats) for k in ("elapsed_s", "cpu_s", "read_bytes", "written_bytes")}
    out["queries"] = len(stats)
    return out


def save(name: str, data) -> Path:
    LAB.mkdir(parents=True, exist_ok=True)
    path = LAB / f"{PREFIX}{name}.json"
    path.write_text(json.dumps(data, indent=1, default=str))
    return path


def load(name: str):
    return json.loads((LAB / f"{PREFIX}{name}.json").read_text())


def mib(n: float) -> str:
    return f"{n / 2**20:,.1f}"


def gib(n: float) -> str:
    return f"{n / 2**30:,.2f}"


# ---------- storage: what the lake's bytes are for ----------


def listing(path: str, key: str, tok: str, **params: str) -> list:
    """Every page of one Polaris list call (REST catalog paging: pageToken in, next-page-token out)."""
    out, token = [], None
    while True:
        query = urllib.parse.urlencode({**params, "pageSize": "1000", **({"pageToken": token} if token else {})})
        status, body = cat9.call("GET", f"{path}?{query}", tok=tok)
        if status != 200:
            raise SystemExit(f"Polaris {status} listing {path}: {body}")
        out += body.get(key, [])
        token = body.get("next-page-token")
        if not token:
            return out


def all_namespaces(tok: str, parent: list[str] | None = None) -> list[list[str]]:
    """Every namespace, nested ones included (the lab has none: each is one level)."""
    params = {"parent": "\x1f".join(parent)} if parent else {}
    found = []
    for ns in listing(f"{cat9.CAT}/namespaces", "namespaces", tok, **params):
        found += [ns, *all_namespaces(tok, ns)]
    return found


def catalog_objects() -> tuple[dict[str, str], dict[str, str]]:
    """Every table's and view's folder in the catalog (all namespaces), from Polaris's REST API as root."""
    tok = cat9.token()
    tables, views = {}, {}
    for ns in all_namespaces(tok):
        path = f"{cat9.CAT}/namespaces/{urllib.parse.quote(chr(31).join(ns))}"
        for kind, out in (("tables", tables), ("views", views)):
            for ident in listing(f"{path}/{kind}", "identifiers", tok):
                meta = cat9.call("GET", f"{path}/{kind}/{ident['name']}", tok=tok)[1]
                where = (
                    meta["metadata"]["location"] if kind == "tables" else meta["metadata-location"].rsplit("/", 2)[0]
                )
                out[where.removeprefix("s3://").rstrip("/")] = f"{'.'.join(ns)}.{ident['name']}"
    return tables, views


REFERENCED = [  # each a separate statement: Trino can't list manifests whose writer left out partition summaries
    "SELECT 'current', file_path FROM lakehouse.{ns}.\"{t}$files\"",
    "SELECT DISTINCT 'history', data_file.file_path FROM lakehouse.{ns}.\"{t}$all_entries\"",
    "SELECT 'metadata', path FROM lakehouse.{ns}.\"{t}$all_manifests\"",
    "SELECT 'metadata', manifest_list FROM lakehouse.{ns}.\"{t}$snapshots\"",
    "SELECT 'metadata', file FROM lakehouse.{ns}.\"{t}$metadata_log_entries\"",
]
STORAGE_KINDS = ["current", "history", "metadata", "orphan", "owner-less"]  # a platform namespace's bytes
KINDS = ["current", "history", "metadata", "orphan", "owner-less", "other lab files", "raw zone files"]


def tolerated(sql: str, error: str) -> bool:
    """The one reference-query failure the lab expects: Part 1's DuckDB-written table has manifests without partition
    summaries, which Trino's $all_manifests can't list. Anything else (an OPA denial, a missing table) is not known."""
    return "$all_manifests" in sql and "partitionSummaries" in error


def history_paths(name: str) -> set[str]:
    """Every data and delete file any snapshot's manifests list (deleted entries included), read with PyIceberg: the
    fallback for a table whose $all_entries Trino can't read. Part 8's DuckDB-written position delete files carry
    bounds for `file_path` but none for `pos`, and Trino 484 fails on them with "delete file pos is null"."""
    table = cat9.pyiceberg("root", vended=False).load_table(name)
    return {
        entry.data_file.file_path
        for snap in table.snapshots()
        for manifest in snap.manifests(table.io)
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False)
    }


def bucket_objects() -> list[tuple[str, int, datetime]]:
    fs = cat9.lake_fs()
    fs.invalidate_cache()
    return [(k, v["size"], v["LastModified"]) for b in BUCKETS for k, v in fs.find(f"{b}/", detail=True).items()]


def storage(label: str = "storage") -> dict:
    """Classify every object in both buckets: a live table's current files, its history (files only older snapshots
    use), its metadata, its orphans (in its folder, used by nothing), owner-less table folders (no table or view in the
    catalog owns them), other lab files (Season 1's Parquet, Delta and DuckLake demos; dlt's state) and the raw zone.
    A reference query that fails for an unknown reason is recorded in `failures`: `sweep` then refuses to delete."""
    tables, views = catalog_objects()
    referenced: dict[str, str] = {}
    unlisted, failures = set(), []  # tables whose manifests Trino couldn't list: their metadata/ counts as metadata
    fallbacks = []  # tables whose history Trino couldn't read, read with PyIceberg instead
    for loc, name in tables.items():
        ns, t = name.rsplit(".", 1)
        for sql in REFERENCED:
            try:
                rows, _ = trino(sql.format(ns=ns, t=t))
            except SystemExit as e:
                error = str(e).splitlines()[0]
                if tolerated(sql, error):
                    unlisted.add(loc)
                    continue
                if "$all_entries" not in sql:
                    failures.append(f"{name}: {error}")
                    continue
                try:
                    rows = [("history", p) for p in history_paths(name)]
                    fallbacks.append(f"{name} ({error.removeprefix('Trino: ')})")
                except Exception as e2:  # noqa: BLE001 - any failure here means the table's history is unknown
                    failures.append(f"{name}: {error}; PyIceberg: {e2}")
                    continue
            for kind, path in rows:  # a current file also appears in $all_entries: keep the first label
                referenced.setdefault(path.removeprefix("s3://"), kind)
    by_kind, objects = Counter(), Counter()
    per_ns: dict[str, Counter] = defaultdict(Counter)
    ownerless: dict[str, list] = defaultdict(lambda: [0, 0, None])  # folder -> bytes, objects, newest write
    for key, n, modified in bucket_objects():
        parts = key.split("/")
        folder = "/".join(parts[:4])
        if parts[0] == "larkspur-raw":
            kind = "raw zone files"
        elif parts[1] != "warehouse" or parts[3].startswith("_") or parts[3:] == ["init"]:
            kind = "other lab files"  # also dlt's state: _dlt_* folders and an `init` marker per dataset (Part 15)
        elif folder in views:
            kind = "metadata"
        elif folder in tables:
            fallback = "metadata" if folder in unlisted and "/metadata/" in key else "orphan"
            kind = referenced.get(key, fallback)
        elif any(p.startswith(folder + "/") for p in referenced):  # used by a table that lives elsewhere: keep
            kind = referenced.get(key, "orphan")
        else:
            kind = "owner-less"
            f = ownerless[folder]
            f[0], f[1], f[2] = f[0] + n, f[1] + 1, max(f[2] or modified, modified)
        by_kind[kind] += n
        objects[kind] += 1
        if parts[0] == "larkspur-lake" and parts[1] == "warehouse":
            per_ns[parts[2]][kind] += n
    platform = json.loads((COST / "assumptions.json").read_text())["platform_namespaces"]
    plat = Counter()
    for ns in platform:
        plat.update(per_ns.get(ns, {}))
    out = {
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "tables": len(tables),
        "views": len(views),
        "bytes": dict(by_kind),
        "objects": dict(objects),
        "platform": {k: plat[k] for k in STORAGE_KINDS},
        "per_namespace": {ns: dict(c) for ns, c in per_ns.items()},
        "ownerless_folders": {k: [b, n, str(m)] for k, (b, n, m) in ownerless.items()},
        "failures": failures,
        "history_read_with_pyiceberg": fallbacks,
    }
    print(f"{len(tables)} tables and {len(views)} views in the catalog; both buckets:")
    print(f"{'kind':<17} {'GiB':>7} {'share':>6} {'objects':>8}")
    whole = sum(by_kind.values())
    for kind in KINDS:
        print(f"{kind:<17} {gib(by_kind[kind]):>7} {by_kind[kind] / whole:>6.1%} {objects[kind]:>8,}")
    print(f"{'total':<17} {gib(whole):>7} {'':>6} {sum(objects.values()):>8,}")
    print(f"of which Larkspur's platform ({', '.join(platform)}): "
          + ", ".join(f"{k} {gib(plat[k])}" for k in STORAGE_KINDS) + " GiB")  # fmt: skip
    worst = sorted(per_ns.items(), key=lambda x: -x[1]["owner-less"])[:3]
    print("owner-less by namespace: " + ", ".join(f"{ns} {gib(c['owner-less'])} GiB" for ns, c in worst))
    print(f"owner-less table folders: {len(ownerless)}")
    for f in fallbacks:
        print(f"history read with PyIceberg, Trino failed: {f}")
    for f in failures:
        print(f"reference query failed: {f}")
    save(label, out)
    return out


# ---------- one daily run, metered ----------

TARGET = re.compile(
    r"^\s*(?:create\s+(?:or\s+replace\s+)?(?:table|view)|insert\s+into|delete\s+from|merge\s+into|alter\s+table)"
    r'\s+"?\w+"?\."?\w+"?\."?(\w+?)(?:__dbt_tmp\w*)?"?[\s(]',
    re.I,
)


def owner(sql: str) -> str:
    """Which model a dbt statement works for: the table it writes, or `tests` / `dbt overhead`."""
    body = re.sub(r"^\s*/\*.*?\*/", "", sql, flags=re.S)  # dbt's query comment
    if "dbt_internal_test" in body:
        return "tests"
    m = TARGET.match(body)
    return m.group(1) if m else "dbt overhead"


def step(name: str, fn, *args) -> dict:
    """Run one pipeline step; meter its wall time, the Trino queries it caused and its S3 requests."""
    before = s3_requests()
    started = time.monotonic()
    with QueryLog() as log:
        fn(*args)
    out = {"step": name, "wall_s": round(time.monotonic() - started, 1), "requests": dict(s3_requests() - before)}
    stats = [(q["query"], query_stats(q)) for q in log.seen.values()]
    out["trino"] = total([s for _, s in stats])
    per = defaultdict(list)
    for sql, s in stats:
        per[owner(sql)].append(s)
    out["per_owner"] = {k: total(v) for k, v in per.items()}
    biggest = sorted(stats, key=lambda x: -x[1]["read_bytes"])[:5]
    out["biggest"] = [{"sql": " ".join(q.split())[-160:], **s} for q, s in biggest]
    return out


def daily(day: date, label: str = "daily") -> dict:
    """Part 19's daily run for one upload day, step by step, as Airflow runs it (maintenance is a separate DAG)."""
    from scripts import part19_pipeline as p19

    steps = [
        step("ingest", p19.ingest, day),
        step("dbt build", lambda d: p19.build(d) == 0 or sys.exit("dbt build failed"), day),
        step("checks", p19.checks, day),
        step("publish", p19.publish, day),
    ]
    print(f"\nupload day {day}")
    print(
        f"{'step':<12} {'wall s':>7} {'Trino CPU s':>12} {'read MiB':>9} {'written MiB':>12} {'queries':>8} {'GET':>6} "
        f"{'PUT':>5} {'LIST':>5}"
    )
    for s in steps:
        t, r = s["trino"], s["requests"]
        print(
            f"{s['step']:<12} {s['wall_s']:>7.1f} {t['cpu_s']:>12.1f} {mib(t['read_bytes']):>9} "
            f"{mib(t['written_bytes']):>12} {t['queries']:>8} {r.get('GET', 0) + r.get('HEAD', 0):>6} "
            f"{r.get('PUT', 0) + r.get('POST', 0):>5} {r.get('LIST', 0):>5}"
        )
    build = steps[1]["per_owner"]
    print("\ndbt build, per model:")
    print(f"{'model':<26} {'CPU s':>7} {'read MiB':>9} {'written MiB':>12} {'queries':>8}")
    for name, t in sorted(build.items(), key=lambda x: -x[1]["cpu_s"]):
        print(
            f"{name:<26} {t['cpu_s']:>7.1f} {mib(t['read_bytes']):>9} {mib(t['written_bytes']):>12} {t['queries']:>8}"
        )
    out = {"day": str(day), "steps": steps}
    save(label, out)
    return out


# ---------- the read workload ----------


def workload(path: Path = SQL / "workload.sql") -> dict[str, str]:
    names = re.findall(r"^-- (\w+):", path.read_text(), flags=re.M)
    return dict(zip(names, p17.statements(path), strict=True))


def measure(sql: str, repeat: int = 3, session: dict[str, str] | None = None) -> tuple[list, dict]:
    """Run a query `repeat` times; per metric, the median (the first run pays for cold caches)."""
    runs = [trino(sql, session=session) for _ in range(repeat)]
    stats = [s for _, s in runs]
    med = {k: statistics.median(s[k] for s in stats) for k in stats[0] if k != "requests"}
    kinds = {k for s in stats for k in s["requests"]}
    med["requests"] = {k: statistics.median(s["requests"].get(k, 0) for s in stats) for k in kinds}
    med["gets_per_run"] = [s["requests"].get("GET", 0) + s["requests"].get("HEAD", 0) for s in stats]
    return runs[0][0], med


def show_reads(results: dict[str, dict]) -> None:
    print(f"{'query':<20} {'seconds':>8} {'CPU s':>6} {'read MiB':>9} {'GETs':>5} {'peak MiB':>9}")
    for name, s in results.items():
        gets = s["requests"].get("GET", 0) + s["requests"].get("HEAD", 0)
        print(
            f"{name:<20} {s['elapsed_s']:>8.2f} {s['cpu_s']:>6.2f} {mib(s['read_bytes']):>9} {gets:>5.0f} "
            f"{mib(s['peak_mem_bytes']):>9}"
        )


def reads(label: str = "reads") -> dict:
    out = {name: measure(sql)[1] for name, sql in workload().items()}
    show_reads(out)
    save(label, out)
    return out


# ---------- optimisation 1: let the date filter prune ----------

LOCAL_DATE = "date(f.event_ts AT TIME ZONE 'Europe/London') BETWEEN DATE '2026-09-24' AND DATE '2026-09-30'"
DATE_KEY = "f.date_key BETWEEN 20260924 AND 20260930"  # the modelled local business date (Parts 4-5)


def pruning() -> dict:
    """The UK store tile as written (a function of event_ts: no file can be skipped) and on date_key, the column the
    model already holds the London business date in (its min/max per file let Trino skip the other days' files)."""
    before_sql = workload()["uk_store_week"]
    after_sql = before_sql.replace(LOCAL_DATE, DATE_KEY)
    assert after_sql != before_sql
    (rows_b, before), (rows_a, after) = measure(before_sql), measure(after_sql)
    # Trino reads a Parquet file under parquet.small-file-threshold (3 MB) whole; at 0B it reads only the columns
    # the query needs, a closer stand-in for production's bigger files (Step 4's caveat)
    _, columns_only = measure(before_sql, session={"lakehouse.parquet_small_file_threshold": "0B"})
    files = trino('SELECT count(*) FROM lakehouse.gold."fct_sales_line$files"')[0][0][0]
    show_reads({"local date of ts": before, "  same, threshold 0B": columns_only, "date_key": after})
    same = sorted(map(tuple, rows_b)) == sorted(map(tuple, rows_a))
    print(
        f"same answer: {same} ({len(rows_a)} stores, {sum(r[1] for r in rows_a):,} lines); "
        f"fct_sales_line has {files} data files"
    )
    print(f"GETs per run, first run first: as written {before['gets_per_run']}, on date_key {after['gets_per_run']}")
    out = {"before": before, "after": after, "columns_only": columns_only, "same": same, "stores": len(rows_a)}
    out["files"] = files
    save("opt1-pruning", out)
    return out


SHAPES = {  # the same seven London days, said six ways
    "date_key BETWEEN": DATE_KEY,
    "event_ts range (UTC)": "f.event_ts >= TIMESTAMP '2026-09-23 23:00:00 UTC' "
    "AND f.event_ts < TIMESTAMP '2026-09-30 23:00:00 UTC'",
    "date(event_ts)": "date(f.event_ts) BETWEEN DATE '2026-09-24' AND DATE '2026-09-30'",
    "date(event_ts AT TIME ZONE)": LOCAL_DATE,
    "CAST(date_key AS varchar)": "CAST(f.date_key AS varchar) BETWEEN '20260924' AND '20260930'",
    "date_key IN (dim_date subquery)": "f.date_key IN (SELECT date_key FROM lakehouse.gold.dim_date "
    "WHERE \"date\" BETWEEN DATE '2026-09-24' AND DATE '2026-09-30')",
}


def shapes() -> dict:
    """Which ways of writing a date filter let Trino skip fct_sales_line's files (one file per UTC day). The UTC
    range is the same London week (BST is UTC+1); date(event_ts) is a different week (UTC days). The point: bytes."""
    base = workload()["uk_store_week"]
    out = {name: measure(base.replace(LOCAL_DATE, pred))[1] for name, pred in SHAPES.items()}
    alone = """SELECT f.store_key, sum(f.amount_minor) FROM lakehouse.gold.fct_sales_line f
               WHERE date(f.event_ts) BETWEEN DATE '2026-09-24' AND DATE '2026-09-30' GROUP BY 1"""
    out["date(event_ts), fact alone"] = measure(alone)[1]  # the same predicate without the join to dim_store
    print(f"{'predicate':<30} {'read MiB':>9} {'CPU s':>6}")
    for name, s_ in out.items():
        print(f"{name:<30} {mib(s_['read_bytes']):>9} {s_['cpu_s']:>6.2f}")
    save("shapes", out)
    return out


# ---------- optimisation 2: pay for the days that changed ----------

INCR = "rpt_daily_net_revenue_incr"
PROD_REPORT = "lakehouse.gold.rpt_daily_net_revenue_v2"
DEV_REPORT = f"lakehouse_dev.dev.{INCR}"


def dbt_step(name: str, *args: str) -> dict:
    def run() -> None:
        r = p17.dbt(*args, echo=False)
        if r.returncode:
            raise SystemExit(f"dbt {' '.join(args)} failed:\n{r.stdout[-3000:]}")

    return step(name, run)


def report_diff() -> list:
    """Rows of the dev report that differ from production's (both directions), with production's value."""
    cols = "country, business_date, sales_minor, refunds_minor, net_minor, is_provisional"
    return trino(f"""
        SELECT d.country, d.business_date, d.net_minor, p.net_minor
        FROM (SELECT {cols} FROM {DEV_REPORT} EXCEPT SELECT {cols} FROM {PROD_REPORT}) d
        LEFT JOIN {PROD_REPORT} p ON p.country = d.country AND p.business_date = d.business_date
        UNION ALL
        SELECT p.country, p.business_date, NULL, p.net_minor
        FROM (SELECT {cols} FROM {PROD_REPORT} EXCEPT SELECT {cols} FROM {DEV_REPORT}) p
        WHERE NOT EXISTS (SELECT 1 FROM {DEV_REPORT} d
                          WHERE d.country = p.country AND d.business_date = p.business_date)
        ORDER BY 2, 1""")[0]


def incremental(day: date) -> dict:
    """The daily run's two biggest lines, each before and after: the report rebuilt from all history vs the incremental
    variant (dev), and fct_sales_line's four data tests over every row vs over the days the run rebuilt (prod, read
    only). The incremental run is proven on a damaged copy: every row is first moved by +1; the run must put back
    exactly the window's rows and leave the rest alone."""
    from scripts.part18_dbt import STATE

    start, end = day - timedelta(days=1), day + timedelta(days=1)  # Part 19: UTC days D-1 and D
    lab_vars = json.dumps({"part24_lab": True, "run_start": str(start), "run_end": str(end)})
    dev = ["--target", "dev", "--defer", "--state", str(STATE), "--favor-state", "--vars", lab_vars, "--select", INCR]
    out = {"day": str(day)}
    out["report_rebuilt"] = dbt_step("report: rebuild", "run", *dev, "--full-refresh")
    out["diff_after_rebuild"] = report_diff()
    rows = trino(f"SELECT count(*) FROM {DEV_REPORT}")[0][0][0]
    trino(f"UPDATE {DEV_REPORT} SET net_minor = net_minor + 1", user="dbt_dev")  # damage every row
    out["report_incremental"] = dbt_step("report: incremental", "run", *dev)
    diff = report_diff()
    lo = start - timedelta(days=3)
    outside = [r for r in diff if not lo <= date.fromisoformat(r[1]) <= end]
    out["incremental_check"] = {"rows": rows, "still_damaged": len(diff), "outside_window": len(outside)}
    tests = ["--target", "prod", "--select", "fct_sales_line,test_type:data"]
    out["tests_all"] = dbt_step("tests: every row", "test", *tests)
    out["tests_window"] = dbt_step(
        "tests: run window", "test", *tests, "--vars", json.dumps({"test_since": str(start)})
    )
    print(f"\nupload day {day}: run window UTC {start} .. {day}; report rows re-derived for {lo} .. {end}")
    print(f"{'':<22} {'wall s':>7} {'Trino CPU s':>12} {'read MiB':>9} {'queries':>8} {'GETs':>6}")
    for key in ("report_rebuilt", "report_incremental", "tests_all", "tests_window"):
        s, t = out[key], out[key]["trino"]
        gets = s["requests"].get("GET", 0) + s["requests"].get("HEAD", 0)
        print(
            f"{s['step']:<22} {s['wall_s']:>7.1f} {t['cpu_s']:>12.1f} {mib(t['read_bytes']):>9} {t['queries']:>8} "
            f"{gets:>6}"
        )
    print(f"rebuild vs production's report: {len(out['diff_after_rebuild'])} rows differ")
    c = out["incremental_check"]
    print(
        f"after damaging all {c['rows']} rows and one incremental run: {c['still_damaged']} rows still differ, "
        f"{c['outside_window']} of them outside the window"
    )
    save("opt2-incremental", out)
    return out


# ---------- optimisation 3: storage nobody owns ----------

# A folder written within the grace period may be a table being created right now (files before the catalog commit).
# The lab uses an hour; production wants days, longer than its slowest write (Trino's remove_orphan_files: 7d).
GRACE = timedelta(hours=1)


def docker_disk() -> str:
    """Free space on the Docker VM's disk, as SeaweedFS's container sees it."""
    import subprocess

    out = subprocess.run(["docker", "exec", "every-record-counts-seaweedfs-1", "df", "-h", "/data"],
                         capture_output=True, text=True).stdout.splitlines()  # fmt: skip
    size_, used, avail = out[-1].split()[1:4]
    return f"{avail} free of {size_}"


def sweep(apply: bool = False, grace: timedelta = GRACE) -> dict:
    """Delete table folders that no table or view in this catalog owns, that hold no file any live table references,
    and that nobody wrote to within `grace`: remove_orphan_files' reachability check, applied to the whole warehouse
    prefix. Safe only if this catalog alone writes under it. Without --apply, only lists. Refuses to delete if any
    reference query failed for an unknown reason: an unread table's files would look owner-less."""
    before = storage("storage-before-sweep")
    if before["failures"] and apply:
        raise SystemExit(f"not sweeping: {len(before['failures'])} reference queries failed (listed above)")
    now = datetime.now(UTC)
    folders = before["ownerless_folders"]
    quiet = {k: v for k, v in folders.items() if now - datetime.fromisoformat(v[2]) > grace}
    groups, counts = Counter(), Counter()
    for k, (b, _, _) in quiet.items():
        ns, folder = k.split("/")[2:4]
        name = f"{ns}/{re.sub(r'-[0-9a-f]{32}$', '', folder)}"  # table folders end in a UUID
        groups[name] += b
        counts[name] += 1
    print(f"\n{len(quiet)} of {len(folders)} owner-less folders untouched for over {grace}; the biggest:")
    for name, b in groups.most_common(5):
        print(f"  {name:<36} {gib(b):>6} GiB in {counts[name]} folders")
    out = {"folders": len(quiet), "bytes": sum(v[0] for v in quiet.values())}
    out["objects"] = sum(v[1] for v in quiet.values())
    if not apply:
        print("dry run: nothing deleted (--apply deletes)")
        return out
    disk = docker_disk()
    fs = cat9.lake_fs()
    for folder in quiet:
        fs.rm(folder, recursive=True)
    print(f"deleted {out['folders']} folders: {out['objects']:,} objects, {gib(out['bytes'])} GiB\n")
    out["after"] = storage("storage-after-sweep")
    from scripts import part23_privacy as privacy

    print()
    privacy.vacuum()  # deleted objects leave SeaweedFS's disk only when their volumes are compacted (Part 23)
    out["disk"] = {"before": disk, "after": docker_disk()}
    print(f"Docker disk: {out['disk']['before']} before, {out['disk']['after']} after")
    save("opt3-sweep", out)
    MEASURED.mkdir(exist_ok=True)  # what the model prices as storage before and after (nothing re-measures them)
    for name, data in (("storage-before-sweep", before), ("storage-after-sweep", out["after"])):
        (MEASURED / f"{name}.json").write_text(json.dumps(data, indent=1, default=str))
    return out


# ---------- the cost model ----------

PUT_LIKE = ("PUT", "POST", "COPY", "LIST")  # S3's PUT/COPY/POST/LIST price; GET, HEAD and other requests the GET price


def inputs(after: bool) -> dict:
    """The measured units, before or after the three optimisations (each replaced by its own after-measurement)."""
    store = json.loads((MEASURED / f"storage-{'after' if after else 'before'}-sweep.json").read_text())
    daily, reads_ = load("daily"), load("reads")
    run = {"cpu_s": 0.0, "read_bytes": 0, "queries": 0, "requests": Counter()}
    for st in daily["steps"]:
        run["cpu_s"] += st["trino"]["cpu_s"]
        run["read_bytes"] += st["trino"]["read_bytes"]
        run["queries"] += st["trino"]["queries"]
        run["requests"].update(st["requests"])
    if after:
        opt1, opt2 = load("opt1-pruning"), load("opt2-incremental")
        reads_["uk_store_week"] = opt1["after"]
        for old, new in (("report_rebuilt", "report_incremental"), ("tests_all", "tests_window")):
            o, n = opt2[old], opt2[new]
            run["cpu_s"] += n["trino"]["cpu_s"] - o["trino"]["cpu_s"]
            run["read_bytes"] += n["trino"]["read_bytes"] - o["trino"]["read_bytes"]
            run["queries"] += n["trino"]["queries"] - o["trino"]["queries"]
            run["requests"].update(n["requests"])
            run["requests"].subtract(o["requests"])
    return {"storage": store, "run": run, "reads": reads_}


def priced_requests(req: dict, prices: dict) -> tuple[float, float]:
    put = sum(v for k, v in req.items() if k in PUT_LIKE)
    get = sum(v for k, v in req.items() if k not in PUT_LIKE and k != "DELETE")
    return get * prices["request_get"]["usd"] + put * prices["request_put"]["usd"], get + put


def model(after: bool = False, prices: dict | None = None, write: bool = True) -> dict:
    """A month of Larkspur's platform at production volume: measured units x assumptions x list prices. Prints the bill
    by line, unit costs and a provisioned-cluster comparison; writes the lines as FOCUS-shaped rows."""
    import duckdb
    import pyarrow as pa

    prices = prices or json.loads((COST / "prices.json").read_text())["prices"]
    a = json.loads((COST / "assumptions.json").read_text())
    scale, days = a["volume_scale"], a["days_per_month"]
    m = inputs(after)
    lines = []  # (line, service category, quantity, unit, usd, tags)

    def scan_usd(read_bytes: float, queries: float) -> tuple[float, float]:
        tb = max(read_bytes * scale, queries * prices["scan_tb"]["minimum_mb_per_query"] * 1e6) / 1e12
        return tb, tb * prices["scan_tb"]["usd"]

    for kind in STORAGE_KINDS:
        gb = m["storage"]["platform"][kind] * scale / 1e9
        lines.append((f"storage: {kind}", "Storage", gb, "GB-Months", gb * prices["storage_gb_month"]["usd"],
                      {"kind": kind}))  # fmt: skip
    gb = m["storage"]["bytes"].get("raw zone files", 0) * scale / 1e9
    lines.append(("storage: raw zone files", "Storage", gb, "GB-Months", gb * prices["storage_gb_month"]["usd"],
                  {"kind": "raw zone"}))  # fmt: skip
    run = m["run"]
    tb, usd = scan_usd(run["read_bytes"], run["queries"])
    product = a["data_product"]
    runs = a["daily_runs_per_day"] * days
    lines.append(("scan: daily run", "Analytics", tb * runs, "TB", usd * runs, {"data_product": product["daily run"]}))
    usd, n = priced_requests(run["requests"], prices)
    lines.append(("requests: daily run", "Storage", n * runs, "Requests", usd * runs,
                  {"data_product": product["daily run"]}))  # fmt: skip
    for name, st in m["reads"].items():
        k = a["reads_per_day"][name] * days
        tb, usd = scan_usd(st["read_bytes"], 1)
        lines.append((f"scan: {name}", "Analytics", tb * k, "TB", usd * k, {"data_product": product[name]}))
        usd, n = priced_requests(st["requests"], prices)
        lines.append((f"requests: {name}", "Storage", n * k, "Requests", usd * k, {"data_product": product[name]}))
    bill = sum(x[4] for x in lines)
    head = "after the three optimisations" if after else "as measured"
    print(
        f"one month at {scale}x the lab's volume, {head}; list prices checked {COST.name}/prices.json "
        f"({json.loads((COST / 'prices.json').read_text())['checked']})"
    )
    print(f"{'line':<28} {'quantity':>14} {'unit':<10} {'USD':>10} {'share':>6}")
    for line, _, q, unit, usd, _ in sorted(lines, key=lambda x: -x[4]):
        print(f"{line:<28} {q:>14,.1f} {unit:<10} {usd:>10,.2f} {usd / bill:>6.1%}")
    print(f"{'total':<28} {'':>14} {'':<10} {bill:>10,.2f}")

    cpu_h = (run["cpu_s"] * a["daily_runs_per_day"] + sum(
        st["cpu_s"] * a["reads_per_day"][n] for n, st in m["reads"].items())) * scale * days / 3600  # fmt: skip
    c = a["provisioned_cluster"]
    hourly = c["vcpu"] * prices["vcpu_hour"]["usd"] + c["memory_gb"] * prices["gb_hour"]["usd"]
    always = hourly * c["hours_per_month"]
    busy = cpu_h / c["vcpu"]
    print(
        f"\nthe same work as CPU: {cpu_h:,.1f} vCPU-hours a month. A {c['vcpu']} vCPU / {c['memory_gb']} GB cluster "
        f"always on: USD {always:,.2f}; busy {busy / c['hours_per_month']:.1%} of its hours "
        f"(USD {hourly * busy:,.2f} if it ran only then)"
    )
    storage_usd = sum(x[4] for x in lines if x[0].startswith("storage"))
    live_gb = next(x[2] for x in lines if x[0] == "storage: current")
    run_usd = sum(x[4] for x in lines if x[0].endswith("daily run")) / runs
    print(
        f"unit costs: one daily run USD {run_usd:,.4f}; storage per live GB-month USD {storage_usd / live_gb:,.4f} "
        f"(list price {prices['storage_gb_month']['usd']})"
    )
    for name in m["reads"]:
        per = sum(x[4] for x in lines if x[0].endswith(name)) / (a["reads_per_day"][name] * days)
        print(f"  one {name} view: USD {per:,.6f}")

    out = {"after": after, "total_usd": bill, "lines": lines, "cpu_hours": cpu_h, "always_on_usd": always,
           "busy_share": busy / c["hours_per_month"], "run_usd": run_usd}  # fmt: skip
    if not write:
        return out
    start, end = date(2026, 10, 1), date(2026, 11, 1)
    rows = [{
        "BillingPeriodStart": start, "BillingPeriodEnd": end, "ChargePeriodStart": start, "ChargePeriodEnd": end,
        "ChargeCategory": "Usage", "ChargeDescription": line, "ServiceProviderName": "Larkspur Data Platform",
        "ServiceName": "Lakehouse storage" if cat == "Storage" else "Lakehouse SQL", "ServiceCategory": cat,
        "ResourceId": line.split(": ", 1)[1], "ConsumedQuantity": q, "ConsumedUnit": unit, "PricingQuantity": q,
        "PricingUnit": unit, "ListCost": usd, "ContractedCost": usd, "BilledCost": usd, "EffectiveCost": usd,
        "BillingCurrency": "USD", "Tags": json.dumps(tags), "x_ModelLabel": "after" if after else "before",
    } for line, cat, q, unit, usd, tags in lines]  # fmt: skip
    path = LAB / f"{PREFIX}focus-{'after' if after else 'before'}.parquet"
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows), path)
    print(f"\n{len(rows)} FOCUS-shaped rows in {path.relative_to(ROOT)}; cost by data product:")
    duckdb.sql((SQL / "focus_by_product.sql").read_text().replace("{path}", str(path))).show()
    save(f"model-{'after' if after else 'before'}", out)
    return out


# ---------- main ----------


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("storage")
    a.add_argument("--label", default="storage")
    a = sub.add_parser("daily")
    a.add_argument("--day", type=date.fromisoformat, default=date(2026, 10, 4))
    a.add_argument("--label", default="daily")
    a = sub.add_parser("reads")
    a.add_argument("--label", default="reads")
    sub.add_parser("pruning")
    sub.add_parser("shapes")
    a = sub.add_parser("incremental")
    a.add_argument("--day", type=date.fromisoformat, default=date(2026, 10, 4))
    a = sub.add_parser("model")
    a.add_argument("--after", action="store_true", help="with the three optimisations' after-measurements")
    a = sub.add_parser("sweep")
    a.add_argument("--apply", action="store_true")
    a.add_argument("--grace-hours", type=float, default=GRACE.total_seconds() / 3600)
    args = p.parse_args()
    if args.cmd == "storage":
        storage(args.label)
    elif args.cmd == "daily":
        daily(args.day, args.label)
    elif args.cmd == "reads":
        reads(args.label)
    elif args.cmd == "pruning":
        pruning()
    elif args.cmd == "shapes":
        shapes()
    elif args.cmd == "incremental":
        incremental(args.day)
    elif args.cmd == "model":
        model(args.after)
    elif args.cmd == "sweep":
        sweep(args.apply, timedelta(hours=args.grace_hours))


if __name__ == "__main__":
    sys.exit(main())
