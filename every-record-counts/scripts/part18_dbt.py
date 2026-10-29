"""Part 18: dbt at scale. Incremental sales facts that absorb late till uploads, a dev namespace for another principal,
local slim CI against production's manifest, a model contract and a model version.

uv run python -m scripts.part18_dbt setup            # namespace `dev`, principal `developer`, its grants (Polaris)
uv run python -m scripts.part18_dbt prod-state       # production's manifest, as the last deploy left it
uv run python -m scripts.part18_dbt replay           # till uploads arrive day by day; dev builds the fact variants
uv run python -m scripts.part18_dbt compare          # each variant vs a full rebuild, row for row
uv run python -m scripts.part18_dbt late             # one late basket: where each variant put it
uv run python -m scripts.part18_dbt ci               # slim CI: state:modified+, deferred to production
uv run python -m scripts.part18_dbt contract         # what an enforced contract stops on Trino, and what it doesn't

Needs `make up-trino`, Part 17's models in production (`make dbt-lake`) and Part 16's CDC replicas.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa

from scripts import part09_catalog as cat9
from scripts import part17_dbt as p17

ROOT = Path(__file__).resolve().parents[1]
PROJECT = p17.PROJECT
STATE = PROJECT / "state/prod"  # production's artifacts; in a real setup, downloaded from the last deploy job
LAB = ROOT / "data/lab/part18"

# ---------- Polaris: a namespace and a principal for development and CI ----------

DEV_NS = "dev"
DEVELOPER = ("developer", "developer-lab-secret")
READ_ONLY = ["TABLE_LIST", "TABLE_READ_DATA", "NAMESPACE_READ_PROPERTIES", "TABLE_READ_PROPERTIES"]
DEV_GRANTS = {  # namespace -> privileges; no write on silver or gold, so no dev build can drop a production table
    DEV_NS: [*cat9.WRITE, *p17.TRANSFORMER_GRANTS],
    "raw": READ_ONLY,
    "silver": READ_ONLY,  # Part 21: POS staging reads silver.pos_event_dispositions
}


def setup() -> None:
    tok = cat9.token()
    mgmt, catalog, cat = cat9.MGMT, cat9.CATALOG, cat9.CAT
    cat9.ok(cat9.call("POST", f"{cat}/namespaces", {"namespace": [DEV_NS]}, tok=tok)[0], "namespace", (200, 409))
    name, secret = DEVELOPER
    cat9.ok(cat9.call("POST", f"{mgmt}/principals", {"principal": {"name": name}}, tok=tok)[0], name, (201, 409))
    body = {"clientId": name, "clientSecret": secret}
    cat9.ok(cat9.call("POST", f"{mgmt}/principals/{name}/reset", body, tok=tok)[0], "reset")
    role = "dev_builder"
    body = {"catalogRole": {"name": role}}
    cat9.ok(cat9.call("POST", f"{mgmt}/catalogs/{catalog}/catalog-roles", body, tok=tok)[0], role, (201, 409))
    grants = [{"type": "catalog", "privilege": "NAMESPACE_LIST"}] + [
        {"type": "namespace", "namespace": [ns], "privilege": p} for ns, privs in DEV_GRANTS.items() for p in privs
    ]
    for grant in grants:
        path = f"{mgmt}/catalogs/{catalog}/catalog-roles/{role}/grants"
        cat9.ok(cat9.call("PUT", path, {"grant": grant}, tok=tok)[0], f"grant {grant['privilege']}")
    body = {"principalRole": {"name": "developers"}}
    cat9.ok(cat9.call("POST", f"{mgmt}/principal-roles", body, tok=tok)[0], "principal role", (201, 409))
    body = {"catalogRole": {"name": role}}
    cat9.ok(cat9.call("PUT", f"{mgmt}/principal-roles/developers/catalog-roles/{catalog}", body, tok=tok)[0], "assign")
    body = {"principalRole": {"name": "developers"}}
    cat9.ok(cat9.call("PUT", f"{mgmt}/principals/{name}/principal-roles", body, tok=tok)[0], "assign principal")
    print(f"namespace {DEV_NS}; principal {name} → principal role developers → catalog role {role}:")
    for where, privs in cat9.grants_by_securable(role, tok).items():
        print(f"  {where:<17} {', '.join(privs)}")


# ---------- production's state ----------

PROD_TAG = "every-record-counts/part-17"  # the code production runs until Part 18 is deployed


def dbt(*args: str, echo: bool = True) -> subprocess.CompletedProcess:
    return p17.dbt(*args, echo=echo)


def prod_state(tag: str | None = PROD_TAG) -> None:
    """Write production's manifest to dbt/larkspur/state/prod. A real CI job downloads it from the last deploy; here,
    `tag` names the code production runs (parsed in a temporary checkout), or None for the working tree (a deploy)."""
    if STATE.exists():
        shutil.rmtree(STATE)
    if tag is None:
        run = dbt("parse", "--target", "prod", "--target-path", str(STATE), echo=False)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            folder = "every-record-counts/dbt/larkspur"
            archive = subprocess.run(
                ["git", "-C", str(ROOT.parent), "archive", tag, folder], check=True, capture_output=True
            ).stdout
            subprocess.run(["tar", "-x", "-C", tmp], input=archive, check=True)
            project = Path(tmp) / folder
            (project / "dbt_packages").symlink_to(PROJECT / "dbt_packages")
            target = "lake" if "target: lake" in (project / "profiles.yml").read_text() else "prod"
            run = p17.dbt("parse", "--target", target, "--target-path", str(STATE), project=project, echo=False)
    if run.returncode:
        raise SystemExit(run.stdout[-2000:])
    nodes = json.loads((STATE / "manifest.json").read_text())["nodes"]
    kinds: dict[str, int] = {}
    for n in nodes.values():
        kinds[n["resource_type"]] = kinds.get(n["resource_type"], 0) + 1
    print(f"state/prod/manifest.json from {tag or 'the working tree'}: {len(nodes)} nodes {kinds}")


# ---------- the replay: till uploads arrive one UTC day at a time ----------

CUTOFF = date(2026, 9, 20)  # uploads up to here are landed before the first build
LAST_UPLOAD = date(2026, 10, 5)
RUN_AT = timedelta(days=1, minutes=30)  # each run starts at 00:30 UTC after its upload day closes
VARIANTS = ["fct_sales_line_naive", "fct_sales_line_lookback1", "fct_sales_line"]
DEV_VIEWS = ["stg_pos_sale_lines", "stg_online_sale_lines", "int_sales_lines"]  # what the facts read, rebuilt in dev
POS = "SELECT * EXCLUDE (schema_version) FROM raw_pos_events WHERE {where} ORDER BY uploaded_at"


def empty_dev(names: list[str]) -> None:
    """Drop tables and views in `dev`. Tables go through the catalog without a purge, their files removed by
    cat9.drop_table: a Trino DROP TABLE asks Polaris to purge, and purging tables with hundreds of snapshots in
    Polaris's background tasks exhausted its S3 connection pool in the lab (Troubleshooting in the post). Part 24 found
    the files weren't removed (drop_table looked for a folder without the UUID Trino adds) and fixed drop_table."""
    tok = cat9.token()
    fs = cat9.lake_fs()
    for name in names:
        cat9.drop_table("dev", name, tok)
        status, view = cat9.call("GET", f"{cat9.CAT}/namespaces/dev/views/{name}", tok=tok)
        cat9.trino(f"DROP VIEW IF EXISTS lakehouse_dev.dev.{name}")
        if status == 200:  # Part 24: a dropped view's metadata files stay too; delete its folder like a table's
            folder = view["metadata-location"].removeprefix("s3://").rsplit("/", 2)[0]
            if folder.startswith("larkspur-lake/warehouse/dev/"):
                fs.invalidate_cache()
                if fs.exists(folder):
                    fs.rm(folder, recursive=True)


def land_pos(where: str, fresh: bool) -> int:
    """Land raw POS uploads (Part 17's raw.pos_uploads), one Iceberg append per call, as the pipeline principal."""
    data: pa.Table = p17.raw_views().sql(POS.format(where=where)).to_arrow_table()
    from scripts import part23_privacy as privacy  # Part 23: people who asked to be erased stay erased

    data = privacy.suppress(data, tombstone=p17.SUPPRESSED["pos_uploads"])
    cat = cat9.pyiceberg("pipeline")
    if fresh:
        cat9.drop_table("raw", "pos_uploads", cat9.token())
        cat.create_table("raw.pos_uploads", schema=data.schema)
    cat.load_table("raw.pos_uploads").append(data)
    return data.num_rows


def trino_queries(since: str) -> list[dict]:
    req = urllib.request.Request("http://localhost:8080/v1/query", headers={"X-Trino-User": "lab"})
    return [q for q in json.load(urllib.request.urlopen(req)) if q["queryStats"]["createTime"] >= since]


def size(text: str) -> float:
    """Trino's data sizes ("12.5MB", "830kB", "0B") in MiB."""
    n, unit = re.fullmatch(r"([\d.]+)([kMGT]?B)", text).groups()
    return float(n) * {"B": 1, "kB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40}[unit] / 2**20


def cpu(text: str) -> float:
    n, unit = re.fullmatch(r"([\d.]+)(ns|us|ms|s|m|h)", text).groups()
    return float(n) * {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1, "m": 60, "h": 3600}[unit]


def invoke(args: list[str], now: datetime) -> dict:
    """One dbt invocation in this process, with dbt's clock set to `now`: microbatch models pick their batches from
    that clock and their lookback, exactly as a scheduled run at that time would. Returns per-model time, rows,
    batches, and the Trino CPU seconds and bytes read by that model's statements."""
    import dbt.task.run as dbt_run
    from dbt.cli.main import dbtRunner

    dbt_run.get_invocation_started_at = lambda: now
    since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
    res = dbtRunner().invoke(args)
    if not res.success:
        raise SystemExit(f"dbt failed at {now}: {res.exception or [r.message for r in res.result]}")
    queries = trino_queries(since)
    out = {}
    for r in res.result:
        name = r.node.name
        pattern = re.compile(rf'"{name}(__dbt_tmp\w*)?"')
        mine = [q for q in queries if pattern.search(q["query"])]
        out[name] = {
            "seconds": r.execution_time,
            "batches": len(r.batch_results.successful) if r.batch_results else None,
            "cpu_s": sum(cpu(q["queryStats"]["totalCpuTime"]) for q in mine),
            "read_mib": sum(size(q["queryStats"]["physicalInputDataSize"]) for q in mine),
        }
    return out


def replay(cutoff: date = CUTOFF, last: date = LAST_UPLOAD) -> list[dict]:
    """Land uploads to `cutoff`, build every variant, then land one upload day at a time and run the variants at 00:30
    UTC the next morning. Online orders placed after each simulated run are hidden (`replay_now`)."""
    os.environ["DBT_PROFILES_DIR"] = str(PROJECT)
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    base = ["--quiet", "--target", "dev", "--defer", "--state", str(STATE), "--favor-state"]

    def run(select: list[str], now: datetime) -> dict:
        lab_vars = f"{{part18_lab: true, replay_now: '{now:%Y-%m-%d %H:%M:%S}'}}"
        return invoke(["run", *base, "--vars", lab_vars, "--select", *DEV_VIEWS, *select], now)

    cwd = os.getcwd()
    os.chdir(PROJECT)
    try:
        empty_dev(VARIANTS + DEV_VIEWS + [FULL])  # dev starts empty, as a new developer's namespace would
        n = land_pos(f"upload_date <= DATE '{cutoff}'", fresh=True)
        now = datetime.combine(cutoff, datetime.min.time(), UTC) + RUN_AT
        first = run(["fct_sales_line", "fct_sales_line_naive"], now)
        # The two microbatch variants differ only in lookback, so their first builds are identical: copy one.
        p17.trino(
            "CREATE TABLE lakehouse_dev.dev.fct_sales_line_lookback1 AS SELECT * FROM lakehouse_dev.dev.fct_sales_line"
        )
        print(
            f"first build, uploads to {cutoff:%d %b} ({n:,} events), run {now:%d %b %H:%M}: "
            f"microbatch {first['fct_sales_line']['seconds']:.0f} s in {first['fct_sales_line']['batches']} batches, "
            f"naive {first['fct_sales_line_naive']['seconds']:.0f} s"
        )
        runs = [{"day": cutoff, "events": n, **first}]
        day = cutoff + timedelta(days=1)
        while day <= last:
            n = land_pos(f"upload_date = DATE '{day}'", fresh=False)
            now = datetime.combine(day, datetime.min.time(), UTC) + RUN_AT
            stats = run(VARIANTS, now)
            runs.append({"day": day, "events": n, **stats})
            label = {"fct_sales_line": "lookback2"}
            cols = "  ".join(f"{label.get(v, v[15:])} {stats[v]['seconds']:4.1f}s" for v in VARIANTS)
            print(f"upload {day:%d %b}: {n:>6,} events  run {now:%d %b %H:%M}  {cols}")
            day += timedelta(days=1)
    finally:
        os.chdir(cwd)
    LAB.mkdir(parents=True, exist_ok=True)
    (LAB / "replay.json").write_text(json.dumps(runs, default=str, indent=1))
    return runs


# ---------- the evidence: each variant against a full rebuild ----------

FULL = "fct_sales_line_full"  # models/lab18: the same SELECT as a plain table, built once from everything landed
COLUMNS = [
    "date_key", "store_key", "product_key", "promo_key", "profile_key", "transaction_id", "line_no",
    "event_ts", "uploaded_at", "currency", "qty", "unit_price_minor", "discount_minor", "amount_minor",
]  # fmt: skip
ROW_HASH = "from_big_endian_64(xxhash64(to_utf8(concat_ws('|', {cols}))))"  # one bigint per row: EXCEPT stays small
LATE_BASKET = "GB-0114-20260930-00019"  # rung up 30 Sep 12:06 BST, uploaded 1 Oct 03:06 UTC (16 h late)


def build_full() -> dict:
    """The full rebuild, once all uploads have landed: the reference for `compare`, and the cost of a rebuild."""
    os.environ["DBT_PROFILES_DIR"] = str(PROJECT)
    args = ["run", "--quiet", "--target", "dev", "--defer", "--state", str(STATE), "--favor-state"]
    args += ["--vars", "{part18_lab: true}", "--select", *DEV_VIEWS, FULL]  # the views without the replay clock
    cwd = os.getcwd()
    os.chdir(PROJECT)
    try:
        stats = invoke(args, datetime.combine(LAST_UPLOAD, datetime.min.time(), UTC) + RUN_AT)[FULL]
    finally:
        os.chdir(cwd)
    print(f"{FULL}: {stats['seconds']:.1f} s, Trino CPU {stats['cpu_s']:.1f} s, read {stats['read_mib']:.1f} MiB")
    (LAB / "full.json").write_text(json.dumps(stats))
    return stats


def compare() -> dict:
    """Row for row: hash every row of each variant and of the full rebuild, then count rows on one side only. One
    month of date_key at a time: the whole table's hashes in one query ran past Trino's per-node memory limit."""
    cols = ", ".join(f"coalesce(cast({c} as varchar), '<null>')" for c in COLUMNS)
    h = ROW_HASH.format(cols=cols)
    months = [r[0] for r in p17.trino(f"SELECT DISTINCT date_key / 100 FROM lakehouse_dev.dev.{FULL} ORDER BY 1")]
    out = {}
    print(f"{'variant':<26} {'rows':>10} {'missing':>8} {'extra':>6}  missing money (minor units, by currency)")
    for v in [*VARIANTS, FULL]:
        missing: dict[str, list[int]] = {}
        extra = 0
        for m in months:
            rows = p17.trino(f"""
                WITH full_rows AS (SELECT {h} AS h, currency, amount_minor FROM lakehouse_dev.dev.{FULL}
                                   WHERE date_key / 100 = {m}),
                     variant AS (SELECT {h} AS h FROM lakehouse_dev.dev.{v} WHERE date_key / 100 = {m})
                SELECT 'missing', currency, count(*), sum(amount_minor) FROM full_rows
                WHERE h NOT IN (SELECT h FROM variant) GROUP BY currency
                UNION ALL
                SELECT 'extra', NULL, count(*), NULL FROM (SELECT h FROM variant EXCEPT SELECT h FROM full_rows)""")
            for kind, currency, n, money in rows:
                if kind == "extra":
                    extra += n
                else:
                    missing.setdefault(currency, [0, 0])
                    missing[currency][0] += n
                    missing[currency][1] += money
        extra += p17.trino(
            f"SELECT count(*) FROM lakehouse_dev.dev.{v} WHERE date_key / 100 NOT IN ({', '.join(map(str, months))})"
        )[0][0]
        total = p17.trino(f"SELECT count(*), sum(amount_minor) FROM lakehouse_dev.dev.{v}")[0]
        out[v] = {"rows": total[0], "sum": total[1], "missing": missing, "extra": extra}
        money = ", ".join(f"{c} {m:,}" for c, (_, m) in sorted(missing.items())) or "none"
        print(f"{v:<26} {total[0]:>10,} {sum(n for n, _ in missing.values()):>8,} {extra:>6,}  {money}")
    (LAB / "compare.json").write_text(json.dumps(out))
    return out


def late(basket: str = LATE_BASKET) -> dict:
    """Where one late basket landed in each variant (and when it was rung up and uploaded)."""
    out = {}
    for v in [*VARIANTS, FULL]:
        rows = p17.trino(f"""
            SELECT date_key, count(*), sum(amount_minor),
                   CAST(min(event_ts) AS varchar), CAST(min(uploaded_at) AS varchar)
            FROM lakehouse_dev.dev.{v} WHERE transaction_id = '{basket}' GROUP BY date_key""")
        out[v] = rows
        where = (
            "; ".join(f"date_key {d}: {n} lines, {m:,} minor, rung up {e}, uploaded {u}" for d, n, m, e, u in rows)
            or "not there"
        )
        print(f"{v:<26} {where}")
    return out


def cost() -> dict:
    """Per daily run (the 15 runs after the first build) vs one full rebuild: wall time, Trino CPU, bytes read."""
    runs = json.loads((LAB / "replay.json").read_text())[1:]
    full = json.loads((LAB / "full.json").read_text())
    print(f"{'build':<34} {'median s':>9} {'CPU s':>7} {'read MiB':>9}  batches")
    out = {}
    for v in VARIANTS:
        stats = sorted((r[v] for r in runs), key=lambda x: x["seconds"])
        mid = stats[len(stats) // 2]
        med = {k: sorted(x[k] for x in stats)[len(stats) // 2] for k in ("seconds", "cpu_s", "read_mib")}
        out[v] = med
        line = f"{v + ' (daily run)':<34} {med['seconds']:>9.1f} {med['cpu_s']:>7.1f} {med['read_mib']:>9.1f}"
        print(f"{line}  {mid['batches']}")
    print(f"{FULL + ' (rebuild)':<34} {full['seconds']:>9.1f} {full['cpu_s']:>7.1f} {full['read_mib']:>9.1f}")
    out[FULL] = full
    for v in (VARIANTS[-1], FULL):  # what the daily delete + insert leave behind (Iceberg content 0 data, 1 deletes)
        files = dict(p17.trino(f'SELECT content, count(*) FROM lakehouse_dev.dev."{v}$files" GROUP BY content'))
        snaps = p17.trino(f'SELECT count(*) FROM lakehouse_dev.dev."{v}$snapshots"')[0][0]
        print(f"{v}: {files.get(0, 0)} data files, {files.get(1, 0)} position-delete files, {snaps} snapshots")
        out[f"{v} files"] = {"data": files.get(0, 0), "deletes": files.get(1, 0), "snapshots": snaps}
    return out


# ---------- slim CI, a contract and a version ----------

DEV = ["--target", "dev", "--defer", "--state", str(STATE), "--favor-state"]


def plain(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def nodes(*select: str) -> list[str]:
    """The buildable nodes `dbt ls` selects (models, tests, unit tests, snapshots; not sources), as type:name."""
    run = dbt("ls", "--target", "dev", *select, "--output", "json", "--output-keys", "resource_type name", echo=False)
    rows = [json.loads(line) for line in run.stdout.splitlines() if line.startswith("{")]
    return [f"{r['resource_type']}:{r['name']}" for r in rows if r["resource_type"] != "source"]


def ci() -> dict:
    """What a pull request's CI job runs: `make dbt-ci` in a fresh `dev` namespace, after counting what
    state:modified+ selects."""
    every, picked = nodes(), nodes("--select", "state:modified+", "--state", str(STATE))
    kinds = {k: sum(1 for p in picked if p.startswith(k + ":")) for k in ("model", "test", "unit_test")}
    print(f"state:modified+ selects {len(picked)} of {len(every)} nodes: {kinds}")
    print("  models:", ", ".join(sorted({p[6:] for p in picked if p.startswith("model:")})))
    empty_dev([r[0] for r in p17.trino("SHOW TABLES FROM lakehouse_dev.dev")])  # a pull request starts empty
    run = subprocess.run(["make", "-s", "dbt-ci"], cwd=ROOT, capture_output=True, text=True)
    out = plain(run.stdout)
    print("\n".join(line for line in out.splitlines() if re.search(r"Found|PASS=|WARN|ERROR|FAIL|Finished", line)))
    return {"every": every, "picked": picked, "build": out, "returncode": run.returncode}


def contract() -> dict:
    """An enforced contract on Trino: dbt refuses a renamed column before running anything, Trino refuses a NULL in a
    not_null column, and nothing stops a duplicate key: only the grain test sees it."""
    table = "lakehouse_dev.dev.rpt_daily_net_revenue_v2"
    snaps = p17.trino("""SELECT operation, element_at(summary, 'added-records'), summary['total-records']
                          FROM lakehouse_dev.dev."rpt_daily_net_revenue_v2$snapshots" ORDER BY committed_at""")
    print("a contracted build's commits: " + "; ".join(f"{o} +{a or 0} rows (total {t})" for o, a, t in snaps))
    model = PROJECT / "models/marts/rpt_daily_net_revenue_v2.sql"
    original = model.read_text()
    out = {"snapshots": snaps}
    try:
        model.write_text(original.replace("as report_currency", "as currency_code"))
        run = dbt("run", "--select", "rpt_daily_net_revenue", "--target", "dev", "--defer", "--state", str(STATE),
                  "--favor-state", echo=False)  # fmt: skip
    finally:
        model.write_text(original)
    out["rename"] = plain(run.stdout)
    print("\n".join(line for line in out["rename"].splitlines() if "|" in line or "contract" in line))
    _, out["null"] = cat9.trino(f"INSERT INTO {table} VALUES (NULL, DATE '2026-10-03', 0, 0, 0, 'GBP', false)")
    print(f"INSERT a NULL country: {out['null']}")
    _, out["dup"] = cat9.trino(
        f"INSERT INTO {table} SELECT * FROM {table} WHERE country = 'GB' AND business_date = DATE '2026-10-03'"
    )
    print(f"INSERT a second GB row for 3 Oct: {out['dup'] or 'accepted'}")
    test = dbt("test", "--select", "rpt_daily_net_revenue", *DEV, echo=False)
    out["test"] = plain(test.stdout)
    print("\n".join(line for line in out["test"].splitlines() if re.search(r"(PASS|FAIL) .*v2|Done", line)))
    cat9.trino(f"DELETE FROM {table} WHERE country = 'GB' AND business_date = DATE '2026-10-03'")
    dbt("run", "--select", "rpt_daily_net_revenue", *DEV, echo=False)  # restore the dev table
    return out


def deploy() -> dict:
    """Merge, then deploy: build what changed in production. The incremental facts changed shape (a new column, a new
    strategy), so they are rebuilt from `begin` with --full-refresh, batch by batch; then production's manifest is the
    new state for the next pull request."""
    started = datetime.now(UTC)
    run = dbt("build", "--target", "prod", "--select", "state:modified+", "--state", str(STATE), "--full-refresh",
              "--event-time-start", "2026-05-31", "--event-time-end", "2026-10-06", echo=False)  # fmt: skip
    took = (datetime.now(UTC) - started).total_seconds()
    out = plain(run.stdout)
    print(
        "\n".join(
            line for line in out.splitlines() if re.search(r"Found|OK created sql microbatch|PASS=|Finished", line)
        )
    )
    print(f"deploy: {took / 60:.1f} min")
    if run.returncode == 0:
        prod_state(None)
    return {"build": out, "returncode": run.returncode, "seconds": took}


# ---------- what one batch does ----------

BATCH_FILTER = "event_ts >= TIMESTAMP '2026-09-15 00:00:00+00:00' AND event_ts < TIMESTAMP '2026-09-16 00:00:00+00:00'"


def explain(view: str = "lakehouse_dev.dev.int_sales_lines") -> list[str]:
    """Trino's plan for one day of int_sales_lines (the view a batch reads): every scan of the raw tables, and of Part
    21's dispositions, should carry the event_ts range."""
    plan = p17.trino(f"EXPLAIN SELECT count(*) FROM {view} WHERE {BATCH_FILTER}")[0][0]
    scans = [line.strip() for line in plan.splitlines() if re.search(r"(Scan\w*)\[table = \w+:(raw|silver)\.", line)]
    for line in scans:
        table = re.search(r"(?:raw|silver)\.(\w+)", line).group(1)
        rng = re.search(r"filterPredicate = \(\((.*?)\) AND \((.*?)\)\)", line)
        print(f"{table:<12} " + (f"{rng.group(1)} AND {rng.group(2)}" if rng else "no event-time filter"))
    return scans


def batch_sql(model: str = "fct_sales_line") -> list[str]:
    """The statements dbt sent for the last batch of `model` in dbt/larkspur/logs/dbt.log (column lists cut)."""
    lines = (PROJECT / "logs/dbt.log").read_text(errors="replace").splitlines()
    starts = [
        i
        for i, line in enumerate(lines[:-1])
        if "create or replace view" in line and f'"{model}__dbt_tmp_' in line + lines[i + 1]
    ]
    i = starts[-1] if starts else None
    keep = re.compile(
        r'(create or replace view|^\s*"lakehouse|from \(select|delete from|insert into|event_ts [<>]|drop view)'
    )
    out = []
    while i is not None and i < len(lines):
        line = re.sub(r"\x1b\[[0-9;]*m", "", lines[i])
        if (m := keep.search(line)) and "information_schema" not in line.lower():
            out.append(line[m.start() :].strip()[:150] if "insert into" in line else line.strip()[:150])
        if "drop view if exists" in line:
            break
        i += 1
    print("\n".join(out))
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command")
    p.add_argument("args", nargs=argparse.REMAINDER)
    a = p.parse_args()
    if a.command == "setup":
        setup()
    elif a.command == "replay":
        replay()
    elif a.command == "full":
        build_full()
    elif a.command == "compare":
        compare()
    elif a.command == "late":
        late()
    elif a.command == "cost":
        cost()
    elif a.command == "ci":
        sys.exit(ci()["returncode"])
    elif a.command == "contract":
        contract()
    elif a.command == "deploy":
        sys.exit(deploy()["returncode"])
    elif a.command == "explain":
        explain()
    elif a.command == "batch-sql":
        batch_sql()
    elif a.command == "prod-state":
        prod_state(None if a.args == ["--working-tree"] else PROD_TAG)
    else:
        raise SystemExit(f"unknown command {a.command}")
