"""Part 28: streaming SQL with Flink. Dedup, an as-of price join, windows and an updating aggregate into Iceberg.

python -m scripts.part28_flink setup      # (make up-flink-lake) Polaris principal, price topic, price export, reset
./gradlew -q :kafka-backbone:run --args="prices"   # the price history onto its topic (Java producer, Part 27's module)
python -m scripts.part28_flink run        # the job: POS + prices from Kafka -> three Iceberg tables [--mini-batch]
python -m scripts.part28_flink explain    # what each query emits: insert-only or updates (EXPLAIN CHANGELOG_MODE)
python -m scripts.part28_flink dedup      # baskets kept by a dedup on arrival vs one on event time
python -m scripts.part28_flink tables     # what the job committed: rows, snapshots, files per table
python -m scripts.part28_flink asof       # one SKU whose price changed mid-day: lines joined to the old and new price
python -m scripts.part28_flink reconcile  # streamed totals against Part 21's dispositions and gold, store by store
python -m scripts.part28_flink changelog  # what updating queries emit, and what an upsert-kafka topic keeps
python -m scripts.part28_flink errors     # three mistakes Flink refuses before any data moves
"""

import argparse
import gzip
import json
import re
import subprocess
import time
import urllib.request
from datetime import date  # noqa: F401 (tests compare reconcile dates with it)
from pathlib import Path

import duckdb

from scripts import part04_star as star
from scripts import part09_catalog as p9
from scripts import part27_backbone as p27

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = p27.COMPOSE
FLINK = "http://localhost:8081"
PRICE_TOPIC = "larkspur.catalog.price-changes"
TOP_TOPIC = "larkspur.analytics.top-stores"
PRICES_DIR = ROOT / "data/lab/part28/prices"
POS_DAY = p27.POS_DAY  # the upload day Part 27 replayed onto the POS topic

# The streaming job's own principal: it may create and write tables in `stream`, and nothing else (Part 9's model).
PRINCIPAL, SECRET = "flink", "flink-lab-secret"  # lab-only credentials, like Part 9's
PRINCIPAL_ROLE, CATALOG_ROLE = "streaming_jobs", "stream_writer"
TABLES = ("stream.pos_sales_lines", "stream.store_sales_hourly", "stream.store_sales_daily")

# Topics as code (Part 27). Price versions are kept, not compacted: an as-of join replayed from the start needs every
# old price, and the whole history is a few thousand records. Top stores is an upsert topic: compaction keeps the
# latest row per key, and a null value (tombstone) deletes a key.
TOPICS = {
    PRICE_TOPIC: {"partitions": 3, "config": {"retention.ms": -1, "cleanup.policy": "delete"}},
    TOP_TOPIC: {"partitions": 1, "config": {"cleanup.policy": "compact"}},
}


def kafka(*args: str, tool: str = "kafka-topics.sh", check: bool = True) -> str:
    return p27.kafka(*args, tool=tool, check=check)


def flink_sql(script: str, *settings: str, expect_error: bool = False) -> str:
    """Run flink/sql/part28/<script> with the SQL client in the JobManager, after tables.sql (sources, views, the
    catalog); `settings` are extra session settings (key=value). Its whole output; an error stops unless expected."""
    sql = "/opt/flink/lab/sql/part28"
    cmd = [*COMPOSE, "exec", "-T", "flink-jobmanager", "./bin/sql-client.sh", "-i", f"{sql}/tables.sql"]
    cmd += [x for kv in settings for x in ("-D", kv)] + ["-f", f"{sql}/{script}"]
    out = subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True).stdout
    if "[ERROR]" in out and not expect_error:
        raise SystemExit(out[out.index("[ERROR]") :][:2000])
    return out


def tableau(out: str) -> list[list[list[str]]]:
    """The result tables in SQL-client output (tableau mode): a list of tables, each a list of rows of cells."""
    tables, rows, border = [], [], 0
    for line in out.splitlines():
        line = line[line.index("+-") :] if "+-" in line else line
        if line.startswith("+-"):
            border += 1
            if border == 3:  # top, under the header, bottom
                tables.append(rows[1:])
                rows, border = [], 0
        elif line.startswith("|") and border:
            rows.append([c.strip() for c in line.strip().strip("|").split("|")])
    return tables


def rest(path: str) -> dict:
    with urllib.request.urlopen(f"{FLINK}{path}", timeout=10) as r:
        return json.load(r)


def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    return c


# ---------- setup ----------


def setup_principal() -> None:
    tok = p9.token()
    cat, mgmt = p9.CAT, p9.MGMT
    p9.ok(p9.call("POST", f"{cat}/namespaces", {"namespace": ["stream"]}, tok=tok)[0], "namespace stream", (200, 409))
    status, _ = p9.call("POST", f"{mgmt}/principals", {"principal": {"name": PRINCIPAL}}, tok=tok)
    p9.ok(status, f"principal {PRINCIPAL}", (201, 409))
    body = {"clientId": PRINCIPAL, "clientSecret": SECRET}
    p9.ok(p9.call("POST", f"{mgmt}/principals/{PRINCIPAL}/reset", body, tok=tok)[0], f"reset {PRINCIPAL}")
    roles = f"{mgmt}/catalogs/{p9.CATALOG}/catalog-roles"
    p9.call("DELETE", f"{roles}/{CATALOG_ROLE}", tok=tok)  # re-created with exactly the grants below
    p9.ok(p9.call("POST", roles, {"catalogRole": {"name": CATALOG_ROLE}}, tok=tok)[0], CATALOG_ROLE, (201, 409))
    # Least privilege: what the job does. It creates, lists, reads and writes tables in `stream`; it drops nothing
    # (setup drops the tables as root). Flink's catalog looks the namespace up before reading a table in it
    # (Polaris op LOAD_NAMESPACE_METADATA), hence NAMESPACE_READ_PROPERTIES.
    grants = [("catalog", None, "NAMESPACE_LIST"), ("namespace", "stream", "NAMESPACE_READ_PROPERTIES")]
    grants += [
        ("namespace", "stream", p) for p in ("TABLE_CREATE", "TABLE_LIST", "TABLE_READ_DATA", "TABLE_WRITE_DATA")
    ]
    for kind, ns, privilege in grants:
        grant = {"type": kind, "privilege": privilege} | ({"namespace": [ns]} if ns else {})
        status, _ = p9.call("PUT", f"{roles}/{CATALOG_ROLE}/grants", {"grant": grant}, tok=tok)
        p9.ok(status, f"grant {privilege}")
    prole = {"principalRole": {"name": PRINCIPAL_ROLE}}
    p9.ok(p9.call("POST", f"{mgmt}/principal-roles", prole, tok=tok)[0], PRINCIPAL_ROLE, (201, 409))
    body = {"catalogRole": {"name": CATALOG_ROLE}}
    status, _ = p9.call("PUT", f"{mgmt}/principal-roles/{PRINCIPAL_ROLE}/catalog-roles/{p9.CATALOG}", body, tok=tok)
    p9.ok(status, f"{CATALOG_ROLE} -> {PRINCIPAL_ROLE}")
    p9.ok(p9.call("PUT", f"{mgmt}/principals/{PRINCIPAL}/principal-roles", prole, tok=tok)[0], "role -> principal")
    privs = p9.grants_by_securable(CATALOG_ROLE, tok)
    for where, ps in privs.items():
        print(f"{PRINCIPAL} -> {PRINCIPAL_ROLE} -> {CATALOG_ROLE}: {where}: {', '.join(ps)}")


def drop_tables() -> None:
    """The job's tables, dropped and their folders deleted: every run starts from empty tables."""
    cat = p27.catalog()
    fs = star.fs()
    for ident in TABLES:
        if cat.table_exists(ident):
            folder = cat.load_table(ident).location().removeprefix("s3://").rstrip("/")
            cat.drop_table(ident)
            fs.invalidate_cache()
            if folder.startswith("larkspur-lake/warehouse/stream/") and fs.exists(folder):
                fs.rm(folder, recursive=True)


def export_prices() -> int:
    """The raw zone's price history as JSON lines with the SKU, for the Java producer: oldest first."""
    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    c = con()
    rows = c.sql(
        """SELECT p.sku, c.country, c.currency, c.price_minor,
                  strftime(c.effective_from, '%Y-%m-%dT%H:%M:%SZ') AS effective_from
           FROM 'data/raw/pricing/price_changes/price_changes.parquet' c
           JOIN (SELECT DISTINCT product_id, sku FROM 'data/raw/catalog/products/*/*.parquet') p USING (product_id)
           ORDER BY c.effective_from, p.sku, c.country"""
    ).fetchall()
    cols = ("sku", "country", "currency", "price_minor", "effective_from")
    with gzip.open(PRICES_DIR / "price-changes.jsonl.gz", "wt") as f:
        for r in rows:
            f.write(json.dumps(dict(zip(cols, r, strict=True))) + "\n")
    return len(rows)


def cmd_setup(_: argparse.Namespace) -> None:
    for job in rest("/jobs")["jobs"]:  # a running job from an earlier run would keep writing
        if job["status"] == "RUNNING":
            urllib.request.urlopen(urllib.request.Request(f"{FLINK}/jobs/{job['id']}", method="PATCH"))
    setup_principal()
    drop_tables()
    kafka("--delete", "--if-exists", "--topic", PRICE_TOPIC)
    kafka("--delete", "--if-exists", "--topic", TOP_TOPIC)
    p27._wait_deleted([PRICE_TOPIC, TOP_TOPIC])
    for topic, spec in TOPICS.items():
        configs = [x for k, v in spec["config"].items() for x in ("--config", f"{k}={v}")]
        kafka("--create", "--topic", topic, "--partitions", str(spec["partitions"]), *configs)
    for subject in (f"{PRICE_TOPIC}-value", f"{TOP_TOPIC}-key", f"{TOP_TOPIC}-value"):
        p27.http("DELETE", f"{p27.REGISTRY}/registry/v3/groups/default/artifacts/{subject}")
    print(f"topics {PRICE_TOPIC} (3 partitions, kept forever), {TOP_TOPIC} (compacted); tables dropped")
    print(f"{export_prices():,} price versions -> {PRICES_DIR.relative_to(ROOT)}")


# ---------- explain: what each query emits ----------

EXPLAINED = (
    "dedup, first copy by arrival",
    "dedup, first copy by event time",
    "versioned view (latest price per key)",
    "temporal join (price as of the sale)",
    "tumbling window, 1 hour",
    "running total per store and day",
    "top 3 stores per country",
    "running total into the Iceberg upsert table",
)


def explain() -> list[tuple[str, str, str]]:
    """(query, operator, row kinds) for each EXPLAIN CHANGELOG_MODE in explain.sql: the operator that decides it."""
    out = flink_sql("explain.sql")
    found = []
    for label, block in zip(EXPLAINED, out.split("== Optimized Physical Plan ==")[1:], strict=True):
        physical = block.split("== Optimized Execution Plan ==")[0]
        execution = block.split("== Optimized Execution Plan ==")[1].split("\n\n")[0]
        ops = re.findall(r"(\w+)\(.*changelogMode=\[([A-Z,]*)\]", physical)
        named = {"Rank", "TemporalJoin", "GlobalWindowAggregate", "GroupAggregate"}
        op, kinds = next(((o, k) for o, k in ops if o in named), ops[0])
        if op == "Rank":  # the execution plan names a deduplication as such
            first = re.search(r"(Deduplicate|Rank)\((?:keep=\[(\w+)\].*?order=\[(\w+)\])?", execution)
            op = f"Deduplicate {first.group(2)} on {first.group(3)}" if first.group(1) == "Deduplicate" else "Rank"
        if op == "GroupAggregate" and ops[0][0] == "Sink":
            op = "GroupAggregate -> Iceberg sink"
        found.append((label, op, kinds))
    return found


def cmd_explain(_: argparse.Namespace) -> None:
    print(f"{'query':45} {'operator':36} emits")
    for label, op, kinds in explain():
        print(f"{label:45} {op:36} {kinds}")


def dedup() -> dict[str, int]:
    """Baskets kept by each dedup, from the last row of each running count."""
    by_arrival, by_event_time = tableau(flink_sql("dedup_event_time.sql"))[:2]
    return {"arrival": int(by_arrival[-1][1]), "event time": int(by_event_time[-1][1])}


def cmd_dedup(_: argparse.Namespace) -> None:
    kept = dedup()
    print(f"first copy by arrival:    {kept['arrival']:,} baskets")
    print(f"first copy by event time: {kept['event time']:,} baskets")


ERRORS = {  # flink/sql/part28/error_<name>.sql: a mistake each, and the first line of Flink's answer
    "append": "an updating query into an insert-only sink (kafka)",
    "nokey": "an upsert sink (upsert-kafka) without a primary key",
    "stream": "the namespace stream without backticks",
}


def errors() -> dict[str, str]:
    out = {}
    for name in ERRORS:
        text = flink_sql(f"error_{name}.sql", expect_error=True)
        reason = text[text.index("Reason:") + len("Reason:") :].strip().splitlines()[0] if "Reason:" in text else ""
        out[name] = reason.replace("\x1b[0m", "")
    return out


def cmd_errors(_: argparse.Namespace) -> None:
    for name, reason in errors().items():
        print(f"{ERRORS[name]}:\n  {reason}")


# ---------- run: the streaming job ----------

MINI_BATCH = (  # Flink's mini-batch aggregation: buffer input, update each key once per batch
    "table.exec.mini-batch.enabled=true",
    "table.exec.mini-batch.allow-latency=5 s",
    "table.exec.mini-batch.size=5000",
)


def wait_job(job_id: str, timeout: int = 600) -> dict:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        job = rest(f"/jobs/{job_id}")
        if job["state"] in ("FINISHED", "FAILED", "CANCELED"):
            return job
        time.sleep(2)
    raise SystemExit(f"job {job_id} still {job['state']} after {timeout} s")


def run(mini_batch: bool = False) -> dict:
    drop_tables()
    out = flink_sql("sales_to_lake.sql", *(MINI_BATCH if mini_batch else ()))
    job_id = re.search(r"Job ID: (\w+)", out).group(1)
    job = wait_job(job_id)
    checkpoints = rest(f"/jobs/{job_id}/checkpoints")["counts"]
    return {"job": job_id, "state": job["state"], "seconds": job["duration"] / 1000, "checkpoints": checkpoints}


def summaries() -> dict[str, dict]:
    """Per table: the snapshots the job committed, and what the last one says the table holds."""
    cat = p27.catalog()
    out = {}
    for ident in TABLES:
        t = cat.load_table(ident)
        snaps = t.snapshots()
        total = t.current_snapshot().summary.additional_properties
        out[ident] = {
            "snapshots": len(snaps),
            "records written": sum(int(s.summary.additional_properties.get("added-records", 0)) for s in snaps),
            "data files": int(total["total-data-files"]),
            "equality deletes": int(total["total-equality-deletes"]),
            "position deletes": int(total["total-position-deletes"]),
            "identifier": ",".join(sorted(t.schema().identifier_field_names())) or "-",
        }
    return out


def print_summaries() -> None:
    print(f"{'table':26} {'snapshots':>9} {'written':>8} {'files':>5} {'eq deletes':>10} {'pos deletes':>11}  key")
    for ident, s in summaries().items():
        print(
            f"{ident:26} {s['snapshots']:>9} {s['records written']:>8,} {s['data files']:>5}"
            f" {s['equality deletes']:>10,} {s['position deletes']:>11,}  {s['identifier']}"
        )


def cmd_run(a: argparse.Namespace) -> None:
    r = run(a.mini_batch)
    c = r["checkpoints"]
    batch = " with mini-batch aggregation" if a.mini_batch else ""
    print(f"job {r['job'][:8]} {r['state']}{batch} in {r['seconds']:.1f} s; checkpoints completed {c['completed']}")
    print_summaries()


def cmd_tables(_: argparse.Namespace) -> None:
    print_summaries()


# ---------- asof: one price change, mid-day ----------


def lines() -> "duckdb.DuckDBPyConnection":
    c = con()
    c.register("l", p27.catalog().load_table("stream.pos_sales_lines").scan().to_arrow())
    return c


def asof() -> dict:
    c = lines()
    out = {
        "versions": c.execute(
            """SELECT price_from, list_price_minor, count(*), min(event_ts), max(event_ts),
                      count(*) FILTER (WHERE unit_price_minor = list_price_minor)
               FROM l WHERE sku = 'SKU-00676' AND country = 'SG' AND business_date = DATE '2026-08-15'
               GROUP BY ALL ORDER BY 1"""
        ).fetchall()
    }
    out["lines"], out["priced"], out["agree"] = c.execute(
        "SELECT count(*), count(list_price_minor), count(*) FILTER (WHERE unit_price_minor = list_price_minor) FROM l"
    ).fetchone()
    # The same lines against the latest price in the topic instead (what a join to the current price table gives).
    c.execute(f"CREATE VIEW p AS SELECT * FROM read_json('{PRICES_DIR}/*.jsonl.gz')")
    out["latest_wrong"] = c.execute(
        """SELECT count(*) FILTER (WHERE l.unit_price_minor <> p.price_minor)
           FROM l JOIN (SELECT * FROM p QUALIFY row_number() OVER (PARTITION BY sku, country
                                                                   ORDER BY effective_from DESC) = 1) p
             ON p.sku = l.sku AND p.country = l.country"""
    ).fetchone()[0]
    return out


def cmd_asof(_: argparse.Namespace) -> None:
    r = asof()
    print("SKU-00676 in Singapore stores on business date 2026-08-15, as the job wrote it (times UTC; SGT = UTC+8):")
    head = f"{'price version from':20} {'list price':>10} {'lines':>5}  {'first sale':19}  {'last sale':19}"
    print(head + "  till agrees")
    ts = "%Y-%m-%d %H:%M:%S"
    for v, price, n, first, last, agree in r["versions"]:
        version = f"{v:{ts}}" if v else "(no version)       "  # a left join with nothing to join to
        listed = f"{price:>10,}" if price is not None else f"{'null':>10}"
        print(f"{version}  {listed} {n:>5}  {first:{ts}}  {last:{ts}}  {agree:>11}")
    print(f"all lines: {r['lines']:,}; priced {r['priced']:,}; till price = as-of list price on {r['agree']:,}")
    wrong = r["latest_wrong"]
    print(f"joined to each SKU's latest price instead (up to 2026-10-03): {wrong:,} lines disagree with the till")


# ---------- reconcile: the stream against the batch answer ----------


def daily_rows() -> list[tuple]:
    """stream.store_sales_daily read by Flink (batch), equality deletes applied: what a reader sees."""
    rows = tableau(flink_sql("read_daily.sql"))[-1]
    return [(r[0], r[1], r[2], int(r[3]), int(r[4]), int(r[5])) for r in rows]


def reconcile() -> dict:
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

    from scripts import part21_reconcile as p21

    cat = p27.catalog()
    c = lines()
    c.execute(
        "CREATE TABLE daily (store_id VARCHAR, business_date DATE, currency VARCHAR, baskets BIGINT, units BIGINT,"
        " sales_minor BIGINT)"
    )
    c.executemany("INSERT INTO daily VALUES (?, ?, ?, ?, ?, ?)", daily_rows())
    days = And(GreaterThanOrEqual("upload_date", "2026-08-12"), LessThanOrEqual("upload_date", "2026-08-16"))
    c.register("d", cat.load_table(p21.DISPOSITIONS).scan(row_filter=days).to_arrow())
    c.register("h", cat.load_table("stream.store_sales_hourly").scan().to_arrow())
    gold = cat.load_table("gold.daily_store_sales").scan(row_filter=EqualTo("business_date", "2026-08-15"))
    c.register("g", gold.to_arrow())
    c.execute(
        f"""CREATE VIEW accepted AS
            SELECT d.*, timezone(s.timezone, d.event_ts)::DATE AS business_date
            FROM d JOIN read_csv('{ROOT}/data/raw/reference/stores/stores.csv') s USING (store_id)
            WHERE d.disposition = 'accepted'"""
    )
    out: dict = {}
    # 1. Same input: the upload day the topic holds, Part 21's accepted events for it, per store and business date.
    c.execute(
        f"""CREATE VIEW batch AS SELECT store_id, business_date, count(*) AS baskets, sum(amount_minor) AS sales_minor
            FROM accepted WHERE upload_date = DATE '{POS_DAY}' GROUP BY ALL"""
    )
    out["store_days"], out["equal"] = c.execute(
        """SELECT count(*), count(*) FILTER (WHERE s.baskets = b.baskets AND s.sales_minor = b.sales_minor)
           FROM daily s FULL JOIN batch b USING (store_id, business_date)"""
    ).fetchone()
    out["differ"] = c.execute(
        """SELECT store_id, business_date, s.baskets, b.baskets, s.sales_minor - b.sales_minor
           FROM daily s FULL JOIN batch b USING (store_id, business_date)
           WHERE s.baskets IS DISTINCT FROM b.baskets OR s.sales_minor IS DISTINCT FROM b.sales_minor"""
    ).fetchall()
    # The baskets the stream accepted and Part 21 did not, and where Part 21 had seen them first.
    out["extra"] = c.execute(
        f"""SELECT r.basket_id, r.store_id, r.amount_minor, r.disposition, a.upload_date
            FROM d r LEFT JOIN accepted a ON a.event_id = r.first_event_id
            WHERE r.upload_date = DATE '{POS_DAY}' AND r.disposition <> 'accepted'
              AND r.basket_id NOT IN (SELECT basket_id FROM d WHERE upload_date = DATE '{POS_DAY}'
                                      AND disposition = 'accepted')
              AND r.basket_id IN (SELECT basket_id FROM l)"""
    ).fetchall()
    # 2. Same business date: gold for 15 August, which also holds baskets uploaded on other days.
    out["gold"] = c.execute("""SELECT count(*), sum(g.baskets), sum(g.sales_minor) FROM g""").fetchone()
    out["stream_15"] = c.execute(
        "SELECT count(*), sum(baskets), sum(sales_minor) FROM daily WHERE business_date = DATE '2026-08-15'"
    ).fetchone()
    out["other_uploads"] = c.execute(
        f"""SELECT upload_date, count(*), sum(amount_minor) FROM accepted
            WHERE business_date = DATE '2026-08-15' AND upload_date <> DATE '{POS_DAY}' GROUP BY 1 ORDER BY 1"""
    ).fetchall()
    # Upload days are UTC days (Part 21); business dates are local. By country and upload day:
    out["other_by_country"] = c.execute(
        f"""SELECT upload_date, country, count(*) FROM accepted
            WHERE business_date = DATE '2026-08-15' AND upload_date <> DATE '{POS_DAY}' GROUP BY ALL ORDER BY 1, 2"""
    ).fetchall()
    out["gold_stores_equal"] = c.execute(
        f"""WITH other AS (
                SELECT store_id, count(*) AS baskets, sum(amount_minor) AS sales_minor FROM accepted
                WHERE business_date = DATE '2026-08-15' AND upload_date <> DATE '{POS_DAY}' GROUP BY 1),
            stream AS (SELECT store_id, baskets, sales_minor FROM daily WHERE business_date = DATE '2026-08-15')
            SELECT count(*) FILTER (WHERE coalesce(s.baskets, 0) + coalesce(o.baskets, 0) = g.baskets
                                    AND coalesce(s.sales_minor, 0) + coalesce(o.sales_minor, 0) = g.sales_minor)
            FROM g LEFT JOIN stream s USING (store_id) LEFT JOIN other o USING (store_id)"""
    ).fetchone()[0]
    # 3. The hourly windows against the lines: what arrived behind the watermark.
    out["hourly"] = c.execute("SELECT sum(baskets), sum(lines), sum(units_out) FROM h").fetchone()
    out["lines"] = c.execute("SELECT count(DISTINCT basket_id), count(*), sum(qty) FROM l").fetchone()
    # Per store-hour, the lines missing from the window, and how many of the window's lines came from baskets uploaded
    # more than 5 minutes after the sale: if the late lines always cover the gap, only late lines went missing.
    out["windows_short"], out["covered_by_late"] = c.execute(
        """WITH w AS (SELECT store_id, date_trunc('hour', event_ts) AS window_start, count(*) AS n,
                             count(*) FILTER (WHERE uploaded_at - event_ts > INTERVAL 5 MINUTE) AS late_n
                      FROM l GROUP BY ALL)
           SELECT count(*), count(*) FILTER (WHERE w.n - coalesce(h.lines, 0) <= w.late_n)
           FROM w LEFT JOIN h USING (store_id, window_start) WHERE w.n > coalesce(h.lines, 0)"""
    ).fetchone()
    out["late_baskets"], out["mid_lag"] = c.execute(
        """SELECT count(DISTINCT basket_id) FILTER (WHERE uploaded_at - event_ts > INTERVAL 5 MINUTE),
                  count(DISTINCT basket_id) FILTER (WHERE uploaded_at - event_ts BETWEEN INTERVAL 5 MINUTE
                                                                                    AND INTERVAL 2 HOUR)
           FROM l"""
    ).fetchone()
    return out


def cmd_reconcile(_: argparse.Namespace) -> None:
    r = reconcile()
    print(f"1. stream.store_sales_daily vs Part 21's accepted events of upload day {POS_DAY}")
    print(f"   store-days {r['store_days']}, equal {r['equal']}")
    for store, day, s, b, diff in r["differ"]:
        print(f"   differs: {store} {day}  stream {s} baskets, Part 21 {b}; stream - Part 21 = {diff:,} minor units")
    for basket, store, amount, disposition, first in r["extra"]:
        print(f"   {basket} ({store}, {amount:,}): Part 21 says {disposition}, first copy uploaded {first}")
    g, s = r["gold"], r["stream_15"]
    print("2. business date 2026-08-15: stream vs gold.daily_store_sales")
    print(f"   gold   {g[0]} stores, {g[1]:,} baskets")
    print(f"   stream {s[0]} stores, {s[1]:,} baskets")
    for day, n, _amount in r["other_uploads"]:
        by = ", ".join(f"{k} {m}" for d, k, m in r["other_by_country"] if d == day)
        print(f"   + accepted baskets of that business date uploaded {day} (UTC upload day): {n} ({by})")
    print(f"   stores where stream + other upload days = gold (baskets and sales): {r['gold_stores_equal']} of {g[0]}")
    hb, hl, hu = r["hourly"]
    lb, ll, lu = r["lines"]
    print("3. stream.store_sales_hourly vs the lines the job wrote")
    print(f"   hourly windows: {hb:,} baskets, {hl:,} lines, {hu:,} units")
    print(f"   lines table:    {lb:,} baskets, {ll:,} lines, {lu:,} units")
    print(f"   missing from the windows: {lb - hb} baskets, {ll - hl} lines, in {r['windows_short']} store-hours")
    print(f"   store-hours where late lines cover the gap: {r['covered_by_late']} of {r['windows_short']}")
    late = r["late_baskets"]
    print(f"   baskets uploaded more than 5 minutes after the sale: {late} ({r['mid_lag']} of them within 2 hours)")


# ---------- changelog: what updating queries emit, and what an upsert topic keeps ----------


def changelog() -> dict:
    kafka("--delete", "--if-exists", "--topic", TOP_TOPIC)
    p27._wait_deleted([TOP_TOPIC])
    spec = TOPICS[TOP_TOPIC]
    configs = [x for k, v in spec["config"].items() for x in ("--config", f"{k}={v}")]
    kafka("--create", "--topic", TOP_TOPIC, "--partitions", str(spec["partitions"]), *configs)
    store, top = tableau(flink_sql("changelog.sql"))[:2]
    ops: dict[str, int] = {}
    for row in top:
        ops[row[0]] = ops.get(row[0], 0) + 1
    upserts = p27.java("upserts", "--topic", TOP_TOPIC)
    return {"store": store, "top_ops": ops, "upserts": upserts}


def cmd_changelog(_: argparse.Namespace) -> None:
    r = changelog()
    store = r["store"]
    print(f"SG-0042, business date 2026-08-15: {len(store)} changes (first 5 and the last)")
    print(f"{'op':3} {'store_id':8} {'baskets':>7} {'sales_minor':>11}")
    for op, sid, baskets, sales in store[:5] + store[-1:]:
        print(f"{op:3} {sid:8} {baskets:>7} {int(sales):>11,}")
    ops = ", ".join(f"{k} {v:,}" for k, v in sorted(r["top_ops"].items()))
    print(f"top 3 stores per country: {sum(r['top_ops'].values()):,} changes ({ops})")
    print(r["upserts"].rstrip())


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    commands = {
        "setup": cmd_setup,
        "explain": cmd_explain,
        "dedup": cmd_dedup,
        "errors": cmd_errors,
        "run": cmd_run,
        "tables": cmd_tables,
        "asof": cmd_asof,
        "reconcile": cmd_reconcile,
        "changelog": cmd_changelog,
    }
    for name in commands:
        cmd = sub.add_parser(name)
        if name == "run":
            cmd.add_argument("--mini-batch", action="store_true", help="buffer updates: one per key per batch")
    a = p.parse_args()
    commands[a.cmd](a)


if __name__ == "__main__":
    main()
