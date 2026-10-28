"""Part 16: orders, payments and refunds from PostgreSQL into Iceberg with change data capture.

uv run python -m scripts.part16_cdc reset            # connector, slot, topics, Iceberg tables, checkpoint, shop schema
uv run python -m scripts.part16_cdc setup            # shop schema; the database as it stood at the cut-over (1 July)
uv run python -m scripts.part16_cdc connect          # register the Debezium connector; wait for its initial snapshot
uv run python -m scripts.part16_cdc replay [--until 2026-10-05T01:00:00Z]   # every later change, in event-time order
uv run python -m scripts.part16_cdc topics           # end offsets per topic
uv run python -m scripts.part16_cdc events ORDER_ID  # the change events Kafka holds for one order
uv run python -m scripts.part16_cdc stream [--max-offsets N] [--fail-after-batch N]   # spark/jobs/part16_cdc.py
uv run python -m scripts.part16_cdc order ORDER_ID   # one order in the poller's extracts, PostgreSQL and the lake
uv run python -m scripts.part16_cdc reconcile        # counts and sums: PostgreSQL vs the Iceberg replicas
uv run python -m scripts.part16_cdc incident         # revenue step 2: the poller's orders vs the CDC replica
uv run python -m scripts.part16_cdc maintain         # delete files after streaming MERGEs, then compaction

Needs `make up-core`, `make up-stream` (Kafka, Connect) and, for stream/maintain, `make up-spark`.

The lab can't wait three months for orders to happen, so it replays the generator's history (seed 42): `setup`
loads the state at the cut-over with COPY, Debezium snapshots it, and `replay` applies every later insert, update
and delete in event-time order, one transaction per hour of event time, as fast as PostgreSQL takes them.
"""

import argparse
import json
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl
import psycopg

from scripts import part04_star as star
from scripts import part07_iceberg as p7
from scripts.part09_catalog import PRINCIPALS
from scripts.part11_single_node import container_peak

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql/part16"
PG = "host=localhost port=5432 dbname=larkspur user=larkspur password=larkspur"
CONNECT = "http://localhost:8083"
CONNECTOR = "larkspur-shop"
SLOT = "larkspur_shop"
CONNECTOR_JSON = ROOT / "kafka/connectors/larkspur-shop.json"
TABLES = ("orders", "payments", "refunds")
KEYS = {"orders": "order_id", "payments": "payment_id", "refunds": "refund_id"}
TOPICS = [f"larkspur.shop.{t}" for t in TABLES]
CUTOVER = datetime(2026, 7, 1, tzinfo=UTC)
END = datetime(2026, 10, 5, 1, tzinfo=UTC)  # when the 5 October extract ran (datagen.output.SNAPSHOT_AT)
COLS = {
    "orders": ["order_id", "customer_id", "country", "currency", "channel", "status", "total_minor", "tender_type",
               "created_at", "updated_at", "fulfilled_at"],
    "payments": ["payment_id", "source", "reference", "country", "currency", "tender_type", "amount_minor", "status",
                 "authorised_at", "captured_at", "voided_at"],
    "refunds": ["refund_id", "original_channel", "original_reference", "country", "store_id", "currency",
                "tender_type", "refund_ts", "amount_minor", "reason"],
}  # fmt: skip
OP_RANK = {"c": 0, "u": 1, "d": 2}


# ---------- the source: the generator's history as row changes ----------


def history() -> dict[str, pl.DataFrame]:
    """Every insert (c), update (u) and delete (d) the order and payment services made, with the row after it."""
    from datagen import reference, sales

    s = sales.build(42, 1.0, reference.build(42, 1.0))
    o = s.orders
    placed = o.with_columns(status=pl.lit("placed"), updated_at=pl.col("created_at"),
                            fulfilled_at=pl.lit(None, o["created_at"].dtype))  # fmt: skip
    orders = pl.concat([
        placed.with_columns(ts=pl.col("created_at"), op=pl.lit("c")),
        o.filter(pl.col("fulfilled_at").is_not_null()).with_columns(
            status=pl.lit("fulfilled"), updated_at=pl.col("fulfilled_at"), ts=pl.col("fulfilled_at"), op=pl.lit("u")),
        placed.filter(pl.col("cancelled")).with_columns(ts=pl.col("deleted_at"), op=pl.lit("d")),  # a hard DELETE
    ], how="diagonal_relaxed")  # fmt: skip
    p = s.payments.with_columns(voided_at=pl.col("voided_at").cast(s.payments["authorised_at"].dtype))
    null_ts = pl.lit(None, p["authorised_at"].dtype)
    payments = pl.concat([
        p.filter(pl.col("source") == "pos").with_columns(status=pl.lit("captured"), ts=pl.col("authorised_at"),
                                                         op=pl.lit("c")),
        p.filter(pl.col("source") == "online").with_columns(status=pl.lit("authorised"), captured_at=null_ts,
                                                            voided_at=null_ts, ts=pl.col("authorised_at"),
                                                            op=pl.lit("c")),
        p.filter(pl.col("source") == "online", pl.col("captured_at").is_not_null()).with_columns(
            status=pl.lit("captured"), voided_at=null_ts, ts=pl.col("captured_at"), op=pl.lit("u")),
        p.filter(pl.col("voided_at").is_not_null()).with_columns(  # a void is a soft delete: an UPDATE
            status=pl.lit("voided"), captured_at=null_ts, ts=pl.col("voided_at"), op=pl.lit("u")),
    ])  # fmt: skip
    refunds = s.refunds.with_columns(ts=pl.col("refund_ts"), op=pl.lit("c"))
    out = {}
    for name, df in (("orders", orders), ("payments", payments), ("refunds", refunds)):
        df = df.filter(pl.col("ts") <= END).with_columns(rank=pl.col("op").replace_strict(OP_RANK))
        out[name] = df.select("ts", "op", "rank", *COLS[name]).sort("ts", "rank")
    return out


def state_at(events: pl.DataFrame, key: str, at: datetime) -> pl.DataFrame:
    """The table as it stood at `at`: the last change per key, minus deleted keys."""
    last = events.filter(pl.col("ts") <= at).unique(key, keep="last", maintain_order=True)
    return last.filter(pl.col("op") != "d")


def pg() -> psycopg.Connection:
    return psycopg.connect(PG, autocommit=True)


def setup(cutover: datetime) -> None:
    started = time.monotonic()
    h = history()
    with pg() as con:
        con.execute((SQL / "postgres/shop.sql").read_text())
        con.execute("CREATE SCHEMA IF NOT EXISTS part16_lab")  # the replayer's bookmark; not published, not captured
        con.execute("CREATE TABLE IF NOT EXISTS part16_lab.replay (applied_until timestamptz NOT NULL)")
        con.execute("DELETE FROM part16_lab.replay")
        con.execute("INSERT INTO part16_lab.replay VALUES (%s)", (cutover,))
        for t in TABLES:
            rows = state_at(h[t], KEYS[t], cutover).select(COLS[t])
            with con.cursor() as cur, cur.copy(f"COPY shop.{t} ({', '.join(COLS[t])}) FROM STDIN") as copy:
                for r in rows.iter_rows():
                    copy.write_row(r)
            con.execute(f"ANALYZE shop.{t}")
            print(f"shop.{t:<9} {rows.height:>9,} rows as of {cutover:%Y-%m-%d %H:%M} UTC")
    print(f"loaded in {time.monotonic() - started:.0f} s (orders: REPLICA IDENTITY FULL; publication larkspur_shop)")


def replay(until: datetime) -> None:
    """Apply every change after the bookmark up to `until`, one transaction per hour of event time."""
    started = time.monotonic()
    h = history()
    with pg() as con:
        since = con.execute("SELECT applied_until FROM part16_lab.replay").fetchone()[0].astimezone(UTC)
        todo = {
            t: h[t]
            .filter(pl.col("ts") > since, pl.col("ts") <= until)
            .with_columns(hour=pl.col("ts").dt.truncate("1h"))
            for t in TABLES
        }
        hours = sorted(set().union(*(set(df["hour"].unique().to_list()) for df in todo.values())))
        counts = {(t, op): 0 for t in TABLES for op in "cud"}
        parts = {t: dict(df.partition_by("hour", as_dict=True, maintain_order=True)) for t, df in todo.items()}
        for hour in hours:
            with con.transaction(), con.cursor() as cur:
                for t in TABLES:
                    chunk = parts[t].get((hour,))
                    if chunk is None:
                        continue
                    key, cols = KEYS[t], COLS[t]
                    # Inserts, then updates, then deletes: per key that is always the order they happened in.
                    for op, df in sorted(
                        chunk.partition_by("op", as_dict=True).items(), key=lambda kv: OP_RANK[kv[0][0]]
                    ):
                        op = op[0]
                        if op == "c":
                            sql = f"INSERT INTO shop.{t} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
                            rows = df.select(cols).rows()
                        elif op == "u":
                            rest = [c for c in cols if c != key]
                            sql = f"UPDATE shop.{t} SET {', '.join(f'{c} = %s' for c in rest)} WHERE {key} = %s"
                            rows = df.select(*rest, key).rows()
                        else:
                            sql = f"DELETE FROM shop.{t} WHERE {key} = %s"
                            rows = df.select(key).rows()
                        cur.executemany(sql, rows)
                        counts[(t, op)] += len(rows)
                # The bookmark moves in the same transaction as the changes, so an interrupted replay resumes cleanly.
                cur.execute("UPDATE part16_lab.replay SET applied_until = %s", (min(until, hour + timedelta(hours=1)),))
        con.execute("UPDATE part16_lab.replay SET applied_until = %s", (until,))
    took = time.monotonic() - started
    total = sum(counts.values())
    print(f"replayed {since:%Y-%m-%d %H:%M} -> {until:%Y-%m-%d %H:%M} UTC: {len(hours):,} transactions, "
          f"{total:,} changes in {took:.0f} s")  # fmt: skip
    for t in TABLES:
        print(f"  shop.{t:<9} " + "  ".join(f"{op}={counts[(t, op)]:>9,}" for op in "cud"))


# ---------- Kafka Connect and Kafka ----------


def http(method: str, path: str, body: dict | None = None) -> tuple[int, dict | str]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(CONNECT + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})  # fmt: skip
    try:
        with urllib.request.urlopen(req) as r:
            text = r.read().decode()
            return r.status, json.loads(text) if text else ""
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def kafka(*args: str) -> str:
    cmd = ["docker", "compose", "exec", "-T", "kafka", *args]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True).stdout


def end_offsets() -> dict[str, int]:
    out = kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:29092",
                "--topic", "larkspur\\.shop\\..*")  # fmt: skip
    totals: dict[str, int] = {}
    for line in out.split():  # topic:partition:offset
        topic, _, n = line.rsplit(":", 2)
        totals[topic] = totals.get(topic, 0) + int(n)
    return totals


def topics() -> None:
    """End offsets per topic, once Debezium has caught up (two readings 3 s apart agree)."""
    last, now = None, end_offsets()
    while now != last:
        time.sleep(3)
        last, now = now, end_offsets()
    for t, n in sorted(now.items()):
        print(f"{t:<24} {n:>10,} events")


def connect() -> None:
    config = json.loads(CONNECTOR_JSON.read_text())
    with pg() as con:
        expected = {f"larkspur.shop.{t}": con.execute(f"SELECT count(*) FROM shop.{t}").fetchone()[0] for t in TABLES}
    started = time.monotonic()
    status, body = http("PUT", f"/connectors/{CONNECTOR}/config", config)
    if status not in (200, 201):
        raise SystemExit(f"Connect said {status}: {body}")
    print(
        f"connector {CONNECTOR}: {'created' if status == 201 else 'updated'}, snapshot.mode={config['snapshot.mode']}"
    )
    while True:  # the initial snapshot is done when every row is in its topic
        time.sleep(2)
        try:
            got = end_offsets()
        except subprocess.CalledProcessError:
            continue
        if all(got.get(t, 0) >= n for t, n in expected.items()):
            break
        if time.monotonic() - started > 900:
            raise SystemExit(f"snapshot not finished after 15 minutes: {got}")
    print(f"initial snapshot in Kafka after {time.monotonic() - started:.0f} s:")
    for t, n in expected.items():
        print(f"  {t:<24} {got[t]:>9,} events for {n:,} rows")
    _, st = http("GET", f"/connectors/{CONNECTOR}/status")
    with pg() as con:
        slot = con.execute("SELECT slot_name, plugin, active, confirmed_flush_lsn::text FROM pg_replication_slots "
                           "WHERE slot_name = %s", (SLOT,)).fetchone()  # fmt: skip
    print(f"connector {st['connector']['state']}, task {st['tasks'][0]['state']}; slot {slot[0]} ({slot[1]}), "
          f"active={slot[2]}, confirmed_flush_lsn={slot[3]}")  # fmt: skip


def events(key: str) -> None:
    """Every event Kafka holds for one order key (orders topic), trimmed to what the post needs."""
    out = kafka("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:29092",
                "--topic", "larkspur.shop.orders", "--from-beginning", "--timeout-ms", "20000",
                "--formatter-property", "print.key=true", "--formatter-property", "key.separator=|")  # fmt: skip
    for line in out.splitlines():
        k, _, v = line.partition("|")
        if json.loads(k).get("order_id") != key:
            continue
        e = json.loads(v)
        row = e["after"] or e["before"]
        src = e["source"]
        print(json.dumps({"op": e["op"], "lsn": src["lsn"], "txId": src["txId"], "snapshot": src["snapshot"],
                          "before": _short(e["before"]), "after": _short(e["after"]),
                          "status": row["status"], "total_minor": row["total_minor"]}))  # fmt: skip


def _short(row: dict | None) -> dict | str | None:
    return None if row is None else {k: row[k] for k in ("order_id", "status", "updated_at")}


def remove_connector() -> None:
    status, _ = http("GET", f"/connectors/{CONNECTOR}")
    if status == 200:
        http("PUT", f"/connectors/{CONNECTOR}/stop")  # offsets can only be deleted while STOPPED
        for _ in range(60):
            _, st = http("GET", f"/connectors/{CONNECTOR}/status")
            if st["connector"]["state"] == "STOPPED":
                break
            time.sleep(1)
        http("DELETE", f"/connectors/{CONNECTOR}/offsets")
        http("DELETE", f"/connectors/{CONNECTOR}")
    with pg() as con:
        for _ in range(60):  # the slot stays active until the task has shut down
            row = con.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone()
            if row is None:
                return
            if not row[0]:
                con.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
                return
            time.sleep(1)
        raise SystemExit(f"replication slot {SLOT} still active")


# ---------- Spark ----------

SPARK_PIPELINE = {  # Part 9's pipeline principal; Polaris vends storage credentials per table
    "spark.sql.catalog.pipeline": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.pipeline.type": "rest",
    "spark.sql.catalog.pipeline.uri": "http://polaris:8181/api/catalog",
    "spark.sql.catalog.pipeline.warehouse": "larkspur",
    "spark.sql.catalog.pipeline.credential": f"pipeline:{PRINCIPALS['pipeline']}",
    "spark.sql.catalog.pipeline.scope": "PRINCIPAL_ROLE:ALL",
    "spark.sql.catalog.pipeline.header.X-Iceberg-Access-Delegation": "vended-credentials",
    "spark.sql.catalog.pipeline.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.pipeline.s3.endpoint": "http://seaweedfs:8333",
    "spark.sql.catalog.pipeline.s3.path-style-access": "true",
    "spark.sql.catalog.pipeline.client.region": "us-east-1",
}


def submit(job: str, args: list[str], sql: str | None = None) -> int:
    stop, peaks = threading.Event(), [0.0]
    sampler = threading.Thread(target=container_peak, args=("every-record-counts-spark", stop, peaks), daemon=True)
    sampler.start()
    cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-submit", "--master", "local[*,3]"]
    for k, v in SPARK_PIPELINE.items():
        cmd += ["--conf", f"{k}={v}"]
    cmd += [f"/opt/lakehouse/jobs/{job}", *args]
    sys.stdout.flush()
    started = time.monotonic()
    run = subprocess.run(cmd, cwd=ROOT, stderr=subprocess.PIPE, text=True)
    stop.set()
    sampler.join()
    if run.returncode:
        lines = [x for x in run.stderr.splitlines() if "Error" in x or "Exception" in x]
        print("\n".join(lines[:5]) or run.stderr[-3000:], file=sys.stderr)
    print(f"spark-submit {job} {' '.join(args)}: exit {run.returncode}, {time.monotonic() - started:.0f} s, "
          f"Spark container peak {max(peaks):,.0f} MiB")  # fmt: skip
    return run.returncode


def stream(max_offsets: int, fail_after: int | None) -> int:
    args = ["--max-offsets", str(max_offsets)]
    if fail_after is not None:
        args += ["--fail-after-batch", str(fail_after)]
    return submit("part16_cdc.py", args)


# ---------- the lake side, read on the host ----------


def lake(con: duckdb.DuckDBPyConnection, *names: str) -> None:
    """Register Iceberg tables (PyIceberg scan, position deletes applied) as DuckDB views."""
    cat = p7.catalog()
    for name in names:
        arrow = cat.load_table(f"raw.{name}").scan().to_arrow()
        con.register(name.replace(".", "_"), arrow)


def order(key: str) -> None:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    poll = con.execute(
        "SELECT extract_date, status, total_minor, currency FROM read_parquet(?, hive_partitioning = true) "
        "WHERE order_id = ? ORDER BY extract_date",
        [str(ROOT / "data/raw/oltp/orders_poll/*/*.parquet"), key],
    ).fetchall()
    h = history()["orders"].filter(pl.col("order_id") == key)
    print("generator: " + ", ".join(f"{op} at {ts:%Y-%m-%d %H:%M:%S} UTC" for op, ts in h.select("op", "ts").rows()))
    print(f"legacy poller extracts: {poll or 'none'}")
    with pg() as pgc:
        row = pgc.execute("SELECT status FROM shop.orders WHERE order_id = %s", (key,)).fetchall()
    print(f"PostgreSQL shop.orders: {row or 'no row'}")
    lake(con, "orders_changes", "orders")
    hist = con.execute(
        "SELECT _op, _lsn, _snapshot, status, total_minor, currency, created_at, updated_at FROM orders_changes "
        "WHERE order_id = ? ORDER BY _lsn",
        [key],
    ).fetchall()
    print("raw.orders_changes:")
    for r in hist:
        print(f"  op={r[0]} lsn={r[1]} snapshot={r[2]} status={r[3]} total_minor={r[4]} {r[5]} "
              f"created_at={r[6]:%Y-%m-%d %H:%M:%S} updated_at={r[7]:%Y-%m-%d %H:%M:%S}")  # fmt: skip
    cur = con.execute("SELECT status FROM orders WHERE order_id = ?", [key]).fetchall()
    print(f"raw.orders (current state): {cur or 'no row'}")


def run_sql(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple]:
    """Run each statement of a SQL file, print its result as a table, return the first result's rows."""
    first = None
    for stmt in con.extract_statements(path.read_text()):
        query = "\n".join(line for line in stmt.query.splitlines() if not line.lstrip().startswith("--"))
        rel = con.sql(query.strip().rstrip(";"))
        if rel is not None:
            rel.show(max_width=140)
            first = rel.fetchall() if first is None else first
    return first or []


def reconcile() -> list[tuple]:
    started = time.monotonic()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{PG}' AS pg (TYPE postgres, READ_ONLY)")
    lake(con, *TABLES)
    rows = run_sql(con, SQL / "reconcile.sql")
    print(f"({time.monotonic() - started:.0f} s)")
    return rows


def incident() -> dict[str, tuple]:
    """Revenue step 2: the board deck's online-orders sum from the poller's extracts vs the CDC replica."""
    con = duckdb.connect()
    con.execute(f"SET VARIABLE raw_root = '{ROOT / 'data/raw'}'")
    con.execute((ROOT / "sql/raw/views.sql").read_text())
    lake(con, "orders", "orders_changes")
    rows = con.sql((SQL / "incident.sql").read_text().strip().rstrip(";")).fetchall()
    print(f"{'source (UK online orders, Q3 by UTC date)':<42} {'orders':>7} {'in EUR':>7} {'GBP':>15}")
    for source, n, eur, minor in rows:
        print(f"{source:<42} {n:>7,} {eur:>7,} {minor / 100:>15,.2f}")
    by = {r[0]: r for r in rows}
    gap = by["1 poller extracts (board deck)"][3] - by["2 CDC replica raw.orders"][3]
    print(f"{'board deck minus CDC replica':<42} {'':>7} {'':>7} {gap / 100:>15,.2f}")
    return by


def maintain() -> int:
    return submit("run_sql.py", ["/opt/lakehouse/sql/part16/spark/maintain.sql", "--strict"])


def reset() -> None:
    try:
        remove_connector()
        print(f"connector {CONNECTOR} and slot {SLOT}: removed")
    except (urllib.error.URLError, ConnectionError) as e:
        print(f"Kafka Connect not reachable ({e}); dropping the slot only")
        with pg() as con:
            con.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)
            )
    try:
        existing = kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:29092", "--list").split()
        for t in [x for x in existing if x.startswith("larkspur.shop.") or x == "__debezium-heartbeat.larkspur"]:
            kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:29092", "--delete", "--topic", t)
        print("topics larkspur.shop.*: deleted")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Kafka not running: topics left alone")
    cat = p7.catalog()
    fs = star.fs()
    for t in TABLES:
        for name in (t, f"{t}_changes"):
            if cat.table_exists(f"raw.{name}"):
                cat.drop_table(f"raw.{name}")
            prefix = f"larkspur-lake/warehouse/raw/{name}"
            if fs.exists(prefix):  # a drop forgets the table; the files stay (Part 1)
                fs.rm(prefix, recursive=True)
    print("Iceberg raw.{orders,payments,refunds}{,_changes}: dropped")
    running = subprocess.run(["docker", "compose", "ps", "--status", "running", "--services"], cwd=ROOT,
                             capture_output=True, text=True).stdout.split()  # fmt: skip
    if "spark" in running:
        subprocess.run(["docker", "compose", "exec", "-T", "spark", "rm", "-rf", "/opt/lakehouse/state/part16"],
                       cwd=ROOT, check=True)  # fmt: skip
        print("Spark checkpoint: deleted")
    with pg() as con:
        con.execute("DROP PUBLICATION IF EXISTS larkspur_shop")
        con.execute("DROP SCHEMA IF EXISTS shop CASCADE")
        con.execute("DROP SCHEMA IF EXISTS part16_lab CASCADE")
    print("PostgreSQL schemas shop, part16_lab and publication larkspur_shop: dropped")


def when(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("reset", "connect", "topics", "reconcile", "incident", "maintain"):
        sub.add_parser(name)
    sub.add_parser("setup").add_argument("--cutover", type=when, default=CUTOVER)
    sub.add_parser("replay").add_argument("--until", type=when, default=END)
    for name in ("events", "order"):
        sub.add_parser(name).add_argument("key")
    s = sub.add_parser("stream")
    s.add_argument("--max-offsets", type=int, default=250_000)
    s.add_argument("--fail-after-batch", type=int)
    a = p.parse_args()
    if a.cmd == "setup":
        setup(a.cutover)
    elif a.cmd == "replay":
        replay(min(a.until, END))
    elif a.cmd in ("events", "order"):
        globals()[a.cmd](a.key)
    elif a.cmd == "stream":
        sys.exit(stream(a.max_offsets, a.fail_after_batch))
    elif a.cmd == "maintain":
        sys.exit(maintain())
    else:
        globals()[a.cmd]()
