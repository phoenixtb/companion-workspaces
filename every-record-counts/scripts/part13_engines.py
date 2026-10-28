"""Part 13: one Iceberg table, three ways to serve it: Trino (MPP over the lake), Trino federated with PostgreSQL,
and ClickHouse (reading the lake through Polaris, then from its own MergeTree storage).

uv run python -m scripts.part13_engines reset        # drop Part 13's ClickHouse databases and PostgreSQL schema
uv run python -m scripts.part13_engines seed         # store-ops sales targets in PostgreSQL (needs make up-core)
uv run python -m scripts.part13_engines trino        # Trino's nodes and plan; Part 11's queries (make up-trino)
uv run python -m scripts.part13_engines federate     # Iceberg joined with PostgreSQL, and what each side was sent
uv run python -m scripts.part13_engines clickhouse   # Part 11's queries in ClickHouse over Iceberg (make up-olap)
uv run python -m scripts.part13_engines load         # copy the lines into a MergeTree table and run them again
uv run python -m scripts.part13_engines serve        # a refreshable materialized view and the query cache
uv run python -m scripts.part13_engines dashboard [--levels 1,8,32] [--queries 256]   # the 9 a.m. load test

Needs Part 10's `part10.pos_monthly` (built by `scripts.part10_layout`). Never start ClickHouse while Spark runs:
the three engines don't fit the lab's 9.8 GB Docker VM together.
"""

import argparse
import base64
import json
import random
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql/part13"
TRINO = "http://localhost:8080/v1/statement"
CLICKHOUSE = "http://localhost:8123/"
CH_AUTH = base64.b64encode(b"larkspur:larkspur-lab-secret").decode()
PG = "host=localhost port=5432 dbname=larkspur user=larkspur password=larkspur"
TRINO_TABLE = "lakehouse.part10.pos_monthly"
LAKE_TABLE = "lake.`part10.pos_monthly`"  # ClickHouse: one namespace level, so namespace.table is one quoted name
WARM_RUNS = 3


# ---------- clients: both engines speak HTTP, so the lab needs no driver ----------
def trino(sql: str, session: dict | None = None) -> tuple[list[str], list[list], dict]:
    headers = {"X-Trino-User": "rohan"}
    if session:
        headers["X-Trino-Session"] = ",".join(f"{k}={v}" for k, v in session.items())
    page = json.load(urllib.request.urlopen(urllib.request.Request(TRINO, data=sql.encode(), headers=headers)))
    columns, rows = [], []
    while True:
        if "error" in page:
            raise RuntimeError(page["error"]["message"])
        columns = columns or [c["name"] for c in page.get("columns", [])]
        rows += page.get("data", [])
        if "nextUri" not in page:
            return columns, rows, page["stats"]
        page = json.load(urllib.request.urlopen(page["nextUri"]))


def clickhouse(sql: str, fmt: str = "JSONCompact", **settings) -> dict | str:
    settings = {"output_format_json_quote_64bit_integers": 0, **settings}  # numbers as numbers, as Trino sends them
    url = CLICKHOUSE + "?" + "&".join(f"{k}={v}" for k, v in settings.items())
    body = sql if not fmt else f"{sql}\nFORMAT {fmt}"
    req = urllib.request.Request(url, data=body.encode(), headers={"Authorization": f"Basic {CH_AUTH}"})
    try:
        text = urllib.request.urlopen(req).read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode().strip()) from None
    return json.loads(text) if fmt == "JSONCompact" else text


def statements(path: Path) -> list[str]:
    code = "\n".join(line for line in path.read_text().splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in code.split(";") if s.strip()]


def show(columns: list[str], rows: list[list], limit: int | None = None) -> None:
    if len(columns) == 1 and len(rows) == 1 and "\n" in str(rows[0][0]):  # EXPLAIN output: one text cell
        print(rows[0][0])
        return
    cells = [[str(c) for c in columns]] + [
        ["NULL" if v is None else f"{v:,}" if isinstance(v, int) else str(v) for v in r] for r in rows[:limit]
    ]
    widths = [max(len(r[i]) for r in cells) for i in range(len(columns))]
    for r in cells:
        print("  ".join(v.rjust(w) for v, w in zip(r, widths, strict=True)))
    if limit and len(rows) > limit:
        print(f"... {len(rows) - limit} more rows")


def mib(n: float) -> str:
    return f"{n / 1048576:,.1f}"


# ---------- the shared queries: Part 11's SQL files, unchanged ----------
def part11(name: str, table: str) -> str:
    from scripts.part11_single_node import query_sql

    return query_sql(name, table)


def digest(rows) -> str:
    from scripts.part11_single_node import digest as d

    return d(rows)


HEADER = (
    f"{'engine':<26}{'query':<9}{'first_s':>8}{'warm_s':>8}{'rows':>6}  {'digest':<9}{'read_MiB':>10}{'peak_MiB':>10}"
)


def report(engine: str, query: str, first: float, warm: list[float], rows: list, read: float, peak: float, extra=""):
    w = f"{statistics.median(warm):.2f}" if warm else "-"
    print(
        f"{engine:<26}{query:<9}{first:8.2f}{w:>8}{len(rows):6}  {digest(rows):<9}{mib(read):>10}{mib(peak):>10}"
        + (f"  {extra}" if extra else "")
    )


# ---------- reset and seed ----------
def reset() -> None:
    import psycopg

    for db in ("serve", "larkspur", "lake"):
        try:
            clickhouse(f"DROP DATABASE IF EXISTS {db} SYNC", fmt="")
        except (RuntimeError, urllib.error.URLError) as e:
            print(f"ClickHouse not reachable, skipped ({e})")
            break
    with psycopg.connect(PG, autocommit=True) as pg:
        pg.execute("DROP SCHEMA IF EXISTS store_ops CASCADE")
    print("dropped ClickHouse databases lake, larkspur, serve and PostgreSQL schema store_ops")


def seed() -> None:
    import duckdb
    import psycopg

    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    targets = con.execute((SQL / "postgres/targets.sql").read_text()).fetchall()
    with psycopg.connect(PG, autocommit=True) as pg:
        pg.execute((SQL / "postgres/store_ops.sql").read_text())
        with pg.cursor() as cur:
            with cur.copy("COPY store_ops.sales_targets FROM STDIN") as copy:
                for row in targets:
                    copy.write_row(row)
            cur.execute("ANALYZE store_ops.sales_targets")
            cur.execute(
                "SELECT count(*), count(DISTINCT store_id), min(month), max(month) FROM store_ops.sales_targets"
            )
            n, stores, first, last = cur.fetchone()
    print(f"store_ops.sales_targets: {n:,} rows, {stores} stores, {first} to {last} (read-only role trino_reader)")


# ---------- Trino ----------
def trino_measure(name: str, session: dict | None = None) -> None:
    sql = part11(name, TRINO_TABLE)
    timings, rows, stats = [], [], {}
    for _ in range(1 + WARM_RUNS):
        started = time.monotonic()
        try:
            _, rows, stats = trino(sql, session)
        except RuntimeError as e:
            print(f"{'trino (Iceberg)':<26}{name:<9}  ERROR: {e}")
            return
        timings.append(time.monotonic() - started)
    report(
        "trino (Iceberg)",
        name,
        timings[0],
        timings[1:],
        rows,
        stats["physicalInputBytes"],
        stats["peakMemoryBytes"],
        f"cpu {stats['cpuTimeMillis'] / 1000:.1f}s",
    )


def trino_steps() -> None:
    show(*trino("SELECT node_id, coordinator, node_version, state FROM system.runtime.nodes")[:2])
    print()
    for statement in statements(SQL / "trino/plan.sql"):
        show(*trino(statement)[:2])
    print()
    print(HEADER)
    for name in ("history", "baskets"):
        trino_measure(name)
    heap = trino('SELECT heapmemoryusage FROM jmx.current."java.lang:type=memory"')[1][0][0]
    pool = trino('SELECT maxbytes FROM jmx.current."trino.memory:type=memorypool,name=general"')[1][0][0]
    heap_max = int(heap.split("max=")[1].split(",")[0])
    print(
        f"JVM heap {mib(heap_max)} MiB; query memory pool {mib(pool)} MiB; per query per node: 30% of the heap,"
        f" {mib(heap_max * 0.3)} MiB"
    )


def federate() -> None:
    for statement in statements(SQL / "trino/federation.sql"):
        print(f"> {statement.splitlines()[0][:100]}")
        started = time.monotonic()
        try:
            columns, rows, stats = trino(statement)
        except RuntimeError as e:
            print(f"ERROR: {e}\n")
            continue
        show(columns, rows, limit=8)
        print(f"({time.monotonic() - started:.2f}s, {stats['processedRows']:,} rows read)\n")


# ---------- ClickHouse ----------
def ch_stats(tag: str) -> dict:
    """The last finished query carrying this comment, from ClickHouse's own query log."""
    for _ in range(50):  # the log entry is written just after the result is sent: wait for it
        clickhouse("SYSTEM FLUSH LOGS", fmt="")
        data = clickhouse(
            "SELECT query_duration_ms, read_rows, memory_usage, ProfileEvents['ReadBufferFromS3Bytes'],"
            " ProfileEvents['ExternalAggregationCompressedBytes'], ProfileEvents['SelectedMarks'],"
            " ProfileEvents['SelectedParts'], ProfileEvents['QueryCacheHits']"
            f" FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE '%/* {tag} */%'"
            " AND query NOT LIKE '%system.query_log%' ORDER BY event_time_microseconds DESC LIMIT 1"
        )["data"]
        if data:
            keys = ("ms", "read_rows", "memory", "s3_bytes", "spill_bytes", "marks", "parts", "cache_hits")
            return dict(zip(keys, data[0], strict=True))
        time.sleep(0.2)
    raise RuntimeError(f"query {tag!r} not found in system.query_log")


def ch_measure(engine: str, name: str, table: str) -> None:
    timings, rows, st = [], [], {}
    for i in range(1 + WARM_RUNS):
        tag = f"part13 {engine} {name} {i} {time.time_ns()}"
        started = time.monotonic()
        rows = clickhouse(f"/* {tag} */ {part11(name, table)}")["data"]
        timings.append(time.monotonic() - started)
        st = ch_stats(tag)
    read = st["s3_bytes"] if st["s3_bytes"] else 0
    extra = f"spilled {mib(st['spill_bytes'])} MiB" if st["spill_bytes"] else ""
    report(engine, name, timings[0], timings[1:], rows, read, st["memory"], extra)


def ch_file(name: str, **settings) -> None:
    for statement in statements(SQL / f"clickhouse/{name}"):
        started = time.monotonic()
        out = clickhouse(statement, fmt="", **settings)
        print(f"> {statement.splitlines()[0][:90]}  ({time.monotonic() - started:.1f}s)")
        if out.strip():
            print(out.rstrip())


def clickhouse_steps() -> None:
    print(clickhouse("SELECT version()", fmt="").strip())
    ch_file("01_lake.sql", allow_database_iceberg=1)
    print(clickhouse("SELECT name FROM system.tables WHERE database = 'lake' AND name LIKE 'part10.%'", fmt="TSV"))
    print(HEADER)
    for name in ("history", "baskets"):
        ch_measure("clickhouse (Iceberg)", name, LAKE_TABLE)


def load() -> None:
    ch_file("02_load.sql")
    print(
        clickhouse(
            "SELECT count() AS parts, sum(rows) AS rows, formatReadableSize(sum(data_compressed_bytes)) AS compressed,"
            " formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed FROM system.parts"
            " WHERE database = 'larkspur' AND table = 'pos_lines' AND active",
            fmt="PrettyCompactMonoBlock",
        )
    )
    print(HEADER)
    for name in ("history", "baskets"):
        ch_measure("clickhouse (MergeTree)", name, "larkspur.pos_lines")
    # One dashboard tile: the sorting key lets the sparse index skip all but a few granules of 8,192 rows.
    tag = f"part13 index {time.time_ns()}"
    clickhouse(f"/* {tag} */ " + tile_sql("tile_lines.sql", "larkspur.pos_lines", "GB-0042"))
    st = ch_stats(tag)
    total = clickhouse("SELECT sum(marks) FROM system.parts WHERE database = 'larkspur' AND active")["data"][0][0]
    print(f"tile for GB-0042: {st['ms']} ms, read {st['read_rows']:,} rows in {st['marks']} of {total:,} marks")


def serve_wait() -> None:
    """A refresh runs in the background; wait until the first one has succeeded."""
    for _ in range(300):
        done = clickhouse(
            "SELECT count() FROM system.view_refreshes WHERE database = 'serve' AND view = 'daily_store_sales_mv'"
            " AND status = 'Scheduled' AND last_success_duration_ms > 0"
        )["data"]
        if done == [[1]]:
            return
        time.sleep(1)
    raise RuntimeError("the materialized view's first refresh didn't finish in 5 minutes")


def serve() -> None:
    ch_file("03_serve.sql")
    serve_wait()
    print(
        clickhouse(
            "SELECT view, status, last_success_time, last_success_duration_ms, next_refresh_time, read_rows,"
            " written_rows FROM system.view_refreshes WHERE database = 'serve'",
            fmt="Vertical",
        )
    )
    tile = (SQL / "clickhouse/country_tile.sql").read_text().strip().rstrip(";")
    for i in range(3):
        tag = f"part13 cache {i} {time.time_ns()}"
        started = time.monotonic()
        rows = clickhouse(f"/* {tag} */ {tile}", use_query_cache=1, query_cache_ttl=300)["data"]
        ms = (time.monotonic() - started) * 1000
        st = ch_stats(tag)
        print(
            f"country tile, run {i + 1}: {ms:6.1f} ms, {len(rows)} rows, server read {st['read_rows']:,} rows,"
            f" query cache hits {st['cache_hits']}"
        )


# ---------- the 9 a.m. load test ----------
def tile_sql(file: str, table: str, store: str) -> str:
    text = (SQL / file).read_text()
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--")).format(
        table=table, store=store
    )


def dashboard(levels: list[int], n: int) -> None:
    stores = [s for (s,) in clickhouse("SELECT DISTINCT store_id FROM serve.daily_store_sales ORDER BY 1")["data"]]
    targets = [
        ("trino", "Trino over Iceberg", "tile_lines.sql", TRINO_TABLE),
        ("clickhouse", "ClickHouse over Iceberg", "tile_lines.sql", LAKE_TABLE),
        ("clickhouse", "ClickHouse MergeTree lines", "tile_lines.sql", "larkspur.pos_lines"),
        ("clickhouse", "ClickHouse serving table", "tile_daily.sql", "serve.daily_store_sales"),
    ]
    print(f"{n} tile queries per level, random stores (seed 13); latency per query in ms")
    print(f"{'target':<28}{'clients':>8}{'p50':>9}{'p95':>9}{'max':>9}{'qps':>8}")
    for engine, label, file, table in targets:
        for level in levels:
            rng = random.Random(13)
            work = [tile_sql(file, table, rng.choice(stores)) for _ in range(n)]

            def one(sql: str, engine: str = engine) -> float:
                started = time.monotonic()
                trino(sql) if engine == "trino" else clickhouse(sql)
                return (time.monotonic() - started) * 1000

            one(work[0])  # warm the catalog and metadata caches once per level
            started = time.monotonic()
            with ThreadPoolExecutor(level) as pool:
                lat = sorted(pool.map(one, work))
            wall = time.monotonic() - started
            p95 = lat[int(0.95 * (len(lat) - 1))]
            print(f"{label:<28}{level:>8}{statistics.median(lat):9.0f}{p95:9.0f}{lat[-1]:9.0f}{n / wall:8.1f}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "command", choices=["reset", "seed", "trino", "federate", "clickhouse", "load", "serve", "dashboard"]
    )
    p.add_argument("--levels", default="1,8,32")
    p.add_argument("--queries", type=int, default=256)
    a = p.parse_args()
    if a.command == "dashboard":
        dashboard([int(x) for x in a.levels.split(",")], a.queries)
    else:
        {
            "reset": reset,
            "seed": seed,
            "trino": trino_steps,
            "federate": federate,
            "clickhouse": clickhouse_steps,
            "load": load,
            "serve": serve,
        }[a.command]()
