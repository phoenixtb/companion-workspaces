"""Part 11: the Season 1 revenue and promotion views on one machine, DuckDB and Polars over Part 10's Iceberg tables.

uv run python -m scripts.part11_single_node host                     # cores and memory of this machine
uv run python -m scripts.part11_single_node profile [--threads 4 --memory-mb 1500] [--table pos_monthly]
                                            [--queries revenue,store_promo,history,baskets] [--engines duckdb,polars]
uv run python -m scripts.part11_single_node spark [--table pos_monthly]  # the same queries in Spark (make up-spark)
uv run python -m scripts.part11_single_node explain                  # Polars' plan for the revenue query
uv run python -m scripts.part11_single_node order                    # Polars row order, with and without maintain_order
uv run python -m scripts.part11_single_node arrow                    # DuckDB and Polars hand each other Arrow data
uv run python -m scripts.part11_single_node publish                  # DuckDB writes a gold table, Polars reads it
uv run python -m scripts.part11_single_node lock                     # a second process vs a DuckDB file being written
uv run python -m scripts.part11_single_node oldfile                  # a DuckDB 2.0 file vs DuckDB 1.5.6
uv run python -m scripts.part11_single_node run ENGINE QUERY [...]   # one measurement; `profile` runs one per process

Needs `make up-core` and Part 10's tables (`scripts.part10_layout`; Spark builds them). Stop Spark before profiling
the host engines (`make stop-batch`), so they don't share the machine with it.
"""

import argparse
import hashlib
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import threading
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "data/lab/part11"
QUERIES = ["revenue", "store_promo", "history", "baskets"]
# PyIceberg FileIO keys; Polars translates them for its native Parquet reader (it doesn't use the catalog's own).
STORAGE = {
    "s3.endpoint": "http://localhost:8333",
    "s3.access-key-id": "larkspur",
    "s3.secret-access-key": "larkspur-lab-secret",
    "s3.region": "us-east-1",
}


def digest(rows) -> str:
    """The same normalised hash in every engine: values as text, rows sorted (spark/jobs/part11_spark.py)."""
    return hashlib.sha256(repr(sorted(tuple(str(v) for v in r) for r in rows)).encode()).hexdigest()[:8]


def query_sql(name: str, table: str) -> str:
    text = (ROOT / f"sql/part11/queries/{name}.sql").read_text()
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--")).format(table=table)


def peak_rss_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS, KiB on Linux
    return peak / 1048576 if sys.platform == "darwin" else peak / 1024


def duckdb_connect(threads: int | None = None, memory_mb: int | None = None):
    import duckdb

    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads = {threads}")
    if memory_mb:
        con.execute(f"SET memory_limit = '{memory_mb}MB'")
    LAB.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{LAB / 'duckdb_tmp'}'")  # where spilled intermediates go
    con.execute("SET enable_external_file_cache = false")  # every run fetches from S3 (Part 3's rule)
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    return con


# The Polars versions of sql/part11/queries, in the lazy API. Nothing runs until collect().
def polars_queries():
    import polars as pl

    month = pl.col("business_date").dt.truncate("1mo").alias("month")
    return {
        "revenue": lambda lf: (
            lf.filter(pl.col("business_date").is_between(date(2026, 9, 1), date(2026, 9, 30)))
            .group_by("country", "currency")
            .agg(lines=pl.len(), sales_minor=pl.col("amount_minor").sum())
        ),
        "store_promo": lambda lf: (
            lf.filter(
                (pl.col("store_id") == "GB-0042")
                & (pl.col("discount_minor") > 0)
                & (pl.col("business_date") >= date(2025, 10, 1))
            )
            .group_by(month)
            .agg(promo_lines=pl.len(), discount_minor=pl.col("discount_minor").sum())
        ),
        "history": lambda lf: lf.group_by(month, "country", "currency").agg(
            lines=pl.len(), sales_minor=pl.col("amount_minor").sum(), discount_minor=pl.col("discount_minor").sum()
        ),
        "baskets": lambda lf: (
            lf.group_by("basket_id", "country")
            .agg(lines_in_basket=pl.len(), basket_minor=pl.col("amount_minor").sum())
            .group_by("country", "lines_in_basket")
            .agg(baskets=pl.len(), sales_minor=pl.col("basket_minor").sum())
        ),
    }


def polars_scan(table: str):
    import polars as pl

    from scripts import part07_iceberg as p7

    return pl.scan_iceberg(p7.catalog().load_table(f"part10.{table}"), storage_options=STORAGE)


def timed(fn) -> tuple[float, list]:
    started = time.monotonic()
    rows = fn()
    return time.monotonic() - started, rows


def run(engine: str, query: str, table: str, threads: int | None, memory_mb: int | None) -> dict:
    """One measurement in this process: ready (import + catalog), first run, median of three more, peak RSS."""
    import re

    started = time.monotonic()
    out = {"engine": engine, "table": table, "query": query}
    if engine == "duckdb":
        con = duckdb_connect(threads, memory_mb)
        sql = query_sql(query, f"lake.part10.{table}")
        out["ready"] = time.monotonic() - started
        out["first"], rows = timed(lambda: con.sql(sql).fetchall())
        out["warm"] = statistics.median(timed(lambda: con.sql(sql).fetchall())[0] for _ in range(3))
        plan = con.sql("EXPLAIN ANALYZE " + sql).fetchall()[0][1]  # one more run, for its HTTP stats
        fetched = re.search(r"in: ([\d.]+) (\w+)", plan)
        scale = {"bytes": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}[fetched.group(2)]
        out["fetched_mib"] = float(fetched.group(1)) * scale
        out["gets"] = int(re.search(r"#GET: (\d+)", plan).group(1))
        profile = LAB / f"profile-{os.getpid()}.json"
        con.execute("SET enable_profiling = 'json'")
        con.execute("SET profiling_mode = 'detailed'")
        con.execute(f"SET profiling_output = '{profile}'")
        con.sql(sql).fetchall()  # and one more, for the profiler's peak buffer and temp-directory sizes
        system = json.loads(profile.read_text())["system"]
        profile.unlink()
        out["buffer_mib"] = system["peak_buffer_memory"] / 1048576
        out["spilled_mib"] = system["peak_temp_dir_size"] / 1048576
    else:
        lf = polars_scan(table)
        q = polars_queries()[query](lf)
        out["ready"] = time.monotonic() - started
        out["first"], df = timed(q.collect)
        rows = df.rows()
        out["warm"] = statistics.median(timed(q.collect)[0] for _ in range(3))
    out |= {"rows": len(rows), "digest": digest(rows), "peak_rss_mib": peak_rss_mib()}
    if query == "baskets":  # (country, lines_in_basket, baskets, sales_minor): how many keys the hash table held
        out["baskets"] = sum(int(r[2]) for r in rows)
    return out


def limits_label(threads: int | None, memory_mb: int | None) -> str:
    return f"{threads or 'all'} thr, {f'{memory_mb} MB' if memory_mb else 'default mem'}"


def print_table(results: list[dict], limits: str) -> None:
    header = (
        f"{'engine':<7} {'table':<11} {'query':<11} {'limits':<20} {'ready_s':>7} {'first_s':>7} {'warm_s':>6} "
        f"{'peak_mib':>8} {'s3_mib':>7} {'spill_mib':>9} {'rows':>4}  digest"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        s3 = f"{r['fetched_mib']:.1f}" if "fetched_mib" in r else "-"
        spill = f"{r['spilled_mib']:,.0f}" if "spilled_mib" in r else "-"
        peak = f"{r['peak_rss_mib']:,.0f}" if "peak_rss_mib" in r else "-"
        ready = f"{r['ready']:.2f}" if "ready" in r else "-"
        print(
            f"{r['engine']:<7} {r['table']:<11} {r['query']:<11} {r.get('limits', limits):<20} {ready:>7} "
            f"{r['first']:>7.2f} {r['warm']:>6.2f} {peak:>8} {s3:>7} {spill:>9} {r['rows']:>4}  {r['digest']}"
        )
    counted = {r["baskets"] for r in results if "baskets" in r}
    if counted:
        print(f"baskets query: {', '.join(f'{n:,}' for n in sorted(counted))} baskets (the inner GROUP BY's keys)")


def profile(engines: list[str], queries: list[str], table: str, threads: int | None, memory_mb: int | None) -> list:
    """Each (engine, query) in a fresh process, so peak RSS belongs to that query alone."""
    env = dict(os.environ)
    if threads:
        env["POLARS_MAX_THREADS"] = str(threads)
    if memory_mb:
        # Polars 2.x spills when its streaming engine passes this budget (default: 80% of RAM). The variable is
        # in Polars' source (crates/polars-config), not yet in its user guide.
        env["POLARS_OOC_MEMORY_BUDGET_MB"] = str(memory_mb)
    results = []
    for engine in engines:
        for q in queries:
            cmd = [sys.executable, "-m", "scripts.part11_single_node", "run", engine, q, "--table", table]
            cmd += ["--threads", str(threads)] if threads else []
            cmd += ["--memory-mb", str(memory_mb)] if memory_mb else []
            done = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True)
            if done.returncode:
                print(done.stderr[-2000:], file=sys.stderr)
                raise SystemExit(f"{engine} {q} failed")
            results.append(json.loads(done.stdout.strip().splitlines()[-1]))
    print_table(results, limits_label(threads, memory_mb))
    return results


def container_peak(name: str, stop: threading.Event, peaks: list[float]) -> None:
    """Sample `docker stats` for one service until stopped; the highest reading goes into peaks."""
    units = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}
    while not stop.is_set():
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"], capture_output=True, text=True
        ).stdout
        for line in out.splitlines():
            if name in line.split()[0]:
                used = line.split()[1]
                unit = next(u for u in ("GiB", "MiB", "KiB", "B") if used.endswith(u))
                peaks.append(float(used[: -len(unit)]) * units[unit])
        stop.wait(1.0)


def spark(table: str, queries: list[str]) -> list[dict]:
    """The same queries in the Spark container (local mode, 4 cores of the Docker VM, 1.5 GB driver)."""
    stop, peaks = threading.Event(), [0.0]
    sampler = threading.Thread(target=container_peak, args=("every-record-counts-spark", stop, peaks), daemon=True)
    sampler.start()
    cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-submit", "--master", "local[*]"]
    cmd += ["/opt/lakehouse/jobs/part11_spark.py", table, "--queries", ",".join(queries)]
    started = time.monotonic()
    done = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    stop.set()
    sampler.join()
    if done.returncode:
        print(done.stderr[-3000:], file=sys.stderr)
        raise SystemExit("spark job failed")
    lines = [json.loads(line) for line in done.stdout.splitlines() if line.startswith("{")]
    session, total = lines[0]["session_seconds"], time.monotonic() - started
    print(f"spark-submit: getOrCreate() took {session:.1f} s once the driver ran; whole job {total:.0f} s")
    results = [r | {"limits": "4 cores, 1.5 GB heap", "peak_rss_mib": max(peaks)} for r in lines[1:]]
    print_table(results, "")
    print("(spark peak_mib: the container's peak from docker stats over the whole job)")
    return results


def host() -> dict:
    mem = int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout or 0)
    info = {"machine": platform.machine(), "cores": os.cpu_count(), "ram_gib": mem / 2**30}
    if sys.platform == "darwin":
        brand = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout
        perf = [
            subprocess.run(["sysctl", "-n", f"hw.perflevel{i}.physicalcpu"], capture_output=True, text=True).stdout
            for i in (0, 1)
        ]
        info |= {"cpu": brand.strip(), "performance_cores": int(perf[0]), "efficiency_cores": int(perf[1])}
    import duckdb
    import polars as pl

    con = duckdb.connect()
    info["duckdb"] = duckdb.__version__
    info["polars"] = pl.__version__
    info["duckdb_defaults"] = con.sql("SELECT current_setting('threads'), current_setting('memory_limit')").fetchall()[
        0
    ]
    info["polars_threads"] = pl.thread_pool_size()
    for k, v in info.items():
        print(f"{k:<18} {v if not isinstance(v, float) else f'{v:.0f}'}")
    return info


def explain(query: str = "revenue") -> str:
    """Polars' optimised plan: the filter and the column list pushed into the Iceberg scan."""
    plan = polars_queries()[query](polars_scan("pos_monthly")).explain()
    print(plan)
    return plan


def order() -> list[list[str]]:
    """Group-by output order in Polars 2.x's streaming engine: not guaranteed unless asked for."""
    import polars as pl

    lf = polars_scan("pos_monthly").filter(pl.col("business_date").is_between(date(2026, 9, 1), date(2026, 9, 30)))
    firsts = []
    for maintain in (False, True):
        q = lf.group_by("store_id", maintain_order=maintain).agg(sales_minor=pl.col("amount_minor").sum())
        runs = [q.collect()["store_id"].head(3).to_list() for _ in range(3)]
        firsts += runs
        for i, r in enumerate(runs, 1):
            print(f"maintain_order={maintain!s:<5} run {i}: first three groups {r}")
    return firsts


def arrow() -> dict:
    """DuckDB result -> Polars frame -> DuckDB query, through Arrow; no rows are converted one by one."""
    import polars as pl

    con = duckdb_connect()
    sql = "SELECT * FROM lake.part10.pos_monthly WHERE business_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'"
    seconds, table = timed(lambda: con.sql(sql).to_arrow_table())
    print(f"DuckDB -> Arrow table: {table.num_rows:,} rows, {table.nbytes / 1048576:.1f} MiB, {seconds:.2f} s")
    seconds, df = timed(lambda: pl.from_arrow(table))
    print(f"Arrow -> Polars DataFrame: {seconds * 1000:.1f} ms")
    per_store = df.group_by("store_id").agg(sales_minor=pl.col("amount_minor").sum())
    con.register("per_store", per_store)  # a view over the Polars frame's Arrow memory, not a copy
    seconds, top = timed(
        lambda: con.sql("SELECT store_id, sales_minor FROM per_store ORDER BY sales_minor DESC LIMIT 3").fetchall()
    )
    print(f"DuckDB SQL over the Polars frame `per_store`: {top} ({seconds * 1000:.1f} ms)")
    return {"rows": table.num_rows, "top": top}


def publish() -> list:
    """Single node as a pipeline step: DuckDB writes the four-year revenue view into the catalog; Polars reads it."""
    import polars as pl

    from scripts import part07_iceberg as p7

    con = duckdb_connect()
    statements = [st.query for st in con.extract_statements((ROOT / "sql/part11/publish.sql").read_text())]
    seconds, _ = timed(lambda: [con.execute(st) for st in statements])
    print(f"DuckDB: sql/part11/publish.sql ({seconds:.1f} s)")
    tbl = p7.catalog().load_table("part11.revenue_by_month")
    print(f"Polaris: snapshot {tbl.current_snapshot().snapshot_id}, written by DuckDB, format v{tbl.format_version}")
    df = pl.scan_iceberg(tbl, storage_options=STORAGE).collect()
    gb = df.filter(pl.col("country") == "GB").sort("month").tail(3)
    print("Polars reads it back:")
    print(gb)
    return df.rows()


def lock() -> list[str]:
    """One process writes a DuckDB file; a second process can't open it, not even read-only."""
    import duckdb

    LAB.mkdir(parents=True, exist_ok=True)
    path = LAB / "lock.duckdb"
    writer = duckdb.connect(str(path))
    writer.execute("CREATE OR REPLACE TABLE t AS SELECT 42 AS x")
    seen = []
    for mode in ("read_only=False", "read_only=True"):
        probe = f"import duckdb; print(duckdb.connect('{path}', {mode}).sql('SELECT x FROM t').fetchall())"
        other = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True)
        errors = [x for x in other.stderr.splitlines() if "Error" in x]
        line = other.stdout.strip() or (errors or ["?"])[-1].split(": Conflicting")[0].replace(f"{ROOT}/", "")
        print(f"second process, {mode}: {line}")
        seen.append(line)
    writer.close()
    return seen


def oldfile() -> tuple[str, str]:
    """DuckDB 2.0 writes its own storage format by default; DuckDB 1.5.6 can't open it unless asked for."""
    import duckdb

    LAB.mkdir(parents=True, exist_ok=True)
    results = []
    for name, attach in (("default.duckdb", ""), ("v1_5.duckdb", " (STORAGE_VERSION 'v1.5.0')")):
        path = LAB / name
        path.unlink(missing_ok=True)
        con = duckdb.connect()
        con.execute(f"ATTACH '{path}' AS db{attach}")
        con.execute("CREATE TABLE db.t AS SELECT 42 AS x")
        con.close()
        probe = f"import duckdb; print(duckdb.__version__, duckdb.connect('{path}').sql('SELECT x FROM t').fetchone())"
        old = subprocess.run(
            ["uv", "run", "--isolated", "--no-project", "--with", "duckdb==1.5.6", "python", "-c", probe],
            capture_output=True,
            text=True,
        )
        errors = [x for x in old.stderr.splitlines() if "Error" in x]
        line = (old.stdout.strip() or (errors or old.stderr.strip().splitlines())[-1])[:200]
        print(f"DuckDB 2.0 ATTACH '{name}'{attach} -> opened by DuckDB 1.5.6: {line}")
        results.append(line)
    return results[0], results[1]


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "command",
        choices=["host", "profile", "spark", "explain", "order", "arrow", "publish", "lock", "oldfile", "run"],
    )
    p.add_argument("args", nargs="*")
    p.add_argument("--table", default="pos_monthly")
    p.add_argument("--queries", default=",".join(QUERIES))
    p.add_argument("--engines", default="duckdb,polars")
    p.add_argument("--threads", type=int)
    p.add_argument("--memory-mb", type=int)
    a = p.parse_args()
    queries = a.queries.split(",")
    if a.command == "run":
        print(json.dumps(run(a.args[0], a.args[1], a.table, a.threads, a.memory_mb)))
    elif a.command == "profile":
        profile(a.engines.split(","), queries, a.table, a.threads, a.memory_mb)
    elif a.command == "spark":
        spark(a.table, queries)
    else:
        commands = {
            "host": host,
            "explain": explain,
            "order": order,
            "arrow": arrow,
            "publish": publish,
            "lock": lock,
            "oldfile": oldfile,
        }
        commands[a.command]()
