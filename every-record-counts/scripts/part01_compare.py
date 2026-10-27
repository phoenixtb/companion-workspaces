"""Part 1: ask the same questions of PostgreSQL, a Parquet lake and an Iceberg lakehouse.

Runs sql/part01/revenue_by_month.sql (an analytical scan) and sql/part01/basket_lookup.sql
(a point read) in each engine, checks the engines agree, and prints time (median of three
runs after one warm-up) and how much data each one read.
"""

import json
import re
import statistics
import time
import urllib.request
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import psycopg
import s3fs

ROOT = Path(__file__).resolve().parents[1]
PG = "host=localhost port=5432 dbname=larkspur user=larkspur password=larkspur"
PARQUET = "read_parquet('s3://larkspur-lake/parquet/sales_line/*.parquet')"


def load(name: str) -> str:
    text = (ROOT / f"sql/part01/{name}.sql").read_text()
    return "\n".join(line for line in text.splitlines() if not line.startswith("--"))


def _size(n: int) -> str:
    return f"{n / 2**20:.1f} MiB" if n >= 2**20 else f"{n // 1024} KiB"


def median_time(run, repeat: int = 3) -> float:
    run()  # warm-up: connections, metadata, caches
    times = []
    for _ in range(repeat):
        started = time.monotonic()
        run()
        times.append(time.monotonic() - started)
    return statistics.median(times)


def postgres(query: str) -> tuple[list, float, str]:
    q = query.replace("{sales_line}", "sales_line")
    with psycopg.connect(PG) as con:
        rows = con.execute(q).fetchall()
        plan = con.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + q).fetchone()[0][0]["Plan"]
        pages = plan["Shared Hit Blocks"] + plan["Shared Read Blocks"]
        t = median_time(lambda: con.execute(q).fetchall())
    hit = plan["Shared Hit Blocks"]
    return rows, t, f"{pages:,} buffers of 8 KiB ({_size(pages * 8192)}; {hit:,} already in shared buffers)"


def duck(query: str, table: str, label: str) -> tuple[list, float, str]:
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    q = query.replace("{sales_line}", table)
    plan = con.sql("EXPLAIN ANALYZE " + q).fetchall()[0][1]  # first run: nothing cached yet
    fetched = re.search(r"in: ([\d.]+ \w+)", plan).group(1)
    rows = con.sql(q).fetchall()
    t = median_time(lambda: con.sql(q).fetchall())
    return rows, t, f"{fetched} fetched from {label}"


def trino(query: str, table: str) -> tuple[list, float, str]:
    def run() -> tuple[list, dict]:
        req = urllib.request.Request(
            "http://localhost:8080/v1/statement",
            data=query.replace("{sales_line}", table).encode(),
            headers={"X-Trino-User": "rohan"},
        )
        page = json.load(urllib.request.urlopen(req))
        rows = []
        while True:
            rows += page.get("data", [])
            if "nextUri" not in page:
                return rows, page["stats"]
            page = json.load(urllib.request.urlopen(page["nextUri"]))

    rows, stats = run()
    t = median_time(run)
    return rows, t, f"{_size(stats['physicalInputBytes'])} physical input, {stats['processedRows']:,} rows decoded"


def normalise(rows: list) -> list:
    """Same values, same types, whichever engine returned them (months as YYYY-MM, numbers as int)."""
    out = []
    for r in rows:
        out.append(
            tuple(
                str(v)[:7]
                if isinstance(v, (date, str)) and str(v).startswith("2026-")
                else int(v)
                if isinstance(v, (int, Decimal))
                else v
                for v in r
            )
        )
    return out


def stored_mb(prefix: str) -> str:
    """Size of the Parquet data under a bucket prefix, as stored."""
    fs = s3fs.S3FileSystem(
        key="larkspur",
        secret="larkspur-lab-secret",
        client_kwargs={"endpoint_url": "http://localhost:8333", "region_name": "us-east-1"},
    )
    files = [f for f in fs.find(prefix, detail=True).values() if f["name"].endswith(".parquet")]
    return f"{sum(f['size'] for f in files) / 1e6:.0f} MB of Parquet"


def compare(name: str) -> dict[str, tuple[list, float, str]]:
    """Run one sql/part01 query in all four engines; fail if their answers differ."""
    query = load(name)
    results = {
        "PostgreSQL 18 (row store, B-tree PK)": postgres(query),
        "DuckDB 2.0 over Parquet files": duck(query, PARQUET, stored_mb("larkspur-lake/parquet/sales_line")),
        "DuckDB 2.0 over Iceberg (Polaris)": duck(
            query, "lake.part01.sales_line", stored_mb("larkspur-lake/warehouse/part01/sales_line/data")
        ),
        "Trino 484 over Iceberg (Polaris)": trino(query, "lakehouse.part01.sales_line"),
    }
    answers = [normalise(r[0]) for r in results.values()]
    assert all(a == answers[0] for a in answers), f"the engines disagree on {name}"
    return results


def main() -> None:
    for name in ("revenue_by_month", "basket_lookup"):
        results = compare(name)
        reference = normalise(next(iter(results.values()))[0])
        print(f"== {name}: all four engines return the same {len(reference)} rows")
        for row in reference:
            print("  ", "  ".join(f"{v:,}" if isinstance(v, int) else str(v) for v in row))
        print(f"\n{'engine':<40}{'median':>10}  read")
        for engine, (_, t, read) in results.items():
            print(f"{engine:<40}{t * 1000:>8.1f}ms  {read}")
        print()


if __name__ == "__main__":
    main()
