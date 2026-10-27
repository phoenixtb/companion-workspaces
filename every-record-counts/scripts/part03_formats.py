"""Part 3: the same sales lines as CSV, JSON Lines, Avro and Parquet, and in four Parquet layouts.

uv run python scripts/part03_formats.py write     # build sales_line from data/raw, write every variant, upload it
uv run python scripts/part03_formats.py compare   # Q3 revenue over each format in s3://larkspur-lake/part03
uv run python scripts/part03_formats.py skip      # one day and one basket over four Parquet layouts

Needs `make data` (write) and `make up-core` (upload, compare, skip).
"""

import argparse
import re
import statistics
import time
from pathlib import Path

import duckdb
import s3fs

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/lab/part03"
BUCKET = "larkspur-lake/part03"

FORMATS = {  # label: (file or glob under part03/, DuckDB reader)
    "CSV": ("sales_line.csv", "read_csv"),
    "CSV, gzip": ("sales_line.csv.gz", "read_csv"),
    "JSON Lines": ("sales_line.jsonl", "read_json"),
    "JSON Lines, gzip": ("sales_line.jsonl.gz", "read_json"),
    "Avro": ("sales_line.avro", "read_avro"),
    "Avro, deflate": ("sales_line.deflate.avro", "read_avro"),
    "Parquet, uncompressed": ("sales_line.uncompressed.parquet", "read_parquet"),
    "Parquet, Snappy": ("sales_line.parquet", "read_parquet"),
    "Parquet, ZSTD": ("sales_line.zstd.parquet", "read_parquet"),
    "Parquet, Snappy, 600 files": ("per_store/*/*.parquet", "read_parquet"),
}
LAYOUTS = {  # label: Parquet file under part03/
    "by basket_id, 122,880-row groups": "sales_line.parquet",
    "by business_date, 122,880-row groups": "by_date.parquet",
    "by business_date, 10,000-row groups": "by_date.rg10k.parquet",
    "by business_date, 1,000,000-row groups": "by_date.rg1m.parquet",
}


def fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key="larkspur",
        secret="larkspur-lab-secret",
        client_kwargs={"endpoint_url": "http://localhost:8333", "region_name": "us-east-1"},
    )


def query(name: str) -> str:
    folder = "part01" if name == "revenue_by_month" or name == "basket_lookup" else "part03"
    text = (ROOT / f"sql/{folder}/{name}.sql").read_text()
    return "\n".join(line for line in text.splitlines() if not line.startswith("--"))


def reader(path: str, function: str) -> str:
    return f"{function}('s3://{BUCKET}/{path}')"


def bucket_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute("LOAD avro")
    # DuckDB keeps remote file blocks in memory between queries; off, every run fetches from storage.
    con.execute("SET enable_external_file_cache = false")
    return con


def measure(con: duckdb.DuckDBPyConnection, sql: str, repeat: int = 5) -> dict:
    """Bytes and GET requests from EXPLAIN ANALYZE's HTTP stats, row groups scanned, median time of `repeat` runs."""
    plan = con.sql("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    groups = re.search(r"Row Groups Scanned: ([\d,]+) / ([\d,]+)", plan)
    times = []
    for _ in range(repeat):
        started = time.monotonic()
        rows = con.sql(sql).fetchall()
        times.append(time.monotonic() - started)
    return {
        "rows": rows,
        "seconds": statistics.median(times),
        "fetched": re.search(r"in: ([\d.]+ \w+)", plan).group(1),
        "gets": int(re.search(r"#GET: (\d+)", plan).group(1)),
        "groups": f"{groups.group(1)} / {groups.group(2)}" if groups else "",
    }


def write() -> None:
    con = duckdb.connect()
    con.execute(f"SET VARIABLE raw_root = '{ROOT / 'data/raw'}'")
    con.execute((ROOT / "sql/raw/views.sql").read_text())
    con.execute("LOAD avro")
    started = time.monotonic()
    con.execute((ROOT / "sql/part01/sales_line.sql").read_text())
    rows = con.execute("SELECT count(*) FROM sales_line").fetchone()[0]
    print(f"sales_line from data/raw: {rows:,} rows, 13 columns ({time.monotonic() - started:.1f}s)\n")

    OUT.mkdir(parents=True, exist_ok=True)
    for old in OUT.rglob("*"):
        if old.is_file():
            old.unlink()
    print(f"{'file':<34}{'size':>10}{'write':>8}")
    sql = (ROOT / "sql/part03/write_formats.sql").read_text().replace("{out}", str(OUT))
    for statement in con.extract_statements(sql):
        target = Path(re.search(r"TO '([^']+)'", statement.query).group(1))
        started = time.monotonic()
        con.execute(statement.query)
        seconds = time.monotonic() - started
        files = [f for f in target.rglob("*") if f.is_file()] if target.is_dir() else [target]
        name = f"{target.name}/ ({len(files)} files)" if target.is_dir() else target.name
        print(f"{name:<34}{sum(f.stat().st_size for f in files) / 1e6:>7.1f} MB{seconds:>7.1f}s")

    lake = fs()
    if lake.exists(BUCKET):
        lake.rm(BUCKET, recursive=True)
    lake.put(f"{OUT}/", f"{BUCKET}/", recursive=True)
    print(f"\nuploaded to s3://{BUCKET}/: {len(lake.find(BUCKET))} objects")


def local_median(con: duckdb.DuckDBPyConnection, sql: str, repeat: int = 5) -> float:
    """The same query over the files on local disk, after one warm-up run (the OS page cache is warm)."""
    con.sql(sql).fetchall()
    times = []
    for _ in range(repeat):
        started = time.monotonic()
        con.sql(sql).fetchall()
        times.append(time.monotonic() - started)
    return statistics.median(times)


def compare() -> None:
    con, disk = bucket_connection(), duckdb.connect()
    disk.execute("LOAD avro")
    sql = query("revenue_by_month")
    lake = fs()
    print("Q3 revenue by country and month (sql/part01/revenue_by_month.sql)")
    print(f"{'':<28}{'':>11}{'---- from s3://larkspur-lake/part03 ----':>40}{'local disk':>12}")
    print(f"{'format':<28}{'stored':>11}{'fetched':>12}{'GETs':>6}{'median':>9}{'median':>12}")
    expected, schemas = None, set()
    for label, (path, function) in FORMATS.items():
        table = reader(path, function)
        result = measure(con, sql.replace("{sales_line}", table))
        on_disk = local_median(disk, sql.replace("{sales_line}", f"{function}('{OUT}/{path}')"))
        expected = expected or result["rows"]
        assert result["rows"] == expected, f"{label} disagrees"
        schemas.add(tuple(sorted(r[:2] for r in con.sql(f"DESCRIBE SELECT * FROM {table}").fetchall())))
        stored = sum(lake.du(p) for p in lake.glob(f"{BUCKET}/{path}")) / 2**20
        print(
            f"{label:<28}{stored:>7.1f} MiB{result['fetched']:>12}{result['gets']:>6}{result['seconds']:>8.2f}s"
            f"{on_disk:>11.2f}s"
        )
    print(f"\nall {len(FORMATS)} return the same {len(expected)} rows")
    print(f"column names and types read back: {'identical in all' if len(schemas) == 1 else 'DIFFERENT'}")


def skip() -> None:
    con = bucket_connection()
    for name in ("one_day", "basket_lookup"):
        sql = query(name)
        print(f"== {name}")
        print(f"{'layout':<46}{'row groups read':>16}{'fetched':>12}{'GETs':>6}{'median':>9}")
        expected = None
        runs = [(label, path, "RESET disable_parquet_prefetching") for label, path in LAYOUTS.items()]
        if name == "one_day":  # the first layout again, without coalescing nearby byte ranges into one GET
            runs.append(
                (
                    "by basket_id, prefetching off",
                    LAYOUTS[next(iter(LAYOUTS))],
                    "SET disable_parquet_prefetching = true",
                )
            )
        for label, path, setting in runs:
            con.execute(setting)
            result = measure(con, sql.replace("{sales_line}", reader(path, "read_parquet")))
            expected = expected or result["rows"]
            assert result["rows"] == expected, f"{label} disagrees"
            print(
                f"{label:<46}{result['groups']:>16}{result['fetched']:>12}{result['gets']:>6}"
                f"{result['seconds'] * 1000:>7.0f}ms"
            )
        con.execute("RESET disable_parquet_prefetching")
        print()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["write", "compare", "skip"])
    {"write": write, "compare": compare, "skip": skip}[p.parse_args().command]()
