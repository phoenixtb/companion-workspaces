"""Run SQL files in DuckDB against the raw zone and print every result.

uv run python scripts/duck.py sql/part00/stock_availability.sql            # reads s3://larkspur-raw/raw
uv run python scripts/duck.py --local sql/incident/uk_revenue_legacy.sql  # reads data/raw
"""

import argparse
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

p = argparse.ArgumentParser()
p.add_argument("files", nargs="+", type=Path)
p.add_argument("--local", action="store_true", help="read data/raw instead of the bucket")
a = p.parse_args()

con = duckdb.connect()
if a.local:
    con.execute(f"SET VARIABLE raw_root = '{ROOT / 'data/raw'}'")
else:
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute("SET VARIABLE raw_root = 's3://larkspur-raw/raw'")
con.execute((ROOT / "sql/raw/views.sql").read_text())

for f in a.files:
    print(f"-- {f}")
    try:
        statements = con.extract_statements(f.read_text())
    except duckdb.Error as e:
        print(f"{type(e).__name__}: {e}")
        continue
    for stmt in statements:
        started = time.monotonic()
        try:
            rel = con.sql(stmt.query)
        except duckdb.Error as e:
            print(f"{type(e).__name__}: {e}")
            continue
        if rel is not None:
            rel.show(max_width=140)
            print(f"({time.monotonic() - started:.1f}s)")
