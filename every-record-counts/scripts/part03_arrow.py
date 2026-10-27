"""Part 3: hand 6 million rows from DuckDB to Python three ways. Arrow moves columns; the others build objects.

uv run python scripts/part03_arrow.py      # after `part03_formats.py write`
"""

import statistics
import time
from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SQL = f"""SELECT store_id, business_date, sku, amount_minor
FROM read_parquet('{ROOT / "data/lab/part03/by_date.parquet"}')"""

con = duckdb.connect()


def timed(fetch) -> tuple[float, object]:
    times, result = [], None
    for _ in range(3):
        started = time.monotonic()
        result = fetch()
        times.append(time.monotonic() - started)
    return statistics.median(times), result


rows_s, rows = timed(lambda: con.sql(SQL).fetchall())
arrow_s, arrow = timed(lambda: con.sql(SQL).to_arrow_table())
polars_s, frame = timed(lambda: con.sql(SQL).pl())

total = sum(r[3] for r in rows)
assert pl.from_arrow(arrow).get_column("amount_minor").sum() == frame.get_column("amount_minor").sum() == total

print(f"{len(rows):,} rows x 4 columns, median of 3")
print(f"{'.fetchall() -> Python tuples':<36}{rows_s:>7.2f}s")
print(f"{'.to_arrow_table() -> Arrow table':<36}{arrow_s:>7.2f}s  {arrow.nbytes / 2**20:,.0f} MiB of Arrow buffers")
print(f"{'.pl() -> Polars DataFrame':<36}{polars_s:>7.2f}s  via pl.from_arrow, no row objects")
