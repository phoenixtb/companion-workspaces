"""Part 1: put the same sales lines in three homes.

1. PostgreSQL: a row-store table with a primary key, like a service's database.
2. A data lake: a folder of Parquet files in object storage.
3. A lakehouse: an Iceberg table registered in the Polaris catalog.
"""

import time
from pathlib import Path

import duckdb
import s3fs

ROOT = Path(__file__).resolve().parents[1]
PG = "host=localhost port=5432 dbname=larkspur user=larkspur password=larkspur"


def timed(label: str, con: duckdb.DuckDBPyConnection, sql: str) -> None:
    started = time.monotonic()
    con.execute(sql)
    print(f"{label:<44} {time.monotonic() - started:6.1f}s")


con = duckdb.connect()
con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
con.execute("SET VARIABLE raw_root = 's3://larkspur-raw/raw'")
con.execute((ROOT / "sql/raw/views.sql").read_text())
con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())

timed("build sales_line from raw POS events", con, (ROOT / "sql/part01/sales_line.sql").read_text())
rows = con.execute("SELECT count(*) FROM sales_line").fetchall()[0][0]
print(f"{'rows':<44} {rows:,}")

con.execute(f"ATTACH '{PG}' AS pg (TYPE postgres)")
con.execute("DROP TABLE IF EXISTS pg.public.sales_line")
timed("1. PostgreSQL table", con, "CREATE TABLE pg.public.sales_line AS FROM sales_line")
timed(
    "   + primary key",
    con,
    "CALL postgres_execute('pg', 'ALTER TABLE sales_line ADD PRIMARY KEY (basket_id, line_no)')",
)
timed("   + ANALYZE", con, "CALL postgres_execute('pg', 'ANALYZE sales_line')")

timed(
    "2. Parquet files in s3://larkspur-lake/parquet",
    con,
    """
    COPY sales_line TO 's3://larkspur-lake/parquet/sales_line'
    (FORMAT parquet, FILE_SIZE_BYTES '64MB', OVERWRITE_OR_IGNORE true)""",
)

con.execute("CREATE SCHEMA IF NOT EXISTS lake.part01")
con.execute("DROP TABLE IF EXISTS lake.part01.sales_line")
# DROP TABLE removes the table from the catalog but leaves its files in the bucket
# (no purge was requested). Delete them, or every re-run leaves 43 MB behind.
lake_fs = s3fs.S3FileSystem(
    key="larkspur",
    secret="larkspur-lab-secret",
    client_kwargs={"endpoint_url": "http://localhost:8333", "region_name": "us-east-1"},
)
if lake_fs.exists("larkspur-lake/warehouse/part01/sales_line"):
    lake_fs.rm("larkspur-lake/warehouse/part01/sales_line", recursive=True)
timed("3. Iceberg table lake.part01.sales_line", con, "CREATE TABLE lake.part01.sales_line AS FROM sales_line")

# What each home holds, as its own metadata reports it.
print()
sizes = con.execute("""SELECT * FROM postgres_query('pg', '
    SELECT pg_size_pretty(pg_relation_size(''sales_line'')) AS heap,
           pg_size_pretty(pg_indexes_size(''sales_line''))  AS primary_key_index')""").fetchall()[0]
print(f"{'PostgreSQL heap / primary-key index':<44} {sizes[0]} / {sizes[1]}")
for label, files in (
    ("Parquet files", "s3://larkspur-lake/parquet/sales_line/*.parquet"),
    ("Iceberg data file", "s3://larkspur-lake/warehouse/part01/sales_line/data/*.parquet"),
):
    groups, biggest, codec = con.execute(f"""
        SELECT count(*), max(row_group_num_rows), any_value(compression)
        FROM (SELECT DISTINCT file_name, row_group_id, row_group_num_rows, compression
              FROM parquet_metadata('{files}'))""").fetchall()[0]
    print(f"{label + ' row groups':<44} {groups}, up to {biggest:,} rows each, {codec}")
