"""Part 1: one table, three homes. Same answers; very different costs; different update semantics."""

import subprocess
import sys

import duckdb
import psycopg
import pytest

from scripts import part01_compare
from tests.conftest import ROOT, needs_batch, trino_rows

pytestmark = needs_batch
WHERE = "basket_id = 'GB-0042-20260815-00009' AND line_no = 1"


@pytest.fixture(scope="module", autouse=True)
def fresh_tables():
    """Reload all three homes, so earlier update experiments don't leak into the comparison."""
    subprocess.run([sys.executable, "scripts/part01_load.py"], cwd=ROOT, check=True, capture_output=True)


@pytest.mark.parametrize("query", ["revenue_by_month", "basket_lookup"])
def test_all_engines_agree(query):
    part01_compare.compare(query)  # asserts the four answers are identical


def test_scan_reads_the_heap_lookup_reads_a_few_pages():
    scan = part01_compare.postgres(part01_compare.load("revenue_by_month"))[2]
    lookup = part01_compare.postgres(part01_compare.load("basket_lookup"))[2]
    with psycopg.connect(part01_compare.PG) as con:
        heap_pages = con.execute("SELECT pg_relation_size('sales_line') / 8192").fetchone()[0]
    buffers = int(scan.split(" ")[0].replace(",", ""))
    assert buffers >= heap_pages and int(lookup.split(" ")[0]) < 10  # every heap page vs a handful


def test_postgres_updates_in_place():
    with psycopg.connect(part01_compare.PG) as con:
        before = con.execute(f"SELECT ctid, amount_minor FROM sales_line WHERE {WHERE}").fetchone()
        con.execute(f"UPDATE sales_line SET amount_minor = amount_minor - 100 WHERE {WHERE}")
        after = con.execute(f"SELECT ctid, amount_minor FROM sales_line WHERE {WHERE}").fetchone()
    assert after[1] == before[1] - 100 and after[0] != before[0]  # a new row version, same table


def test_a_folder_of_parquet_cannot_be_updated():
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    with pytest.raises(duckdb.Error):
        con.execute((ROOT / "sql/part01/update_parquet.sql").read_text())


def test_iceberg_update_is_a_new_snapshot_with_small_files():
    table = 'lakehouse.part01."sales_line$snapshots"'
    first = trino_rows(f"SELECT snapshot_id FROM {table} ORDER BY committed_at LIMIT 1")[0][0]
    old = trino_rows(f"SELECT amount_minor FROM lakehouse.part01.sales_line WHERE {WHERE}")[0][0]
    trino_rows(f"UPDATE lakehouse.part01.sales_line SET amount_minor = amount_minor - 100 WHERE {WHERE}")
    files = trino_rows('SELECT content, record_count FROM lakehouse.part01."sales_line$files"')
    assert sorted(files) == [[0, 1], [0, 6055922], [1, 1]]  # original file untouched, +1 row, +1 delete
    assert [r[0] for r in trino_rows(f"SELECT operation FROM {table} ORDER BY committed_at")] == ["append", "overwrite"]
    travelled = trino_rows(
        f"SELECT amount_minor FROM lakehouse.part01.sales_line FOR VERSION AS OF {first} WHERE {WHERE}"
    )
    assert travelled[0][0] == old
