"""Part 3: same rows, very different bytes. Columnar files fetch a fraction of row formats, sorting decides which row
groups a filter can skip, the footer is at the end, and object storage offers conditional writes but no rename."""

import struct
import subprocess
import sys

import duckdb
import pytest

from scripts import part03_formats as p
from tests.conftest import ROOT, needs_stack

pytestmark = needs_stack


@pytest.fixture(scope="module", autouse=True)
def written(lab_manifest):
    """Write the variants (about a minute and a half) unless every file is already on disk and in the bucket."""
    wanted = [path for path, _ in p.FORMATS.values()] + list(p.LAYOUTS.values())
    local = all(list(p.OUT.glob(path)) for path in wanted)
    lake = p.fs()
    remote = all(lake.glob(f"{p.BUCKET}/{path}") for path in wanted)
    if not (local and remote):
        subprocess.run([sys.executable, "scripts/part03_formats.py", "write"], cwd=ROOT, check=True)


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    return p.bucket_connection()


def run(con, query: str, path: str, function: str = "read_parquet") -> dict:
    return p.measure(con, p.query(query).replace("{sales_line}", p.reader(path, function)), repeat=1)


def test_every_format_gives_the_same_answer_and_types(con):
    answers, schemas = set(), set()
    for path, function in p.FORMATS.values():
        table = p.reader(path, function)
        answers.add(tuple(con.sql(p.query("revenue_by_month").replace("{sales_line}", table)).fetchall()))
        schemas.add(tuple(sorted(r[:2] for r in con.sql(f"DESCRIBE SELECT * FROM {table}").fetchall())))
    assert len(answers) == 1 and len(schemas) == 1


def test_parquet_fetches_a_fraction_of_csv_and_small_files_multiply_requests(con):
    csv = run(con, "revenue_by_month", "sales_line.csv", "read_csv")
    parquet = run(con, "revenue_by_month", "sales_line.parquet")
    per_store = run(con, "revenue_by_month", "per_store/*/*.parquet")
    mib = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024}
    size = {
        k: float(r["fetched"].split()[0]) * mib[r["fetched"].split()[1]]
        for k, r in {"csv": csv, "parquet": parquet}.items()
    }
    assert size["parquet"] * 50 < size["csv"]  # 9 MiB vs 700 MiB in the post
    assert per_store["gets"] > 10 * parquet["gets"]  # 600 footers and their LISTs


def test_footer_is_at_the_end():
    path = p.OUT / "by_date.parquet"
    data = path.read_bytes()
    footer = duckdb.connect().execute(f"SELECT footer_size FROM parquet_file_metadata('{path}')").fetchone()[0]
    assert data[:4] == data[-4:] == b"PAR1"
    assert struct.unpack("<I", data[-8:-4])[0] == footer


def test_sort_order_decides_what_a_filter_skips(con):
    assert run(con, "one_day", "sales_line.parquet")["groups"] == "50 / 50"  # sorted by basket_id
    assert run(con, "one_day", "by_date.parquet")["groups"] in ("1 / 50", "2 / 50")
    assert run(con, "basket_lookup", "sales_line.parquet")["groups"] == "1 / 50"
    assert int(run(con, "basket_lookup", "by_date.parquet")["groups"].split(" / ")[0]) > 40


def test_prefetching_trades_bytes_for_requests(con):
    coalesced = run(con, "one_day", "sales_line.parquet")
    con.execute("SET disable_parquet_prefetching = true")
    try:
        exact = run(con, "one_day", "sales_line.parquet")
    finally:
        con.execute("RESET disable_parquet_prefetching")
    assert exact["gets"] > 20 * coalesced["gets"]


def test_object_store_conditional_writes_and_no_rename():
    out = subprocess.run(
        [sys.executable, "scripts/part03_object_store.py"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    assert out.count("writer B: PreconditionFailed (HTTP 412)") == 1
    assert out.count("writer B -> version 2: PreconditionFailed (HTTP 412)") == 1
    assert "after CopyObject:   ['data/part-0.parquet', 'staging/part-0.parquet']" in out
    assert "3,458 objects, 4 LIST requests" in out
