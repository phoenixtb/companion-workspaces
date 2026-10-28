"""Part 14: the design review's sizing script and the Season 1 incident ledger.

The estimate reads the raw zone on disk and Iceberg manifests through Polaris; it needs `make data`, `make up-core`
and Part 10's part10.pos_monthly (and Part 7's part07.clicks_v2). The ledger needs only `make data`.
"""

import math

import pytest

from scripts import part14_sizing as lab
from tests.conftest import needs_stack

MIB, GIB = 2**20, 2**30


@pytest.fixture(scope="module")
def sized() -> tuple[dict, dict]:
    from scripts import part07_iceberg as p7

    cat = p7.catalog()
    if not (cat.table_exists("part10.pos_monthly") and cat.table_exists("part07.clicks_v2")):
        pytest.skip("needs Part 10's pos_monthly and Part 7's clicks_v2")
    x = lab.measure()
    return x, lab.estimate(x)


def test_incident_ledger_sums_to_the_gap(lab_manifest):
    r = lab.incident()
    assert r["total"] == r["gap"] == 20_855_837  # £208,558.37, the 0.700% gap
    assert r["accounted"] == 15_149_341  # duplicates found, refund dating, FX and business dates fixed
    assert r["gap"] - r["accounted"] == 5_706_496  # hard-deleted orders: Part 16


def test_rebuild_rewrites_whole_month_partitions():
    day = lab.rebuild_bytes_per_night(1.0, month_partitions=False)
    month = lab.rebuild_bytes_per_night(1.0, month_partitions=True)
    assert day == lab.LATE_DAYS + 1  # three day partitions a night
    assert 15 < month < 20  # half a month on average, a month and a bit at the turn of a month


def test_nine_am_burst_drains_in_seconds_only_from_the_serving_table():
    burst = lab.VIEWERS * lab.TILES
    lo, hi = lab.QPS_32_CLIENTS["Trino over Iceberg"]
    assert burst / hi > 90  # a minute and a half of queueing, at best
    lo, hi = lab.QPS_32_CLIENTS["ClickHouse serving table"]
    assert burst / lo < 3


@needs_stack
def test_layout_bytes_and_scans_match_part10(sized):
    x, _ = sized
    t = x["sales_table"]
    assert (t["files"], t["rows"]) == (197, 72_670_965)
    assert t["bytes"] / t["rows"] == pytest.approx(14.83, abs=0.01)
    s = x["scans"]
    assert (s["revenue (Sep)"]["files"], s["store promo (12 mo)"]["files"]) == (4, 11)  # Part 10, step 3
    assert s["revenue (Sep)"]["file_bytes"] / MIB == pytest.approx(20.2, abs=0.05)
    assert s["history (4 yr)"]["files"] == 197
    # column bytes are what a columnar reader needs; DuckDB fetched 1.9 and 107.5 MiB for these in Part 11
    assert s["revenue (Sep)"]["column_bytes"] / MIB < 1.9
    assert s["history (4 yr)"]["column_bytes"] / MIB < 107.5


@needs_stack
def test_full_volume_matches_the_partition_choice(sized):
    x, e = sized
    assert x["pos_days"] == 126
    sales = e["per_day"]["sales lines (Iceberg)"]
    assert 33 * MIB < sales < 35 * MIB  # Part 10: ~34 MiB a day
    assert 0.95 * GIB < sales * lab.DAYS_PER_MONTH < 1.1 * GIB  # ~1 GiB a month: months(business_date)
    assert math.ceil(sales * lab.DAYS_PER_MONTH / lab.TARGET_FILE) == 3  # files per month after compaction
    assert sum(e["retained"].values()) < 400 * GIB  # not big data
    _, gz, js, pq = e["rows"]["POS lines"]
    assert js / pq > 15 and gz / pq > 1.5
    assert all(years > 9 for years in e["triggers"].values())  # no day partitions within the decade at 30% a year
