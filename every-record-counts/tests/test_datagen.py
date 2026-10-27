"""The generator is deterministic, calibrated, and its manifest agrees with plain SQL over the files."""

import json

import pytest

from tests.conftest import ROOT, generate, raw_connection


def test_same_seed_same_world(small_raw, tmp_path):
    _, first = small_raw
    assert generate(tmp_path / "again") == first


def test_uk_revenue_gap_is_calibrated_and_the_waterfall_sums(small_raw):
    _, m = small_raw
    r = m["uk_net_revenue_q3"]
    assert r["gap_share"] == pytest.approx(0.007, abs=0.0005)  # small runs have coarse baskets
    assert sum(s["amount_minor"] for s in r["steps"]) == r["legacy_board_deck_minor"] - r["correct_minor"]
    assert [s["key"] for s in r["steps"]] == [
        "duplicate_pos_events",
        "hard_deleted_orders",
        "refund_dating",
        "fx_today_rate",
        "utc_business_date",
    ]


@pytest.mark.parametrize(
    "query,key", [("uk_revenue_legacy", "legacy_board_deck_minor"), ("uk_revenue_correct", "correct_minor")]
)
def test_sql_reproduces_the_manifest(small_raw, query, key):
    raw, m = small_raw
    con = raw_connection(raw)
    got = con.execute((ROOT / f"sql/incident/{query}.sql").read_text()).fetchall()[0][0]
    assert got == m["uk_net_revenue_q3"][key]


def test_the_other_two_headline_numbers_disagree(small_raw):
    _, m = small_raw
    stock = m["stock_availability_q3"]
    assert stock["legacy_weekly_sum"] > stock["per_day_reported_snapshot"] > stock["correct_close_of_day"]
    conv = m["conversion_q3"]
    assert conv["product_team"] - conv["marketing"] == pytest.approx(0.009, abs=0.002)


def test_published_manifest_matches_the_lab_data():
    """docs/injected-defects.md is written from the full-size run in data/raw."""
    manifest = ROOT / "data/raw/_manifest.json"
    if not manifest.exists():
        pytest.skip("run `make data` first")
    m = json.loads(manifest.read_text())
    doc = (ROOT / "docs/injected-defects.md").read_text()
    legacy = m["uk_net_revenue_q3"]["legacy_board_deck_minor"]
    assert f"£{legacy / 100:,.2f}" in doc
    assert f"(seed {m['generator']['seed']}, scale {m['generator']['scale']}" in doc
