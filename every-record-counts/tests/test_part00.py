"""Part 0: the three headline numbers, each computed two ways from the raw zone in the bucket."""

import pytest

from tests.conftest import ROOT, needs_stack

pytestmark = needs_stack


def run(con, path: str) -> list[tuple]:
    return con.execute((ROOT / path).read_text()).fetchall()


def test_uk_revenue_two_answers(bucket, lab_manifest):
    r = lab_manifest["uk_net_revenue_q3"]
    assert run(bucket, "sql/incident/uk_revenue_legacy.sql")[0][0] == r["legacy_board_deck_minor"]
    assert run(bucket, "sql/incident/uk_revenue_correct.sql")[0][0] == r["correct_minor"]


def test_stock_two_answers(bucket, lab_manifest):
    s = lab_manifest["stock_availability_q3"]
    dashboard, per_day, shelf = run(bucket, "sql/part00/stock_availability.sql")[0]
    assert dashboard == pytest.approx(s["legacy_weekly_sum"] * 100, abs=0.05)
    assert per_day == pytest.approx(s["per_day_reported_snapshot"] * 100, abs=0.05)
    assert shelf == pytest.approx(s["correct_close_of_day"] * 100, abs=0.05)


def test_conversion_two_answers(bucket, lab_manifest):
    c = lab_manifest["conversion_q3"]
    rows = dict((team, pct) for team, _, pct in run(bucket, "sql/part00/conversion.sql"))
    assert rows["product"] == pytest.approx(c["product_team"] * 100, abs=0.005)
    assert rows["marketing"] == pytest.approx(c["marketing"] * 100, abs=0.005)
