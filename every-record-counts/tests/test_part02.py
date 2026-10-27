"""Part 2: an append-only daily job is wrong and gets worse on every re-run; the rebuild-by-business-date
job gives the same tables however often, and in whatever order, its runs happen.

Runs on the small generated world (no containers needed), three weeks of uploads.
"""

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from scripts import part02_pipeline as p

UPLOADS = p.days(date(2026, 8, 1), date(2026, 8, 21))
DAY = date(2026, 8, 15)


def lab(tmp_path_factory, source, name: str, uploads=UPLOADS) -> tuple[duckdb.DuckDBPyConnection, Path]:
    """A fresh raw zone and warehouse with `uploads` already landed."""
    root = tmp_path_factory.mktemp(name)
    for d in uploads:
        p.land(d, source, root)
    return p.connect(root), root


@pytest.fixture(scope="module")
def daily(small_raw, tmp_path_factory) -> duckdb.DuckDBPyConnection:
    """The production schedule: each morning one more upload lands and v2 runs for it."""
    source, _ = small_raw
    root = tmp_path_factory.mktemp("daily")
    con = None
    for d in UPLOADS:
        p.land(d, source, root)
        con = con or p.connect(root)
        p.refresh_views(con)
        p.run(con, d)
    return con


def recompute(con) -> dict:
    """Every landed event, deduplicated in one query: what the incremental runs must equal."""
    return {
        (bd, store): (baskets, revenue)
        for bd, store, baskets, revenue in con.execute("""
            SELECT business_date, store_id, count(*), sum(amount)
            FROM (SELECT DISTINCT ON (basket_id) e.store_id, e.tenders[1].amount_minor AS amount,
                         (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date
                  FROM landed_pos_events e JOIN landed_stores s USING (store_id)
                  ORDER BY basket_id, uploaded_at, event_id)
            GROUP BY ALL""").fetchall()
    }


def gold(con) -> dict:
    rows = con.execute("SELECT business_date, store_id, baskets, revenue_minor FROM gold.daily_store_sales").fetchall()
    return {(bd, store): (baskets, revenue) for bd, store, baskets, revenue in rows}


def test_v1_rerun_doubles_the_day(small_raw, tmp_path_factory):
    con, _ = lab(tmp_path_factory, small_raw[0], "naive")
    total = f"SELECT sum(revenue_minor) FROM naive.daily_store_sales WHERE business_date = DATE '{DAY}'"
    p.naive(con, DAY)
    once = con.execute(total).fetchone()[0]
    p.naive(con, DAY)
    assert con.execute(total).fetchone()[0] == 2 * once


def test_v1_error_is_exactly_missed_baskets_minus_extra_copies(small_raw, tmp_path_factory):
    con, _ = lab(tmp_path_factory, small_raw[0], "explained")
    p.naive(con, DAY)
    con.execute(f"SET VARIABLE check_date = DATE '{DAY}'")
    checked = {r[0]: r[1:] for r in con.execute(p.sql("check")).fetchall()}  # v1, v2, recompute, ...
    explained = {r[0]: r[1:] for r in con.execute(p.sql("v1_explained")).fetchall()}
    for country, (v1, _, full, *_) in checked.items():
        _, missed, _, doubled = explained[country]
        assert v1 == full - (missed or 0) + (doubled or 0)
        assert missed > 0  # late and early uploads are lost on every day


def test_v2_equals_a_full_recompute(daily):
    assert gold(daily) == recompute(daily)


def test_v2_rerun_changes_nothing(daily):
    before = p.fingerprint(daily)
    for d in (DAY, DAY - timedelta(days=3), DAY):
        p.run(daily, d)
    assert p.fingerprint(daily) == before


def test_v2_order_does_not_matter(daily, small_raw, tmp_path_factory):
    con, _ = lab(tmp_path_factory, small_raw[0], "backwards")
    for d in reversed(UPLOADS):  # a backfill, newest first, after everything landed
        p.run(con, d)
    assert p.fingerprint(con) == p.fingerprint(daily)


def test_late_upload_restates_yesterday(small_raw, tmp_path_factory):
    con, root = lab(tmp_path_factory, small_raw[0], "restate", p.days(DAY - timedelta(days=2), DAY))
    for d in p.days(DAY - timedelta(days=2), DAY):
        p.run(con, d)
    provisional = gold(con)
    p.land(DAY + timedelta(days=1), small_raw[0], root)
    p.refresh_views(con)
    assert DAY in p.run(con, DAY + timedelta(days=1))  # the new upload touched yesterday
    final = gold(con)
    day = [k for k in final if k[0] == DAY]
    assert any(final[k] != provisional.get(k) for k in day)  # yesterday changed...
    assert {k: final[k] for k in day} == {k: v for k, v in recompute(con).items() if k[0] == DAY}  # ...to the truth


def test_a_crash_mid_run_leaves_both_layers_untouched(daily):
    before = p.fingerprint(daily), p.fingerprint(daily, "silver.pos_baskets")
    with pytest.raises(duckdb.Error, match="simulated crash"):
        p.run(daily, DAY, extra=["SELECT error('simulated crash')"])
    assert (p.fingerprint(daily), p.fingerprint(daily, "silver.pos_baskets")) == before


def test_silver_refuses_a_second_copy_of_a_basket(daily):
    with pytest.raises(duckdb.ConstraintException):
        daily.execute("INSERT INTO silver.pos_baskets SELECT * FROM silver.pos_baskets LIMIT 1")
