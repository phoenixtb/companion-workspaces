"""Part 6: the SQL workbook finds what the manifest says is there.

Re-sent POS events are found by basket_id (not event_id) and are exactly revenue step 1; the two session definitions
give the two conversion rates, and the breakdown of the difference adds up; SDK retries change events, not sessions;
gaps-and-islands runs add up to the stock-out days; a recomputing MERGE is idempotent and an incrementing one is not;
an event-time high-water mark loses late baskets and an upload-time one doesn't; HyperLogLog stays close to exact.

The small generated world is built into a Part 4 star, extended by Part 5, then the Part 6 workbook.
`test_the_numbers_in_the_post` builds the workbook from data/raw and Part 5's database, when present.
`test_trino` runs the Trino statements against the full-size table (needs `make up-trino`).
"""

from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from scripts import part04_star as star
from scripts import part05_shapes as shapes
from scripts import part06_sql as workbook
from tests.conftest import ROOT, needs_batch


def query(con: duckdb.DuckDBPyConnection, name: str, statement: int = -1) -> list[tuple]:
    """The rows of one statement of sql/part06/<name>.sql (the last one by default)."""
    statements = con.extract_statements((ROOT / f"sql/part06/{name}.sql").read_text())
    return con.execute(statements[statement].query).fetchall()


def run_all(con: duckdb.DuckDBPyConnection, name: str) -> list[str]:
    """Run every statement of a file in order; return the error messages of those that failed."""
    errors = []
    for stmt in con.extract_statements((ROOT / f"sql/part06/{name}.sql").read_text()):
        try:
            con.execute(stmt.query)
        except duckdb.Error as e:
            errors.append(str(e))
    return errors


def one(con, sql: str):
    return con.execute(sql).fetchone()[0]


@pytest.fixture(scope="module")
def small(small_raw, tmp_path_factory) -> tuple[duckdb.DuckDBPyConnection, dict]:
    raw, manifest = small_raw
    tmp = tmp_path_factory.mktemp("part06")
    star_db = tmp / "star.duckdb"
    star_con = star.connect(star_db, raw_root=str(raw))
    star.build(star_con, star_db)
    star_con.close()
    part05 = shapes.prepare(str(raw), tmp / "part05.duckdb", star_db)
    shapes.build(part05)
    part05.close()
    con = workbook.connect(str(raw), tmp / "workbook.duckdb", tmp / "part05.duckdb")
    workbook.build(con)
    return con, manifest


def test_resends_share_a_basket_id_and_are_revenue_step_1(small):
    con, m = small
    by_country = query(con, "pos_duplicates", 0)
    assert all(events == ids for _, events, ids, _, _ in by_country)  # event_id finds nothing
    assert sum(r[4] for r in by_country) == m["defects"]["pos_duplicate_events_total"]
    assert dict((r[0], r[4]) for r in by_country)["GB"] == m["defects"]["pos_duplicate_events_gb"]
    resends, _, same_payload, *_ = query(con, "pos_duplicates", 1)[0]
    assert resends == same_payload == m["defects"]["pos_duplicate_events_total"]
    step_1 = next(s for s in m["uk_net_revenue_q3"]["steps"] if s["key"] == "duplicate_pos_events")
    resent, every_event, one_per_basket, resent_gbp = query(con, "pos_duplicates", 2)[0]
    assert resent == m["defects"]["pos_duplicate_events_gb"]
    assert resent_gbp * 100 == step_1["amount_minor"] and every_event - one_per_basket == resent_gbp
    counts = {method: n for method, n in query(con, "pos_duplicates", 3)}
    assert counts["QUALIFY row_number() = 1 by basket_id"] == counts["DISTINCT ON (basket_id)"]
    assert counts["QUALIFY row_number() = 1 by event_id"] - counts["DISTINCT ON (basket_id)"] == sum(
        r[4] for r in by_country
    )


def test_two_session_definitions_give_the_two_conversion_rates(small):
    con, m = small
    rates = {(d, bots): pct for d, bots, _, _, pct in query(con, "conversion", 0)}
    assert rates[("product", "excluded")] == round(100 * m["conversion_q3"]["product_team"], 2)
    assert rates[("marketing", "included")] == round(100 * m["conversion_q3"]["marketing"], 2)
    steps = query(con, "conversion", 1)
    assert steps[0][0].startswith("product") and steps[-1][0].startswith("marketing")
    assert sum(n for _, n in steps[:-1]) == steps[-1][1]  # the breakdown adds up exactly
    events, without, with_retries, without_retries = query(con, "conversion", 2)[0]
    assert events - without == m["defects"]["clickstream_sdk_retry_duplicates"]
    assert with_retries == without_retries  # retries change events, not sessions
    bots = one(con, "SELECT count(*) FROM sessions WHERE definition = 'product' AND is_bot")
    assert bots == m["defects"]["clickstream_bot_hits"]  # every bot hit is its own visitor and its own session


def test_funnel_ends_at_the_conversion_rate(small):
    con, m = small
    funnel = {step: (marketing, product) for step, marketing, product in query(con, "funnel", 0)}
    assert list(funnel) == ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"]
    assert funnel["purchase"] == (
        round(100 * m["conversion_q3"]["marketing"], 2),
        round(100 * m["conversion_q3"]["product_team"], 2),
    )
    sessions, *ordered = query(con, "funnel", 1)[0]
    assert sessions >= ordered[0] and ordered == sorted(ordered, reverse=True)


def test_stockout_islands_add_up(small):
    con, _ = small
    zero_days = one(
        con,
        """SELECT count(*) FROM star.fct_stock_daily f JOIN star.dim_date d USING (date_key)
           WHERE d.quarter = '2026-Q3' AND f.closing_on_hand_qty = 0""",
    )
    assert sum(r[2] for r in query(con, "stockouts", 0)) == zero_days
    measures = {r[0]: r[1:] for r in query(con, "stockouts", 1)}
    assert measures["at close"][0] == zero_days
    assert measures["as reported"][0] < zero_days  # morning counts hide overnight refills
    by_sku, by_key = query(con, "stockouts", 2)[0]
    assert by_key >= by_sku


def test_merge_recomputed_is_idempotent_and_increments_are_not(small):
    con, _ = small
    errors = run_all(con, "merge")
    assert len(errors) == 1 and "cannot affect the same target row more than once" in errors[0]
    log = {step[:2]: rest for step, *rest in con.execute("SELECT * FROM merge_log").fetchall()}
    assert log["1."] == log["2."] == log["5."]
    assert log["4."] != log["5."]
    rows, baskets = query(con, "merge")[0]
    assert rows > baskets  # unmatched duplicates in the source were inserted twice


def test_high_water_marks(small):
    con, m = small
    baskets, by_event_ts, by_uploaded_at, _ = query(con, "high_water_mark")[0]
    assert baskets == m["volumes"]["pos_baskets"]
    assert by_uploaded_at == 0 and by_event_ts > 0


def test_approx_distinct_is_close(small):
    con, _ = small
    for _, exact, approx, error_pct in query(con, "approx", 0):
        assert abs(approx - exact) / exact < 0.05 and error_pct == round(100 * (approx - exact) / exact, 2)
    _, _, _, daily_sum, quarter = query(con, "approx", 1)[0]
    assert daily_sum > quarter  # distinct counts are not additive


def test_schema_drift(small):
    con, m = small
    _, _, missing = query(con, "schema_drift", 0)[0]
    assert missing == ["consent", "page_path"]
    assert one(con, "SELECT count(*) FROM clicks WHERE page IS NULL") == 0
    assert (
        one(con, "SELECT count(*) FROM raw_clickstream WHERE page_path IS NOT NULL")
        == m["defects"]["clickstream_events_with_page_path"]
    )


def full_workbook(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    if not (ROOT / "data/raw/_manifest.json").exists() or not shapes.DB.exists():
        pytest.skip("needs `make data` and Part 5's database (scripts.part05_shapes build --local)")
    con = workbook.connect(str(ROOT / "data/raw"), tmp_path / "workbook.duckdb")
    workbook.build(con)
    return con


def test_the_numbers_in_the_post(tmp_path):
    con = full_workbook(tmp_path)
    assert query(con, "pos_duplicates", 2) == [
        (2455, Decimal("27948482.95"), Decimal("27830405.97"), Decimal("118076.98"))
    ]
    assert [r[2:] for r in query(con, "conversion", 0)] == [
        (2161516, 134224, 6.21),
        (2635601, 134224, 5.09),
        (2051700, 134112, 6.54),
        (2525785, 134112, 5.31),
    ]
    assert [n for _, n in query(con, "conversion", 1)] == [2161516, -67142, -30902, -13273, 1501, 2051700]
    assert query(con, "stockouts", 1) == [
        ("at close", 1284530, 793952, 712239, 19219),
        ("as reported", 999923, 537956, 475449, 18803),
    ]
    assert query(con, "high_water_mark")[0][:3] == (2329358, 19804, 0)
    assert query(con, "windows", 0) == [(600, 9, 1800, 1809)]
    assert query(con, "windows", -1) == [(53, 41, 13)]


@needs_batch
def test_trino(tmp_path):
    """Trino over an Iceberg copy of the full clickstream: same sessions, no QUALIFY, PIVOT needs GROUP BY."""
    from scripts import part06_trino as trino

    if not workbook.DB.exists():
        pytest.skip("needs the Part 6 workbook (scripts.part06_sql build --local)")
    trino.load()
    _, rows, _ = trino.run((ROOT / "sql/part06/trino/dialect.sql").read_text().split(";")[-2])
    assert rows == [[2161515, 134223, 6.21]]  # the workbook's 2,161,516 minus Part 23's erasure (load suppresses it)
    with pytest.raises(RuntimeError, match="mismatched input 'row_number'"):
        trino.run("SELECT count(*) FROM lakehouse.part06.clicks QUALIFY row_number() OVER (ORDER BY event_ts) = 1")
    _, rows, _ = trino.run(
        "SELECT * FROM (SELECT event_type, platform FROM lakehouse.part06.clicks) "
        "PIVOT (count(*) FOR platform IN ('app' AS app, 'web' AS web))"
    )
    assert len(rows) == 1
    _, rows, _ = trino.run(
        "SELECT count(DISTINCT anonymous_id), approx_distinct(anonymous_id) FROM lakehouse.part06.clicks"
    )
    exact, approx = rows[0]
    assert abs(approx - exact) / exact < 0.05
