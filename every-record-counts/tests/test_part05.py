"""Part 5: the same refunds as a Data Vault and as a wide table give the same answer; the vault is insert-only and
re-runs insert nothing; FX as of the transaction date and London business dates are exactly the manifest's
waterfall steps 4 and 5, landing on the correct UK figure; weekends carry Friday's rate; stock counted at the
close of the business day gives the correct availability; the vault reproduces a report as it was filed.

The small generated world is built into a Part 4 star, then extended by scripts/part05_shapes.py.
`test_the_numbers_in_the_post` does the same from data/raw, when present, and checks the post's figures.
"""

from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from scripts import part04_star as star
from scripts import part05_shapes as shapes
from tests.conftest import ROOT


def query(con: duckdb.DuckDBPyConnection, name: str, statement: int = -1) -> list[tuple]:
    """The rows of one statement of sql/part05/<name>.sql (the last one by default)."""
    statements = con.extract_statements((ROOT / f"sql/part05/{name}.sql").read_text())
    return con.execute(statements[statement].query).fetchall()


def one(con, sql: str):
    return con.execute(sql).fetchone()[0]


def build(raw: Path, tmp: Path) -> duckdb.DuckDBPyConnection:
    star_db = tmp / "star.duckdb"
    star_con = star.connect(star_db, raw_root=str(raw))
    star.build(star_con, star_db)
    star_con.close()
    con = shapes.prepare(str(raw), tmp / "part05.duckdb", star_db)
    shapes.build(con)
    return con


@pytest.fixture(scope="module")
def small(small_raw, tmp_path_factory) -> tuple[duckdb.DuckDBPyConnection, dict, Path]:
    raw, manifest = small_raw
    return build(raw, tmp_path_factory.mktemp("part05")), manifest, raw


def test_the_vault_is_insert_only_and_a_rerun_inserts_nothing(small):
    con, _, _ = small
    before = shapes.counts(con)
    assert set(shapes.load_vault(con, "infinity").values()) == {0}
    assert shapes.counts(con) == before
    # One row per business key in each hub; every refund links to a sale the vault knows.
    for hub, key in [("hub_sale", "sale_id"), ("hub_refund", "refund_id"), ("hub_store", "store_id")]:
        assert one(con, f"SELECT count(*) - count(DISTINCT {key}) FROM {hub}") == 0, hub
    assert one(con, "SELECT count(*) FROM hub_refund") == one(con, "SELECT count(*) FROM raw_refunds")
    assert one(con, "SELECT count(*) FROM lnk_refund_sale l ANTI JOIN hub_sale h USING (sale_hk)") == 0
    # A satellite row is a change: no key has two consecutive rows with the same hash_diff.
    repeats = """SELECT count(*) FROM (SELECT hash_diff = lag(hash_diff) OVER (PARTITION BY sale_hk ORDER BY load_ts)
                 AS same FROM sat_sale_order) WHERE same"""
    assert one(con, repeats) == 0
    # 127 nightly extracts became one row per status change: placed, then fulfilled (or only one of them).
    histories = {h for h, _ in query(con, "vault_history")}
    assert histories <= {"placed -> fulfilled", "placed", "fulfilled"}


def test_vault_and_wide_table_give_the_same_answer(small):
    con, _, _ = small
    vault, wide = query(con, "refunds_two_shapes", 0), query(con, "refunds_two_shapes", 1)
    assert vault and vault == wide
    assert one(con, "SELECT count(*) FROM wide_refund") == one(con, "SELECT count(*) FROM raw_refunds")


def test_fx_as_of_and_london_dates_are_waterfall_steps_4_and_5(small):
    con, m, _ = small
    revenue = m["uk_net_revenue_q3"]
    step = {s["key"]: s["amount_minor"] for s in revenue["steps"]}
    rows = query(con, "uk_revenue_fx_dates", 1)
    after_3 = revenue["legacy_board_deck_minor"] - step["duplicate_pos_events"] - step["hard_deleted_orders"]
    after_3 -= step["refund_dating"]
    assert rows[0][1] * 100 == after_3
    assert rows[1][2] * 100 == -step["fx_today_rate"]
    assert rows[2][2] * 100 == -step["utc_business_date"]
    assert rows[2][1] * 100 == revenue["correct_minor"]
    split = query(con, "uk_revenue_fx_dates", 2)
    assert split[-1][0] == "both" and split[-1][4] * 100 == -step["utc_business_date"]
    assert split[0][4] + split[1][4] == split[-1][4]


def test_weekends_carry_fridays_rate(small):
    con, m, _ = small
    carried = one(
        con,
        """SELECT count(*) FROM dim_fx_rate WHERE base_currency = 'EUR' AND carried_forward
           AND date BETWEEN DATE '2026-06-01' AND DATE '2026-10-04'""",
    )
    assert carried == m["defects"]["calendar_days_without_fx_rate"]
    assert one(con, "SELECT count(*) FROM dim_fx_rate WHERE carried_forward AND isodow(rate_date) <> 5") == 0


def test_stock_counted_at_close_of_day_gives_the_correct_availability(small):
    con, m, _ = small
    stock = m["stock_availability_q3"]
    assert query(con, "stock_close_of_day") == [
        (round(100 * stock["per_day_reported_snapshot"], 1), round(100 * stock["correct_close_of_day"], 1))
    ]
    late = query(con, "stock_close_of_day", 0)
    assert {r[0]: r[2] for r in late} == {"after_delivery_06_00": 1, "close_23_30": 0}


def test_the_vault_reproduces_the_report_as_filed(small, tmp_path):
    """A vault loaded only up to the filing time and the full vault asked 'as filed' give the same answer."""
    con, _, raw = small
    early = star.connect(tmp_path / "early.duckdb", raw_root=str(raw))
    early.execute("CREATE MACRO major(minor) AS CAST(minor AS DECIMAL(18, 0)) * 0.01")
    early.execute(shapes.sql("vault_tables"))
    shapes.load_vault(early, shapes.LOADS[0][1])
    then = {v: (n, gbp) for v, n, gbp in query(early, "as_filed")}
    now = {v: (n, gbp) for v, n, gbp in query(con, "as_filed")}
    assert now["as filed, 1 Oct 06:00 UTC"] == then["as of now"] == then["as filed, 1 Oct 06:00 UTC"]
    assert now["as of now"][0] > now["as filed, 1 Oct 06:00 UTC"][0]  # late till uploads arrived afterwards


def test_the_numbers_in_the_post(lab_manifest, tmp_path):
    """The full-size world from data/raw: the figures quoted in Part 5."""
    con = build(ROOT / "data/raw", tmp_path)
    steps = query(con, "uk_revenue_fx_dates", 1)
    assert [r[1] for r in steps] == [Decimal("29781578.58"), Decimal("29790375.96"), Decimal("29790674.06")]
    split = {r[0]: r[4] for r in query(con, "uk_revenue_fx_dates", 2)}
    assert split == {"refunds": Decimal("479.68"), "sales": Decimal("-181.58"), "both": Decimal("298.10")}
    assert query(con, "uk_revenue_fx_dates")[0][1:3] == (92, Decimal("1264.16"))
    assert query(con, "stock_close_of_day") == [(91.1, 88.5)]
    assert [r[1:] for r in query(con, "as_filed")] == [(5625, Decimal("264816.30")), (5646, Decimal("265838.13"))]
    assert query(con, "refunds_two_shapes", 0) == query(con, "refunds_two_shapes", 1)
    assert one(con, "SELECT count(*) FROM hub_sale") == 2509747
    assert dict(query(con, "vault_history")) == {"placed -> fulfilled": 148390, "fulfilled": 27278, "placed": 4721}
