"""Part 4: a star schema with declared grains. Summing a basket-level measure at line grain inflates it; refunds
dated by refund move the UK number by the manifest's step 3; stock on hand can't be summed across days; Type 2
rows keep history, Type 1 columns restate it; as-of joins reproduce what the till charged; placeholders keep
early-arriving facts; the accumulating snapshot fills in as deliveries land.

Most tests build the star from the small generated world (no containers) and check it against that world's
manifest. `test_the_numbers_in_the_post` builds it from data/raw, when present, and checks the post's figures.
"""

from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from scripts import part04_star as star
from tests.conftest import ROOT

# UK net revenue, Q3 2026, from the star, with euros converted as of the sale (Part 5's fix).
UK_REVENUE = """WITH eur AS (SELECT rate_date, rate FROM raw_fx_rates WHERE base_currency = 'EUR'),
    gb AS (
        SELECT d.date, b.currency, b.amount_minor AS amount
        FROM fct_basket b JOIN dim_date d USING (date_key) JOIN dim_store s USING (store_key)
        WHERE s.country = 'GB'
        UNION ALL
        SELECT d.date, r.currency, -r.refund_minor
        FROM fct_refund r JOIN dim_date d ON d.date_key = r.refund_date_key
        JOIN dim_store s USING (store_key) WHERE s.country = 'GB')
    SELECT sum(CASE WHEN gb.currency = 'EUR' THEN sign(amount) * round(abs(amount) * e.rate)
                    ELSE amount END)::BIGINT
    FROM gb ASOF LEFT JOIN eur e ON gb.date >= e.rate_date
    WHERE gb.date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'"""


def build(raw: Path, db: Path) -> duckdb.DuckDBPyConnection:
    con = star.connect(db, raw_root=str(raw))
    star.build(con, db)
    return con


def query(con: duckdb.DuckDBPyConnection, name: str, statement: int = -1) -> list[tuple]:
    """The rows of one statement of sql/part04/<name>.sql (the last one by default)."""
    statements = con.extract_statements((ROOT / f"sql/part04/{name}.sql").read_text())
    return con.execute(statements[statement].query).fetchall()


@pytest.fixture(scope="module")
def small(small_raw, tmp_path_factory) -> tuple[duckdb.DuckDBPyConnection, dict]:
    raw, manifest = small_raw
    return build(raw, tmp_path_factory.mktemp("star") / "star.duckdb"), manifest


def one(con, sql: str):
    return con.execute(sql).fetchone()[0]


def test_every_fact_has_a_unique_declared_grain(small):
    con, _ = small
    grains = {
        "fct_sales_line": "transaction_id, line_no",
        "fct_basket": "transaction_id",
        "fct_refund": "refund_id",
        "fct_stock_daily": "date_key, store_key, product_key",
        "fct_po_line": "po_number, po_line",
    }
    for table, key in grains.items():
        assert one(con, f"SELECT count(*) - count(DISTINCT ({key})) FROM {table}") == 0, table
    # Two versions of one product are two product_keys: the snapshot is unique by SKU too.
    by_sku = "SELECT count(*) - count(DISTINCT (date_key, store_key, sku)) FROM fct_stock_daily JOIN dim_product"
    assert one(con, by_sku + " USING (product_key)") == 0


def test_placeholders_keep_every_early_arriving_fact(small):
    con, m = small
    pairs = {
        "fct_sales_line": "stg_sales_line",
        "fct_refund": "stg_refund",
        "fct_stock_daily": "raw_stock_snapshots",
        "fct_stock_adjustment": "raw_stock_adjustments",
        "fct_po_line": "raw_po_lines",
    }
    for fact, source in pairs.items():
        assert one(con, f"SELECT count(*) FROM {fact}") == one(con, f"SELECT count(*) FROM {source}"), fact
    # The manifest counts POS lines sold on the launch day (UTC) before the catalogue lists the product.
    launch_day = one(
        con,
        """SELECT count(*) FROM stg_sales_line l
           JOIN (SELECT sku, min(valid_from) - 1 AS launched FROM dim_product WHERE NOT is_inferred GROUP BY sku
                 HAVING min(valid_from) > DATE '2026-06-01') p USING (sku)
           WHERE l.channel = 'pos' AND l.event_ts::DATE = p.launched""",
    )
    assert launch_day == m["defects"]["pos_lines_sold_on_launch_day_before_catalogue_lists_them"]
    # After the catalogue catches up, no placeholder is left without attributes, and keys didn't move.
    assert one(con, "SELECT count(*) FROM dim_product WHERE category = '(not yet in catalogue)'") == 0
    assert one(con, "SELECT count(*) FROM dim_product WHERE is_inferred") > 0


def test_a_basket_measure_summed_at_line_grain_is_inflated(small):
    con, _ = small
    report, line_grain, receipts = query(con, "discount_grain_bug", 1)[0]
    assert line_grain == receipts
    assert report > 2 * line_grain
    # Exactly: each basket's discount once per line.
    weighted = one(
        con,
        """SELECT major(sum(b.discount_minor * b.lines)) FROM fct_basket b JOIN dim_date d USING (date_key)
           WHERE d.quarter = '2026-Q3' AND b.currency = 'GBP'""",
    )
    assert report == weighted


def test_joining_a_type_2_dimension_on_its_natural_key_fans_out(small):
    con, _ = small
    rows = {join_on: (n, matching) for join_on, n, matching in query(con, "promotion_as_of", 1)}
    lines, matching = rows["promo_key, version as of the sale"]
    assert matching == lines  # the version in force explains every discount the till gave
    assert rows["promo_id, latest version"][1] < lines  # edited promotions break "latest"
    assert rows["promo_id alone"][0] > lines  # one row per version: the line counted twice


def test_refunds_dated_by_refund_move_uk_revenue_by_waterfall_step_3(small):
    con, m = small
    step = {s["key"]: s["amount_minor"] for s in m["uk_net_revenue_q3"]["steps"]}["refund_dating"]
    rows = {r[0]: r for r in query(con, "refund_dating")}
    assert rows["UTC dates (waterfall step 3)"][3] * 100 == step


def test_stock_on_hand_summed_over_a_week_hides_stock_outs(small):
    con, m = small
    dashboard, per_day = query(con, "stock_weekly", 2)[0]
    stock = m["stock_availability_q3"]
    assert dashboard == round(100 * stock["legacy_weekly_sum"], 1)
    assert per_day == round(100 * stock["per_day_reported_snapshot"], 1)
    assert dashboard > per_day


def test_type_2_keeps_history_and_type_1_columns_restate_it(small):
    con, m = small
    moved = one(con, "SELECT count(DISTINCT sku) FROM dim_product WHERE version = 2")
    assert moved == m["defects"]["products_moved_in_1_aug_reorg"]
    assert one(con, "SELECT count(*) FROM dim_product WHERE version = 2 AND valid_from <> DATE '2026-08-01'") == 0
    assert one(con, "SELECT count(*) FROM dim_product WHERE is_current AND category <> current_category") == 0
    diffs = query(con, "category_as_of")
    assert diffs and sum(d[3] for d in diffs) == 0  # restating moves units between categories, never creates them


def test_prices_must_be_looked_up_at_the_instant_of_sale(small):
    con, m = small
    rows = {name: wrong for name, _, wrong in query(con, "price_as_of")}
    assert rows["price at the time of sale"] == 0
    assert rows["daily price, at local midnight"] > 0


def test_the_accumulating_snapshot_fills_in_and_the_audit_row_counts_what_was_left_out(small):
    con, m = small
    fill, otif, _ = query(con, "supplier_fill", 0)[0]
    assert fill == pytest.approx(100 * m["supplier_fill_q3"]["fill_rate_q3"], abs=0.5)
    assert otif == pytest.approx(100 * m["supplier_fill_q3"]["otif_q3"], abs=0.5)
    assert one(con, "SELECT count(*) FROM fct_po_line WHERE status = 'delivered' AND delivered_date_key = -1") == 0
    rejected, duplicate = con.execute(
        "SELECT rows_rejected, rows_duplicate FROM dim_audit WHERE table_name = 'fct_po_line'"
    ).fetchone()
    d = m["defects"]
    assert rejected + duplicate == d["supplier_feed_malformed_rows"] + d["supplier_feed_duplicate_rows"]


def test_the_star_reproduces_the_correct_uk_revenue(small):
    """Nothing lost or doubled on the way into the star: with euros converted as of the sale (Part 5), the
    sales and refund facts give exactly the manifest's correct UK figure."""
    con, m = small
    assert one(con, UK_REVENUE) == m["uk_net_revenue_q3"]["correct_minor"]


def test_the_numbers_in_the_post(lab_manifest, tmp_path):
    """The full-size world from data/raw: the figures quoted in Part 4."""
    con = build(ROOT / "data/raw", tmp_path / "star.duckdb")
    assert query(con, "refund_dating")[0][3] == Decimal("42511.91")
    assert query(con, "stock_weekly", 2)[0] == (97.2, 91.1)
    assert query(con, "discount_grain_bug", 1)[0][:2] == (Decimal("549590.78"), Decimal("165051.86"))
    assert one(con, "SELECT count(DISTINCT sku) FROM dim_product WHERE version = 2") == 56
    assert one(con, "SELECT count(*) FROM stg_unresolved_product WHERE source = 'sale line'") == 3629
    assert {r[0]: r[2] for r in query(con, "price_as_of")}["daily price, at local midnight"] == 1694
    assert one(con, UK_REVENUE) == lab_manifest["uk_net_revenue_q3"]["correct_minor"] == 2979067406
