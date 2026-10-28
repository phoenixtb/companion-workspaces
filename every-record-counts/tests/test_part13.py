"""Part 13: what the post claims about Trino over the lake, Trino federated with PostgreSQL, and ClickHouse.

The fixtures rebuild Part 13's state from scratch: the store-ops targets in PostgreSQL (seconds), and in ClickHouse
the lake database, the MergeTree copy of Part 10's lines and the refreshable materialized view (about a minute).
Trino tests need `make up-trino`, ClickHouse tests `make up-olap`; both need Part 10's part10.pos_monthly.
"""

import time

import psycopg
import pytest

from scripts import part13_engines as lab
from tests.conftest import needs_clickhouse, needs_stack, needs_trino

HISTORY, BASKETS = "b711851d", "241362c2"  # Part 11's digests (DuckDB, Polars and Spark agreed on them)
LINES = 72_670_965
FEDERATED = lab.SQL / "trino/federation.sql"


def needs_part10() -> None:
    from scripts import part07_iceberg as p7

    if not p7.catalog().table_exists("part10.pos_monthly"):
        pytest.skip("needs Part 10's tables: run tests/test_part10.py with `make up-spark` first")


@pytest.fixture(scope="module")
def seeded() -> None:
    needs_part10()
    with psycopg.connect(lab.PG, autocommit=True) as pg:
        pg.execute("DROP SCHEMA IF EXISTS store_ops CASCADE")
    lab.seed()


# ---------- PostgreSQL ----------
@needs_stack
def test_targets_are_seeded_and_trino_role_is_read_only(seeded):
    with psycopg.connect(lab.PG) as pg:
        n, stores, months = pg.execute(
            "SELECT count(*), count(DISTINCT store_id), count(DISTINCT month) FROM store_ops.sales_targets"
        ).fetchone()
    assert (n, stores, months) == (3_600, 600, 6)
    reader = lab.PG.replace("user=larkspur password=larkspur", "user=trino_reader password=trino-reader-lab-secret")
    with psycopg.connect(reader) as pg:
        assert pg.execute("SELECT count(*) FROM store_ops.sales_targets").fetchone()[0] == 3_600
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            pg.execute("UPDATE store_ops.sales_targets SET target_minor = 0")


# ---------- Trino ----------
@needs_trino
def test_trino_history_matches_part11_and_baskets_hit_the_per_node_limit():
    _, rows, stats = lab.trino(lab.part11("history", lab.TRINO_TABLE))
    assert len(rows) == 153 and lab.digest(rows) == HISTORY
    assert stats["processedRows"] == LINES
    with pytest.raises(RuntimeError, match="Query exceeded per-node memory limit"):
        lab.trino(lab.part11("baskets", lab.TRINO_TABLE))


@needs_trino
def test_postgres_pushdown_as_explained(seeded):
    plans = [lab.trino(s)[1][0][0] for s in lab.statements(FEDERATED)[:3]]
    pushed, text_group, regex = plans
    assert "oltp:Query[SELECT" in pushed and "GROUP BY" in pushed and "Aggregate[type = PARTIAL" not in pushed
    assert "Aggregate[type = PARTIAL, keys = [currency]]" in text_group and "constraint on [month]" in text_group
    assert "filterPredicate = regexp_like(set_by" in regex and "constraint on [month]" in regex


@needs_trino
def test_federated_query_pushes_filters_and_dynamic_filter(seeded):
    analyze, answer = lab.statements(FEDERATED)[3:5]
    plan = lab.trino(analyze)[1][0][0]
    assert "constraint on [month, currency]" in plan  # pushed into PostgreSQL
    assert "dynamicFilters = {store_id" in plan  # the lake side's store IDs, pushed into PostgreSQL too
    assert "pos_monthly" in plan and "constraint on [business_date]" in plan
    rows = lab.trino(answer)[1]
    assert len(rows) == 5
    store, target, sales, pct = rows[0]
    with psycopg.connect(lab.PG) as pg:
        assert (
            pg.execute(
                "SELECT target_minor FROM store_ops.sales_targets WHERE store_id = %s AND month = '2026-09-01'",
                (store,),
            ).fetchone()[0]
            == target
        )
    assert float(pct) == round(100 * sales / target, 1)
    assert [float(r[3]) for r in rows] == sorted(float(r[3]) for r in rows)


@needs_trino
def test_trino_refuses_materialized_views_on_a_rest_catalog():
    with pytest.raises(RuntimeError, match="createMaterializedView is not supported for Iceberg REST catalog"):
        lab.trino(lab.statements(FEDERATED)[6])


# ---------- ClickHouse ----------
@pytest.fixture(scope="module")
def olap() -> None:
    needs_part10()
    for db in ("serve", "larkspur", "lake"):
        lab.clickhouse(f"DROP DATABASE IF EXISTS {db} SYNC", fmt="")
    lab.ch_file("01_lake.sql", allow_database_iceberg=1)
    lab.ch_file("02_load.sql")
    lab.ch_file("03_serve.sql")
    lab.serve_wait()


@needs_clickhouse
def test_datalake_catalog_is_beta_and_needs_its_setting(olap):
    tier = lab.clickhouse("SELECT tier FROM system.settings WHERE name = 'allow_database_iceberg'")["data"]
    assert tier == [["Beta"]]
    with pytest.raises(RuntimeError, match="allow_database_iceberg"):
        lab.clickhouse(
            "CREATE DATABASE lake_without_setting ENGINE = DataLakeCatalog('http://polaris:8181/api/catalog')"
            " SETTINGS catalog_type = 'rest', warehouse = 'larkspur'",
            fmt="",
        )


@needs_clickhouse
@pytest.mark.parametrize("table", [lab.LAKE_TABLE, "larkspur.pos_lines"])
def test_clickhouse_answers_match_part11(olap, table):
    assert lab.digest(lab.clickhouse(lab.part11("history", table))["data"]) == HISTORY
    assert lab.digest(lab.clickhouse(lab.part11("baskets", table))["data"]) == BASKETS


@needs_clickhouse
def test_mergetree_copy_is_complete_and_index_skips_granules(olap):
    assert lab.clickhouse("SELECT count() FROM larkspur.pos_lines")["data"] == [[LINES]]
    tag = f"part13 test index {time.time_ns()}"
    lab.clickhouse(f"/* {tag} */ " + lab.tile_sql("tile_lines.sql", "larkspur.pos_lines", "GB-0042"))
    st = lab.ch_stats(tag)
    assert st["read_rows"] < 100_000  # a few granules of one store, not 72 million lines


@needs_clickhouse
def test_serving_table_agrees_with_the_lines_and_the_cache_hits(olap):
    # Count the table, not system.view_refreshes: an hourly refresh may be running and its counters restart.
    written = lab.clickhouse("SELECT count() FROM serve.daily_store_sales")
    distinct = lab.clickhouse(f"SELECT count() FROM (SELECT DISTINCT store_id, business_date FROM {lab.LAKE_TABLE})")
    assert written["data"] == distinct["data"]
    for store in ("GB-0042", "IN-0007", "SG-0100"):
        from_lines = lab.clickhouse(lab.tile_sql("tile_lines.sql", "larkspur.pos_lines", store))["data"]
        served = lab.clickhouse(lab.tile_sql("tile_daily.sql", "serve.daily_store_sales", store))["data"]
        assert served == from_lines and len(served) == 28
    tile = (lab.SQL / "clickhouse/country_tile.sql").read_text().strip().rstrip(";")
    hits = []
    for i in range(2):
        tag = f"part13 test cache {i} {time.time_ns()}"
        lab.clickhouse(f"/* {tag} */ {tile}", use_query_cache=1, query_cache_ttl=300)
        hits.append(lab.ch_stats(tag)["cache_hits"])
    assert hits[-1] == 1


@needs_clickhouse
@needs_trino
def test_trino_and_clickhouse_tiles_agree(olap):
    sql = lab.tile_sql("tile_lines.sql", lab.TRINO_TABLE, "GB-0042")
    assert lab.trino(sql)[1] == lab.clickhouse(lab.tile_sql("tile_lines.sql", lab.LAKE_TABLE, "GB-0042"))["data"]
