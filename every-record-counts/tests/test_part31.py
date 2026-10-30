"""Part 31: three metrics defined once in Cube, served over REST, the SQL API and Superset, governed by OPA."""

import json
import socket
from decimal import Decimal

import psycopg
import pytest

from tests.conftest import ROOT, stack_up, trino_up


def serve_up() -> bool:
    for port in (4000, 15432, 8088):
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return stack_up() and trino_up()


pytestmark = pytest.mark.skipif(not serve_up(), reason="needs `make up-serve` (Parts 17-25's gold tables)")

PACK = json.loads((ROOT / "governance/board-pack-2026-q3.json").read_text())


@pytest.fixture(scope="module")
def lab():
    from scripts import part31_serve

    return part31_serve


@pytest.fixture(scope="module")
def metrics(lab) -> dict:
    return lab.metrics(show=False)


def test_the_three_numbers_are_the_board_packs(metrics):
    assert metrics["uk_net_revenue_q3"] == Decimal(PACK["uk_net_revenue_q3_minor"]) / 100 == Decimal("29790674.06")
    assert round(100 * metrics["stock_availability_q3"], 1) == PACK["stock_available_at_close_pct"] == 88.5
    assert (metrics["converted_sessions"], metrics["sessions"]) == (134_223, 2_161_515)
    assert round(100 * metrics["conversion_rate"], 2) == PACK["conversion_pct"]


def test_cube_matches_dbts_report_on_every_country_day(lab):
    out = lab.check()
    assert out["differing"] == 0 and out["country_days"] > 300
    assert all(ok for _, _, ok in out["rows"])


def test_ad_hoc_sql_gives_several_answers_and_the_definition_gives_cubes(lab, metrics):
    found = {label.split(",")[0] + "|" + label: rows for label, _, rows in lab.opinions()}
    revenue = [Decimal(rows[0][0]) for key, rows in found.items() if key.startswith("revenue|")]
    stock = [float(rows[0][0]) for key, rows in found.items() if key.startswith("stock|")]
    assert revenue[0] == metrics["uk_net_revenue_q3"] and len(set(revenue)) == 4
    assert stock == [88.5, 91.1, 97.2]
    conversion = [rows[0] for key, rows in found.items() if key.startswith("conversion|")]
    assert conversion[0][:2] == [929_032, 57_486]  # the right SQL as analyst_gb: OPA keeps GB rows only
    assert float(conversion[1][2]) == 5.31  # marketing's old rule


def test_each_scope_reads_through_its_own_trino_identity(lab):
    out = lab.scopes()
    assert [c for c, _, _ in out["sessions"]["all"]] == ["GB", "IN", "SG"]
    assert out["sessions"]["GB"] == [("GB", 929_032, 57_486)]
    assert out["sessions"]["SG"] == [("SG", 302_318, 19_058)]
    assert "unknown scope: FR" in out["sessions"]["FR"]
    # cached answers run nothing in Trino, so a re-run within two minutes may list fewer users, never other ones
    assert {u for u, _ in out["users"]} <= {"cube", "analyst_gb", "analyst_in", "analyst_sg"}


def test_a_clients_time_zone_cannot_move_business_dates(lab):
    values = set(lab.timezone_demo().values())
    assert values == {Decimal("29790674.06")}


def test_detailed_queries_hit_the_default_row_limit(lab):
    out = lab.limit_demo()
    assert out["rows"] == 10_000 and out["total"] > out["rows"]


def test_sql_api_serves_the_same_measures_and_keeps_ratios_ratios(lab, metrics):
    out = lab.sql_api()
    gb = dict(out["net revenue by country, Q3"])["GB"]
    assert isinstance(gb, float) and "net_revenue float8" in out["net revenue by country, Q3 types"]
    assert Decimal(str(gb)) == metrics["uk_net_revenue_q3"]  # exact at this size, but a double, not a decimal
    with psycopg.connect(lab.SQL_API.format(lab.env("CUBE_SQL_PASSWORD")), autocommit=True) as con:
        measure = con.execute("SELECT MEASURE(availability) FROM stock_availability").fetchone()[0]
        summed = con.execute("SELECT SUM(availability) FROM stock_availability").fetchone()[0]
    assert summed == measure < 1  # a BI tool's SUM over a ratio still gets the ratio


def test_superset_charts_read_the_metrics_through_cube(lab, metrics):
    out = lab.superset()
    assert Decimal(str(out["net_revenue"]["value"])) == metrics["uk_net_revenue_q3"]
    assert out["stock_availability"]["value"] == pytest.approx(metrics["stock_availability_q3"])
    assert out["conversion"]["value"] == pytest.approx(metrics["conversion_rate"])
    assert "MEASURE(net_revenue)" in out["net_revenue"]["query"]
