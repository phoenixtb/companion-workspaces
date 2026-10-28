"""Part 18: what the post claims about incremental models, slim CI, dev targets, contracts and versions.

The module fixture runs the post's sequence (about 40 minutes with `make up-trino`; needs Part 17's models in
production and Part 16's CDC replicas): grants for the `developer` principal, production's manifest from the Part 17
tag, the replay of till uploads from 21 September to 5 October against three variants of the sales fact, a full
rebuild to compare with, the deploy of the new models to production, slim CI and the contract checks. The dev
namespace is emptied and raw.pos_uploads re-landed at the start, so the tests can be re-run.
"""

import re
import sys
from pathlib import Path

import pytest

from scripts import part09_catalog as cat9
from scripts import part18_dbt as lab
from tests.conftest import needs_batch

dbt_v1 = pytest.mark.skipif(not (Path(sys.executable).parent / "dbt").exists(), reason="needs `make dbt-install`")
pytestmark = [needs_batch, dbt_v1]


@pytest.fixture(scope="module")
def run() -> dict:
    lab.setup()
    lab.prod_state()
    out = {"replay": lab.replay()}
    lab.build_full()
    out["compare"] = lab.compare()
    out["late"] = lab.late()
    out["cost"] = lab.cost()
    out["ci"] = lab.ci()
    out["contract"] = lab.contract()
    out["deploy"] = lab.deploy()
    return out


def test_developer_cannot_touch_production():
    _, err = cat9.trino("DROP TABLE lakehouse_dev.gold.dim_date")
    assert err and "not authorized" in err


def test_event_time_high_water_mark_drops_late_lines(run):
    naive = run["compare"]["fct_sales_line_naive"]
    assert sum(n for n, _ in naive["missing"].values()) > 1_000 and naive["extra"] == 0


def test_lookback_of_one_day_drops_lines_uploaded_after_midnight(run):
    one = run["compare"]["fct_sales_line_lookback1"]
    assert sum(n for n, _ in one["missing"].values()) > 0 and one["extra"] == 0


def test_lookback_of_two_days_equals_a_full_rebuild(run):
    c = run["compare"]
    assert c["fct_sales_line"]["missing"] == {} and c["fct_sales_line"]["extra"] == 0
    assert c["fct_sales_line"]["rows"] == c[lab.FULL]["rows"] == 6_658_738
    assert c["fct_sales_line"]["sum"] == c[lab.FULL]["sum"]


def test_the_late_basket_lands_on_its_business_day(run):
    late = run["late"]
    assert late["fct_sales_line_naive"] == [] and late["fct_sales_line_lookback1"] == []
    assert [r[0] for r in late["fct_sales_line"]] == [20260930] and late["fct_sales_line"] == late[lab.FULL]


def test_daily_run_reads_less_than_a_rebuild(run):
    cost = run["cost"]
    assert cost["fct_sales_line"]["read_mib"] < cost[lab.FULL]["read_mib"]
    assert cost["fct_sales_line"]["cpu_s"] < cost[lab.FULL]["cpu_s"]


def test_slim_ci_builds_only_what_changed(run):
    ci = run["ci"]
    assert ci["returncode"] == 0, ci["build"][-3000:]
    assert 0 < len(ci["picked"]) < len(ci["every"])
    assert "model:dim_store" not in ci["picked"] and "model:fct_sales_line" in ci["picked"]


def test_contract_on_trino(run):
    c = run["contract"]
    assert "This model has an enforced contract that failed" in c["rename"]
    assert c["null"] and "NULL value not allowed for NOT NULL column" in c["null"]
    assert c["dup"] is None  # primary_key is declared, but nothing enforces it on Trino
    assert re.search(r"FAIL \d+ \S*rpt_daily_net_revenue_v2", c["test"])


def test_production_runs_the_new_models(run):
    d = run["deploy"]
    assert d["returncode"] == 0, d["build"][-3000:]
    rows = cat9.trino("SELECT count(*), count(uploaded_at) FROM lakehouse_pipeline.gold.fct_sales_line")[0]
    assert rows[0][0] == 6_658_738 and rows[0][1] > 0


def test_a_batch_filter_reaches_every_raw_upload_scan(run):
    scans = [s for s in lab.explain() if "raw.pos_uploads" in s]
    assert len(scans) == 3 and all("2026-09-15 00:00:00" in s for s in scans)


def test_lateness_crosses_one_utc_midnight_at_most():
    con = lab.p17.raw_views()
    sql = (lab.ROOT / "sql/part18/lateness.sql").read_text()
    events, later_day, baskets, worst = con.execute(sql).fetchone()
    assert (later_day, baskets) == (22_454, 20_029) and worst.total_seconds() < 24 * 3600
