"""Part 17: what the post claims about the star schema rebuilt with dbt.

The module fixture runs the post's sequence on the lake (about ten minutes with `make up-trino`): Polaris grants,
landing, a first `dbt build`, a weekly then a daily replay of the snapshots, `dbt build` again (models, snapshots, data
and unit tests) and the check against Part 5's numbers. Snapshots are dropped before each replay and every other table
is replaced, so the tests can be re-run. The dbt v2 tests need `make dbt-oss-install` and no containers.
"""

import json
import re
import sys
from pathlib import Path

import pytest

from scripts import part17_dbt as lab
from tests.conftest import ROOT, needs_batch

dbt_v1 = pytest.mark.skipif(not (Path(sys.executable).parent / "dbt").exists(), reason="needs `make dbt-install`")
dbt_v2 = pytest.mark.skipif(not lab.DBT2.exists(), reason="needs `make dbt-oss-install`")


def plain(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


@pytest.fixture(scope="module")
def run() -> dict:
    lab.setup()
    out = {"landed": lab.land()}
    out["first_build"] = plain(lab.dbt("build", echo=False).stdout)  # the int_ models history() compares against
    lab.replay(every=7)
    out["weekly"] = lab.history()
    out["daily_runs"] = lab.replay()
    out["daily"] = lab.history()
    out["build"] = plain(lab.dbt("build", echo=False).stdout)
    out["check"] = lab.check()
    return out


@needs_batch
@dbt_v1
def test_landing_copies_every_raw_row(run):
    con = lab.raw_views()
    for name, query in lab.LANDED.items():
        assert run["landed"][name] == con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0], name
    assert run["landed"]["pos_uploads"] == 2_338_812  # re-sent baskets included: dedup is the model's job


@needs_batch
@dbt_v1
def test_daily_snapshots_rebuild_the_history(run):
    rows = {r[0]: r[1:] for r in run["daily"]["snapshot rows"]}
    assert rows["snp_product"] == [2_056, 2_000, 56]
    assert rows["snp_promotion"] == [63, 48, 15]
    assert run["daily"]["snapshot vs extracts"][0] == [2_056, 2_056, 0, 0]  # same versions, same first days
    assert run["daily"]["snapshot vs admin history"][0] == [63, 63, 0, 0]
    assert run["daily"]["lost versions"] == []
    assert len(run["daily_runs"]) == 126


@needs_batch
@dbt_v1
def test_weekly_snapshots_lose_history(run):
    assert run["weekly"]["snapshot vs admin history"][0] == [62, 63, 0, 1]
    assert [r[:2] for r in run["weekly"]["lost versions"]] == [["PR-0045", 1]]
    assert run["weekly"]["lost versions"][0][4] == 1_908  # sale lines priced under the version nobody saw
    _, _, only_snapshot, only_source = run["weekly"]["snapshot vs extracts"][0]
    assert only_snapshot > 100 and only_source > 100  # versions dated by the weekly run, not the day they began


@needs_batch
@dbt_v1
def test_build_passes_every_test(run):
    done = re.search(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+)", run["build"])
    assert done and done.group(3) == "0", run["build"][-3000:]
    assert "unique_combination_hashed_fct_stock_daily" in run["build"]
    for fact in lab.FACTS:  # every fact has a grain test
        assert re.search(rf"PASS \S*(unique|combination)\S*{fact}", run["build"]), fact
    assert "a_resent_basket_counts_once" in run["build"]


@needs_batch
@dbt_v1
def test_star_matches_part5(run):
    c = run["check"]
    m = json.loads((ROOT / "data/raw/_manifest.json").read_text())
    assert c["uk_minor"] == m["uk_net_revenue_q3"]["correct_minor"] == 2_979_067_406  # GBP 29,790,674.06
    assert round(c["stock"][0], 6) == m["stock_availability_q3"]["per_day_reported_snapshot"]
    assert round(c["stock"][1], 6) == m["stock_availability_q3"]["correct_close_of_day"]
    assert c["counts"]["fct_sales_line"] == 6_658_738 and c["counts"]["fct_stock_daily"] == 11_659_200
    if c["part05"]:
        assert c["counts"] == c["part05"]
        assert c["versions_same"]


@needs_batch
@dbt_v1
def test_unit_test_catches_a_resent_basket(run):
    out = lab.unit_demo()
    assert "FAIL 1 stg_pos_sale_lines::a_resent_basket_counts_once" in out
    assert "where c.event_id is null" in (lab.PROJECT / "models/staging/stg_pos_sale_lines.sql").read_text()  # restored


@dbt_v1
@dbt_v2
def test_v2_refuses_the_trino_project_v1_accepts():
    v1 = lab.dbt("parse", "--target", "local", echo=False)
    assert v1.returncode == 0  # dbt-core with the DuckDB adapter ignores the Trino-only key without a word
    v2 = lab.dbt("parse", "--target", "local", v2=True, echo=False)
    assert v2.returncode != 0
    assert "Unrecognized key `larkspur.+on_table_exists`" in plain(v2.stdout + v2.stderr)


@needs_batch
@dbt_v1
@dbt_v2
def test_v2_reads_only_duckdb_1x_files(run):
    lab.extract()  # DuckDB 2.0's own storage format
    debug = lab.dbt("debug", v2=True, project=lab.SUBSET, echo=False)
    assert debug.returncode != 0 and "we can only read versions between 64 and 68" in plain(debug.stdout + debug.stderr)
    lab.extract("v1.5.0")
    assert lab.dbt("build", v2=True, project=lab.SUBSET, echo=False).returncode == 0
    assert lab.dbt("build", project=lab.SUBSET, echo=False).returncode == 0  # v1 + dbt-duckdb on the same file
    show = lab.dbt("show", "--inline", "select version() as v", v2=True, project=lab.SUBSET, echo=False)
    assert "v1.5.4" in plain(show.stdout)


@dbt_v1
@dbt_v2
def test_v2_catches_a_misspelt_key_v1_ignores():
    out = lab.strict()
    assert out["v1 parse, with `+materialised: view`"].returncode == 0
    v2 = out["v2 parse, with `+materialised: view`"]
    assert v2.returncode != 0 and "Unrecognized key `larkspur_subset.+materialised`" in plain(v2.stdout + v2.stderr)
    # The upgrade guide says these v1 flags are errors in v2; 2.0.8 accepted them.
    assert out["v2 parse --no-partial-parse"].returncode == 0
    assert out["v2 run -m dim_date"].returncode == 0
    for label in ("v2 parse --no-partial-parse", "v2 run -m dim_date"):
        assert "dbt1700" not in plain(out[label].stdout + out[label].stderr)
