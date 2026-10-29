"""Part 19: what the post claims about orchestrating the daily revenue pipeline with Airflow 3.

The module fixture (about 2.5 minutes with `make up-orchestrate`; needs Part 18's production models) partitions
raw.pos_uploads if needed, unpauses both DAGs, then runs a two-day backfill (10 and 11 September) through Airflow, with
`--reprocess-behavior completed` so it re-runs days that already ran: re-running must change nothing.
"""

import json
import subprocess
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

import pytest

from scripts import part09_catalog as cat9
from scripts import part19_pipeline as lab
from tests.conftest import ROOT, needs_batch


def airflow_up() -> bool:
    try:
        with urllib.request.urlopen(f"{lab.AIRFLOW}/api/v2/monitor/health", timeout=2) as r:
            return json.load(r)["scheduler"]["status"] == "healthy"
    except OSError:
        return False


needs_airflow = pytest.mark.skipif(not airflow_up(), reason="needs `make up-orchestrate` (Part 19)")
dbt_v1 = pytest.mark.skipif(not (Path(sys.executable).parent / "dbt").exists(), reason="needs `make dbt-install`")
pytestmark = [needs_batch, needs_airflow]

DAG_ID, MAINT = "larkspur_daily_revenue", "larkspur_lake_maintenance"
DAYS = (date(2026, 9, 10), date(2026, 9, 11))


def airflow(*args: str) -> str:
    cmd = ["make", "-s", "airflow", "CMD=" + " ".join(args)]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True).stdout


@pytest.fixture(scope="module")
def run() -> dict:
    lab.setup()
    airflow("variables", "set", "lake_retention", "1h")
    for dag in (MAINT, DAG_ID):
        airflow("dags", "unpause", dag)
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    before = lab.fingerprint(DAYS[0])
    airflow(
        "backfill", "create", "--dag-id", DAG_ID, "--from-date", str(DAYS[0]), "--to-date", str(DAYS[1]),
        "--reprocess-behavior", "completed", "--max-active-runs", "1",
    )  # fmt: skip
    time.sleep(10)
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        runs = lab.api(f"/dags/{DAG_ID}/dagRuns?limit=100")["dag_runs"]
        mine = [r for r in runs if r["logical_date"][:10] in {str(d) for d in DAYS}]
        if mine and all(r["state"] in ("success", "failed") for r in mine):
            break
        time.sleep(10)
    lab.wait([MAINT])
    events = lab.api(f"/assets/events?limit=200&order_by=-timestamp&timestamp_gte={started}")["asset_events"]
    maint = lab.api(f"/dags/{MAINT}/dagRuns?limit=200")["dag_runs"]
    return {
        "runs": mine,
        "before": before,
        "after": lab.fingerprint(DAYS[0]),
        "events": events,
        "maintenance": [r for r in maint if (r.get("start_date") or "") >= started],
        "q3": lab.q3(),
    }


def test_dags_parse_without_errors():
    assert "No data found" in airflow("dags", "list-import-errors")


def test_backfill_runs_every_day_through_publish(run):
    assert len(run["runs"]) == 2 and all(r["state"] == "success" for r in run["runs"])
    tasks = lab.api(f"/dags/{DAG_ID}/dagRuns/{run['runs'][0]['dag_run_id']}/taskInstances")["task_instances"]
    assert {t["task_id"]: t["state"] for t in tasks} == {
        "wait_for_uploads": "success",
        "ingest_pos_uploads": "success",
        "dbt_build": "success",
        "quality_checks": "success",
        "publish": "success",
    }


def test_rerunning_a_day_changes_nothing(run):
    assert run["after"] == run["before"]


def test_uk_q3_as_published_is_exact(run, lab_manifest):
    assert run["q3"]["GB"] == lab_manifest["uk_net_revenue_q3"]["correct_minor"] == 2_979_067_406


def test_publish_event_names_the_published_snapshot(run):
    published = [e for e in run["events"] if e["name"] == "gold.rpt_daily_net_revenue_v2"]
    assert {e["extra"]["upload_date"] for e in published} >= {str(d) for d in DAYS}
    tag = lab.trino("""SELECT snapshot_id FROM lakehouse_pipeline.gold."rpt_daily_net_revenue_v2$refs"
                       WHERE name = 'published'""")[0][0]
    assert str(tag) == published[0]["extra"]["snapshot_id"]  # the newest event points at the tagged snapshot


def test_maintenance_is_triggered_by_the_publish_asset(run):
    assert run["maintenance"] and all(r["run_type"] == "asset_triggered" for r in run["maintenance"])
    assert all(r["state"] == "success" for r in run["maintenance"])


def test_ingesting_a_day_twice_leaves_one_copy():
    first, second = lab.ingest(DAYS[0]), lab.ingest(DAYS[0])
    assert first["in_table"] == second["in_table"] == first["events"] > 0
    files = lab.trino(f"""SELECT count(*) FROM lakehouse_pipeline.raw."pos_uploads$files"
                          WHERE partition.upload_date = DATE '{DAYS[0]}'""")[0][0]
    assert files == 1


def test_trino_refuses_retention_below_its_minimum():
    _, err = cat9.trino(
        "ALTER TABLE lakehouse_pipeline.gold.fct_basket EXECUTE expire_snapshots(retention_threshold => '1h')"
    )
    assert err and "shorter than the minimum retention configured in the system (7.00d)" in err


@dbt_v1
def test_checks_stop_a_restated_closed_day(run):
    """A hand-made rate change on 1 September restates a closed day; the check refuses it. dbt puts the rate back."""
    from scripts import part17_dbt as p17

    day = date(2026, 9, 20)
    lab.fx_correction(date(2026, 9, 1))
    try:
        p17.dbt("run", "--target", "prod", "--select", "rpt_daily_net_revenue", echo=False)
        moved = lab.checks(day)["closed_days_moved"]
        assert [(r[0], r[1]) for r in moved] == [("IN", "2026-09-01")]
    finally:
        p17.dbt("run", "--target", "prod", "--select", "dim_fx_rate", "rpt_daily_net_revenue", echo=False)
    assert not any(lab.checks(day).values())


def test_deadline_callback_pages_only_for_runs_that_matter(caplog):
    """The callback gets {"dag_run": …, "deadline": …}; a backfilled day is late by definition and isn't paged."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("larkspur_alerts", ROOT / "airflow/dags/larkspur_alerts.py")
    alerts = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(alerts)
    with caplog.at_level("INFO", logger=alerts.__name__):
        alerts.revenue_late("r", context={"dag_run": {"run_type": "backfill", "dag_run_id": "b"}})
        alerts.revenue_late("r", context={"dag_run": {"run_type": "scheduled", "dag_run_id": "s"}})
    levels = [(r.levelname, "DEADLINE MISSED" in r.getMessage()) for r in caplog.records]
    assert levels == [("INFO", False), ("WARNING", True)]
