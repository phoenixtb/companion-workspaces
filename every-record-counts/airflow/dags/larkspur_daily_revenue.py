"""Part 19: Larkspur's daily revenue pipeline. One run per UTC upload day: land the day's till uploads, rebuild the
microbatch facts for the days those uploads can touch, check, then publish the report.

Part 21 adds three tasks: accept the day's POS events by their natural key (one disposition per event), land the PSP's
settlement files that have arrived, and reconcile (the daily control table) as a gate before publish.

Part 22 declares each task's input and output tables as OpenLineage datasets (a BashOperator can't say what it read)
and hands the dbt task's run ID to dbt-ol, so the model runs become children of the Airflow task in the lineage graph.

The DAG decides only *when* and *in what order*. What each step does lives in the lab (scripts/part19_pipeline.py,
sql/part19/checks.sql, the dbt project), so the same commands run by hand, in CI and here.
"""

import json
import subprocess
from datetime import timedelta

import larkspur_alerts
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import (
    DAG,
    Asset,
    CronDataIntervalTimetable,
    DeadlineAlert,
    DeadlineReference,
    Metadata,
    SyncCallback,
    task,
)
from openlineage.client.event_v2 import Dataset

LAB = "cd /opt/lab && /opt/lab-venv/bin/python -m scripts.part19_pipeline"
LAB21 = "cd /opt/lab && /opt/lab-venv/bin/python -m scripts.part21_reconcile"

# Assets name the tables, not the tasks: anything that needs the published report waits for this, whoever makes it.
RAW_POS = Asset(uri="iceberg://larkspur/raw/pos_uploads", name="raw.pos_uploads")
SALES_FACTS = Asset(uri="iceberg://larkspur/gold/fct_sales_line", name="gold.fct_sales_line")
REPORT = Asset(uri="iceberg://larkspur/gold/rpt_daily_net_revenue_v2", name="gold.rpt_daily_net_revenue_v2")


def table(name: str) -> Dataset:
    """Part 22: a lake table as dbt-ol names it (Trino's address + catalog.namespace.table), so the tasks that write a
    table with PyIceberg and the dbt models that read it meet at one node in the lineage graph."""
    return Dataset(namespace="trino://trino:8080", name=f"lakehouse_pipeline.{name}")


def files(path: str) -> Dataset:
    return Dataset(namespace="file", name=f"/opt/lab/data/raw/{path}")


CONTROL_INPUTS = "raw.payments raw.refunds raw.psp_settlements silver.pos_event_dispositions gold.fct_basket".split()

# dbt-ol reads its parent (this task's OpenLineage run) from the environment; the macros come from the provider.
OL_PARENT = {
    "OPENLINEAGE_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}",
    "OPENLINEAGE_ROOT_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}",
    "OPENLINEAGE_NAMESPACE": "larkspur",
    # instead of dbt-run-<project>; no dot: OpenMetadata 2.0.7 can't match a pipeline whose name has one (Part 22)
    "OPENLINEAGE_DBT_JOB_NAME": "larkspur_daily_revenue_dbt",
}

with DAG(
    dag_id="larkspur_daily_revenue",
    # Data intervals, not trigger times: the run for upload day D covers [D 00:00, D+1 00:00) UTC, starts when the
    # day has closed, and `ds` is D. Airflow 3 turns a bare cron string into a trigger timetable without intervals.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,  # history is a deliberate backfill, not a surprise when the DAG is unpaused
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    # Experimental in Airflow 3.3: Farah's numbers are due by 06:00 UTC the morning after the upload day.
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=30),
        callback=SyncCallback("larkspur_alerts.revenue_late", kwargs={"report": REPORT.name}),
    ),
    tags=["larkspur", "part19"],
) as dag:
    # The till uploads for day D land in the raw zone during D. Wait for them without holding a worker slot: in
    # `reschedule` mode the sensor gives its slot back between pokes. (A deferrable sensor would hand the wait to the
    # triggerer, a service this lab doesn't run.)
    wait_for_uploads = BashSensor(
        task_id="wait_for_uploads",
        bash_command="ls /opt/lab/data/raw/pos/events/upload_date={{ ds }}/*.jsonl.gz > /dev/null 2>&1",
        mode="reschedule",
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=6),
        retries=0,
    )

    ingest = BashOperator(
        task_id="ingest_pos_uploads",
        bash_command=f"{LAB} ingest --day {{{{ ds }}}}",
        pool="lakehouse",
        inlets=[files("pos/events")],
        outlets=[RAW_POS, table("raw.pos_uploads")],
    )

    # Part 21: every event the tills sent gets one disposition (accepted, resend, collision) before anything counts it.
    accept = BashOperator(
        task_id="accept_pos_events",
        bash_command=f"{LAB21} accept --day {{{{ ds }}}}",
        pool="lakehouse",
        inlets=[table("raw.pos_uploads")],
        outlets=[table("silver.pos_event_dispositions")],
    )

    # The PSP's files arrive on their own clock: whatever is dated up to the morning after the upload day has landed.
    land_settlements = BashOperator(
        task_id="land_settlements",
        bash_command=f"{LAB21} land-psp --until {{{{ macros.ds_add(ds, 1) }}}}",
        pool="lakehouse",
        inlets=[files("psp/settlements")],
        outlets=[table("raw.psp_settlements")],
    )

    # Which models and which microbatch days a run for D rebuilds is the lab's decision (scripts/part19_pipeline.py).
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{LAB} build --day {{{{ ds }}}}",
        env=OL_PARENT,
        append_env=True,  # keep the container's environment (PATH, LAB_*, OPENLINEAGE__*)
        pool="lakehouse",
        outlets=[SALES_FACTS],
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=f"{LAB} check --day {{{{ ds }}}}",
        retries=0,  # a broken rule is not transient: retrying it only delays the page
    )

    # Part 21: the books must balance before anyone sees the numbers. Ledger vs gold vs PSP, stock and clickstream
    # control totals, for the last week of closed days; any difference fails the task, and publish never runs.
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{LAB21} control --day {{{{ ds }}}}",
        pool="lakehouse",
        retries=0,  # a difference in the books is not transient
        on_failure_callback=larkspur_alerts.reconciliation_failed,
        inlets=[table(t) for t in CONTROL_INPUTS],
        outlets=[table("gold.daily_controls")],
    )

    @task(pool="lakehouse", inlets=[table("gold.rpt_daily_net_revenue_v2")], outlets=[REPORT])
    def publish(ds=None):
        out = subprocess.run(
            ["/opt/lab-venv/bin/python", "-m", "scripts.part19_pipeline", "publish", "--day", ds],
            cwd="/opt/lab",
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        info = json.loads(out.strip().splitlines()[-1])
        yield Metadata(REPORT, {"upload_date": ds, "snapshot_id": str(info["snapshot_id"])})

    wait_for_uploads >> ingest >> accept >> dbt_build >> quality_checks >> reconcile >> publish()
    land_settlements >> reconcile
