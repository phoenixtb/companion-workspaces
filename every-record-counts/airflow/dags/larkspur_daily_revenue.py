"""Part 19: Larkspur's daily revenue pipeline. One run per UTC upload day: land the day's till uploads, rebuild the
microbatch facts for the days those uploads can touch, check, then publish the report.

The DAG decides only *when* and *in what order*. What each step does lives in the lab (scripts/part19_pipeline.py,
sql/part19/checks.sql, the dbt project), so the same commands run by hand, in CI and here.
"""

import json
import subprocess
from datetime import timedelta

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

LAB = "cd /opt/lab && /opt/lab-venv/bin/python -m scripts.part19_pipeline"

# Assets name the tables, not the tasks: anything that needs the published report waits for this, whoever makes it.
RAW_POS = Asset(uri="iceberg://larkspur/raw/pos_uploads", name="raw.pos_uploads")
SALES_FACTS = Asset(uri="iceberg://larkspur/gold/fct_sales_line", name="gold.fct_sales_line")
REPORT = Asset(uri="iceberg://larkspur/gold/rpt_daily_net_revenue_v2", name="gold.rpt_daily_net_revenue_v2")

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
        outlets=[RAW_POS],
    )

    # Which models and which microbatch days a run for D rebuilds is the lab's decision (scripts/part19_pipeline.py).
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{LAB} build --day {{{{ ds }}}}",
        pool="lakehouse",
        outlets=[SALES_FACTS],
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=f"{LAB} check --day {{{{ ds }}}}",
        retries=0,  # a broken rule is not transient: retrying it only delays the page
    )

    @task(pool="lakehouse", outlets=[REPORT])
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

    wait_for_uploads >> ingest >> dbt_build >> quality_checks >> publish()
