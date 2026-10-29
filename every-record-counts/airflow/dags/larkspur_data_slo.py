"""Part 22: the daily revenue report's service level objectives, checked at 06:00 London every morning: was yesterday's
upload day published, did every control pass, is the volume plausible, does the published table match its contract.
A check that fails pages; gold.slo_results keeps every morning's measurements, which is where the error budget comes
from. What is measured, and how, lives in scripts/part22_observe.py and sql/part22/slo.sql, not here.
"""

import larkspur_alerts
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronTriggerTimetable
from openlineage.client.event_v2 import Dataset

LAB22 = "cd /opt/lab && /opt/lab-venv/bin/python -m scripts.part22_observe"

with DAG(
    dag_id="larkspur_data_slo",
    # A trigger timetable: the run *is* the 06:00 check, and its logical date is that moment in London.
    schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/London"),
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    tags=["larkspur", "part22"],
) as dag:
    BashOperator(
        task_id="evaluate_slos",
        bash_command=f"{LAB22} slo --at {{{{ logical_date.isoformat() }}}}",
        retries=0,  # a breach is a fact about the morning, not a transient error
        pool="lakehouse",  # it writes gold.slo_results: one lake writer at a time, like every other task
        on_failure_callback=larkspur_alerts.slo_breached,
        inlets=[Dataset(namespace="trino://trino:8080", name="lakehouse_pipeline.gold.daily_controls")],
        outlets=[Dataset(namespace="trino://trino:8080", name="lakehouse_pipeline.gold.slo_results")],
    )
