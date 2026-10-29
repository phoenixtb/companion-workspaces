"""Part 19: table maintenance, scheduled on data instead of the clock. Every time the daily pipeline publishes the
revenue report, compact the tables it churned, expire old snapshots and remove orphan files (Part 10's jobs, run with
Trino's Iceberg table procedures). The `lakehouse` pool (one slot) keeps it from committing to a table while the
daily run is writing the same one.
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

LAB = "cd /opt/lab && /opt/lab-venv/bin/python -m scripts.part19_pipeline"
REPORT = Asset(uri="iceberg://larkspur/gold/rpt_daily_net_revenue_v2", name="gold.rpt_daily_net_revenue_v2")

with DAG(
    dag_id="larkspur_lake_maintenance",
    schedule=[REPORT],  # one run per batch of publish events that arrived since the last run
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["larkspur", "part19"],
) as dag:
    BashOperator(
        task_id="maintain_tables",
        # Which tables, in which order (expire, then orphans), is the lab's decision: `maintain` with no --table.
        # Retention from an Airflow Variable: production 7d (Trino's minimum); the lab sets 1h (its history is hours
        # old), which is safe only because the `lakehouse` pool serialises every write the DAGs make.
        bash_command=f"{LAB} maintain --retention {{{{ var.value.get('lake_retention', '7d') }}}}",
        pool="lakehouse",
    )
