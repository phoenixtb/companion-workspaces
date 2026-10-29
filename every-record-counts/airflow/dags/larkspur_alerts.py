"""Part 19: the deadline callback. A real one would page or post to a channel; the lab writes a log line."""

import logging

log = logging.getLogger(__name__)


def revenue_late(report: str, context: dict | None = None, **_) -> None:
    """Airflow passes `context` = {"dag_run": <the run, as the REST API shows it>, "deadline": {...}}."""
    run = (context or {}).get("dag_run") or {}
    if run.get("run_type") == "backfill":  # a backfilled day was always going to be "late": history, not an incident
        log.info("deadline passed for backfill run %s: not paging", run.get("dag_run_id"))
        return
    log.warning("DEADLINE MISSED: %s not published in time (run %s)", report, run.get("dag_run_id"))


def reconciliation_failed(context: dict) -> None:
    """Part 21: the reconcile task's failure callback. The failing checks are in the task log and in
    gold.daily_controls (status = 'FAIL'); a real one would page the data platform's on-call with them."""
    ti = context.get("task_instance")
    log.warning("RECONCILIATION FAILED: %s for upload day %s; publish withheld. See gold.daily_controls",
                getattr(ti, "dag_id", "?"), context.get("ds"))  # fmt: skip
