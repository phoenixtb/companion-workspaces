"""Part 12: read what a Spark SQL query did from the Spark UI's REST API (/api/v1), the numbers the UI shows.

Used by the other part12_*.py jobs (same folder, so a plain import works under spark-submit). For the last SQL
execution it returns the final physical plan and, per stage that ran, the task count, median and max task time,
shuffle read and write, the largest shuffle read of any one task, and the bytes spilled.
"""

import json
import statistics
import urllib.request

from pyspark.sql import SparkSession


def get(spark: SparkSession, path: str):
    sc = spark.sparkContext
    return json.load(urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"))


def last_execution(spark: SparkSession) -> dict:
    return get(spark, "sql?details=true&planDescription=true&length=10000")[-1]


def stage_report(spark: SparkSession, stage_id: int) -> dict:
    """One stage's totals plus the spread of its tasks (from the task list, not the sampled summary)."""
    stage = get(spark, f"stages/{stage_id}")[-1]  # the last attempt
    tasks = get(spark, f"stages/{stage_id}/{stage['attemptId']}/taskList?length=100000")
    done = [t for t in tasks if t.get("status") == "SUCCESS"]
    durations = [t["taskMetrics"]["executorRunTime"] / 1000 for t in done]
    reads = [t["taskMetrics"]["shuffleReadMetrics"] for t in done]
    read_bytes = [r["remoteBytesRead"] + r["localBytesRead"] for r in reads]
    read_records = [r["recordsRead"] for r in reads]
    return {
        "stage": stage_id,
        "tasks": len(done),
        "median_s": statistics.median(durations) if durations else 0.0,
        "max_s": max(durations, default=0.0),
        "shuffle_read_mib": stage["shuffleReadBytes"] / 1048576,
        "max_task_read_mib": max(read_bytes, default=0) / 1048576,
        "max_task_read_rows": max(read_records, default=0),
        "shuffle_write_mib": stage["shuffleWriteBytes"] / 1048576,
        "spill_mem_mib": stage["memoryBytesSpilled"] / 1048576,
        "spill_disk_mib": stage["diskBytesSpilled"] / 1048576,
        "name": stage["name"],
    }


def execution_report(spark: SparkSession) -> tuple[dict, list[dict]]:
    """The last SQL execution: its record and a report per stage that ran (skipped stages have no tasks)."""
    ex = last_execution(spark)
    stage_ids: list[int] = []
    for job_id in ex.get("successJobIds", []) + ex.get("failedJobIds", []):
        stage_ids += get(spark, f"jobs/{job_id}")["stageIds"]
    reports = []
    for sid in sorted(set(stage_ids)):
        try:
            r = stage_report(spark, sid)
        except Exception:  # noqa: BLE001 - a stage AQE skipped has no attempt to report
            continue
        if r["tasks"]:
            reports.append(r)
    return ex, reports


HEADER = (
    f"{'stage':>5} {'tasks':>5} {'median_s':>8} {'max_s':>6} {'shuf_read_MiB':>13} {'max_task_MiB':>12} "
    f"{'max_task_rows':>13} {'shuf_write_MiB':>14} {'spill_mem_MiB':>13} {'spill_disk_MiB':>14}"
)


def print_stages(reports: list[dict]) -> None:
    print(HEADER)
    for r in reports:
        print(
            f"{r['stage']:>5} {r['tasks']:>5} {r['median_s']:>8.2f} {r['max_s']:>6.2f} {r['shuffle_read_mib']:>13.1f} "
            f"{r['max_task_read_mib']:>12.1f} {r['max_task_read_rows']:>13,} {r['shuffle_write_mib']:>14.1f} "
            f"{r['spill_mem_mib']:>13.1f} {r['spill_disk_mib']:>14.1f}"
        )


def plan_lines(ex: dict, words: tuple[str, ...]) -> list[str]:
    """Lines of the final physical plan's tree that mention one of `words`, each AQE shuffle read with its arguments
    (coalesced, skewed, local) appended."""
    text = ex.get("planDescription", "")
    tree = text.split("\n\n")[0].split("+- == Initial Plan ==")[0]
    reads = {}
    for block in text.split("\n\n")[1:]:
        head, *rest = block.strip().splitlines() or [""]
        if "AQEShuffleRead" in head:
            args = next((r.split(": ", 1)[1] for r in rest if r.startswith("Arguments:")), "")
            reads[head.split(")")[0] + ")"] = args
    out = []
    for line in tree.splitlines():
        if any(w in line for w in words):
            node = line[line.index("AQEShuffleRead") :].strip() if "AQEShuffleRead" in line else ""
            num = "(" + node.split("(")[-1] if node else ""
            out.append(line + (f"  [{reads[num]}]" if reads.get(num) else ""))
    return out
