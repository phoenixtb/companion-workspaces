"""Part 12: distributed batch with Spark 4.1 — a PySpark silver/gold job, then a hot store, shuffles, spill and skew.

uv run python -m scripts.part12_spark stage                 # copy the store reference file where Spark can read it
uv run python -m scripts.part12_spark reset                 # drop Part 12's tables (silver, gold, part12) and files
uv run python -m scripts.part12_spark pipeline [--months 2026-07,2026-08,2026-09]
uv run python -m scripts.part12_spark ansi                  # sql/part12/ansi.sql: ANSI mode on and off
uv run python -m scripts.part12_spark build                 # part12.campaign_lines: a year with one store at ~30%
uv run python -m scripts.part12_spark skew [broadcast merge aqe_default aqe_scaled salted]

Needs `make up-spark` and Part 10's tables (lakehouse.part10.pos_monthly). The Spark jobs are spark/jobs/part12_*.py;
every submit samples `docker stats` and prints the Spark container's peak memory.
"""

import argparse
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from scripts import part04_star as star
from scripts import part07_iceberg as p7
from scripts.part11_single_node import container_peak

ROOT = Path(__file__).resolve().parents[1]
STORES = ROOT / "data/raw/reference/stores/stores.csv"
STAGED = ROOT / "data/lab/part12/stores.csv"
TABLES = ["silver.dim_store", "silver.sales_line", "gold.daily_sales_by_format", "part12.campaign_lines"]
VARIANTS = ["broadcast", "merge", "aqe_default", "aqe_scaled", "salted"]
# Local mode gives a task one attempt; `local[*,3]` allows three, as a cluster would (Part 10: SeaweedFS reads now
# and then fail with "Socket is closed by peer" or an early end of stream).
MASTER = "local[*,3]"


def stage() -> None:
    STAGED.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(STORES, STAGED)
    print(f"staged {STAGED.relative_to(ROOT)} ({sum(1 for _ in STAGED.open()) - 1} stores)")


def reset() -> None:
    cat = p7.catalog()
    namespaces = {ns[0] for ns in cat.list_namespaces()}
    fs = star.fs()
    for ident in TABLES:
        ns, name = ident.split(".")
        if ns in namespaces and cat.table_exists(ident):
            cat.drop_table(ident)
        prefix = f"larkspur-lake/warehouse/{ns}/{name}"
        if fs.exists(prefix):  # a drop forgets the table; the files stay (Part 1)
            fs.rm(prefix, recursive=True)
    print("part12: no silver.dim_store, silver.sales_line, gold.daily_sales_by_format or part12.campaign_lines")


def submit(job: str, args: list[str], capture: bool = False) -> tuple[int, str, float]:
    """spark-submit spark/jobs/<job> in the Spark container; return exit code, output and the container's peak MiB."""
    if not STAGED.exists():
        stage()
    stop, peaks = threading.Event(), [0.0]
    sampler = threading.Thread(target=container_peak, args=("every-record-counts-spark", stop, peaks), daemon=True)
    sampler.start()
    cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-submit", "--master", MASTER]
    cmd += [f"/opt/lakehouse/jobs/{job}", *args]
    sys.stdout.flush()
    started = time.monotonic()
    run = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE, text=True)
    stop.set()
    sampler.join()
    if run.returncode:
        print(run.stderr[-3000:], file=sys.stderr)
    took = time.monotonic() - started
    note = f"spark-submit {job} {' '.join(args)}: {took:.0f} s, Spark container peak {max(peaks):,.0f} MiB"
    if capture:
        return run.returncode, (run.stdout or "") + note + "\n", max(peaks)
    print(note)
    return run.returncode, "", max(peaks)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["stage", "reset", "pipeline", "ansi", "build", "hot", "skew"])
    p.add_argument("args", nargs="*")
    p.add_argument("--months", help="comma-separated yyyy-MM (pipeline only)")
    a = p.parse_args()
    if a.command in ("stage", "reset"):
        {"stage": stage, "reset": reset}[a.command]()
    elif a.command == "pipeline":
        sys.exit(submit("part12_pipeline.py", ["--months", a.months] if a.months else [])[0])
    elif a.command == "ansi":
        sys.exit(submit("run_sql.py", ["/opt/lakehouse/sql/part12/ansi.sql"])[0])
    elif a.command in ("build", "hot"):
        sys.exit(submit("part12_skew.py", [a.command, *a.args])[0])
    else:
        sys.exit(submit("part12_skew.py", ["run", *(a.args or VARIANTS)])[0])
