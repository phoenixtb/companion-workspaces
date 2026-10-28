"""Part 10: partitioning, file layout and table maintenance, on four years of POS lines.

uv run python scripts/duck.py --local sql/part07/stage.sql            # Part 7's staged POS lines, if not there yet
uv run python -m scripts.part10_layout reset                          # drop the part10 tables and their files
uv run python -m scripts.part10_layout spark 01_build.sql [...]       # run sql/part10/spark/*.sql in Spark
uv run python -m scripts.part10_layout measure pos_flat [...] [--queries revenue,store_promo]
uv run python -m scripts.part10_layout microbatch                     # 4 October as 41 hourly appends + a delete
uv run python -m scripts.part10_layout objects                        # pos_monthly's objects in SeaweedFS, by kind
uv run python -m scripts.part10_layout crash                          # leave an uncommitted data file behind
uv run python -m scripts.part10_layout orphans                        # remove it with Iceberg's Spark action

Needs `make up-spark` (core + Spark). The Spark jobs are in spark/jobs/part10_*.py.
"""

import argparse
import subprocess
import sys
from pathlib import Path

from scripts import part04_star as star
from scripts import part07_iceberg as p7

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "larkspur-lake/warehouse/part10"


def reset() -> None:
    cat = p7.catalog()
    if ("part10",) not in cat.list_namespaces():
        cat.create_namespace("part10")
    for ident in cat.list_tables("part10"):
        cat.drop_table(ident)
    # Dropping a table forgets it; the files stay (Part 1's Gotcha). Remove them too.
    fs = star.fs()
    if fs.exists(PREFIX):
        fs.rm(PREFIX, recursive=True)
    print("part10: no tables, no files")


# Local mode gives a task no second attempt by default; `local[*,3]` allows three. Repeated scans of pos_flat
# occasionally failed with "Socket is closed by peer" from SeaweedFS (Troubleshooting in the post), and a retry
# of the one task is what a cluster would do anyway.
MASTER = "local[*,3]"


def submit(args: list[str], capture: bool = False) -> tuple[int, str]:
    """spark-submit inside the Spark container; return the exit code and, if captured, the output."""
    sys.stdout.flush()
    cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-submit", "--master", MASTER, *args]
    run = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE, text=True)
    if run.returncode:
        print(run.stderr[-3000:], file=sys.stderr)
    return run.returncode, run.stdout or ""


def spark(files: list[str], strict: bool = False, capture: bool = False) -> tuple[int, str]:
    """Run sql/part10/spark/<file> statement by statement (spark/jobs/run_sql.py)."""
    args = ["/opt/lakehouse/jobs/run_sql.py", *(["--strict"] if strict else [])]
    return submit(args + [f"/opt/lakehouse/sql/part10/spark/{f}" for f in files], capture)


def job(name: str, args: list[str] | None = None, capture: bool = False) -> tuple[int, str]:
    """Run spark/jobs/part10_<name>.py."""
    return submit([f"/opt/lakehouse/jobs/part10_{name}.py", *(args or [])], capture)


def objects(table: str = "pos_monthly") -> dict[str, int]:
    """Count the table's objects in SeaweedFS by kind: what is really in the bucket, referenced or not."""
    kinds: dict[str, list[int]] = {}
    for path, info in star.fs().find(f"{PREFIX}/{table}", detail=True).items():
        name = path.rsplit("/", 1)[-1]
        if name.endswith("-deletes.parquet"):
            kind = "delete files (Parquet)"
        elif name.endswith(".parquet"):
            kind = "data files (Parquet)"
        elif name.startswith("snap-"):
            kind = "manifest lists (Avro)"
        elif name.endswith(".avro"):
            kind = "manifests (Avro)"
        else:
            kind = "metadata files (JSON)"
        kinds.setdefault(kind, []).append(info["size"])
    print(f"s3://{PREFIX}/{table}/")
    for kind in sorted(kinds):
        sizes = kinds[kind]
        print(f"  {kind:<24} {len(sizes):>5,} objects {sum(sizes) / 1048576:>9.1f} MiB")
    return {kind: len(sizes) for kind, sizes in kinds.items()}


STRAY = "00000-0-crashed-writer-00001.parquet"


def crash(table: str = "pos_monthly") -> str:
    """Leave what a writer that died before its commit leaves: a data file no metadata references."""
    fs = star.fs()
    data = sorted(p for p in fs.find(f"{PREFIX}/{table}/data") if p.endswith(".parquet"))
    stray = data[-1].rsplit("/", 1)[0] + "/" + STRAY
    fs.copy(data[-1], stray)
    print(f"uploaded, never committed: s3://{stray} ({fs.size(stray):,} bytes)")
    return stray


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["reset", "spark", "measure", "microbatch", "objects", "crash", "orphans"])
    p.add_argument("args", nargs="*")
    p.add_argument("--queries", help="comma-separated names from sql/part10/queries (measure only)")
    a = p.parse_args()
    if a.command == "spark":
        sys.exit(spark(a.args)[0])
    if a.command == "measure":
        sys.exit(job("measure", a.args + (["--queries", a.queries] if a.queries else []))[0])
    if a.command in ("microbatch", "orphans"):
        sys.exit(job({"microbatch": "microbatches", "orphans": "orphans"}[a.command])[0])
    {"reset": reset, "objects": objects, "crash": crash}[a.command]()
