"""Part 20: Write-Audit-Publish on an Iceberg branch, driven from the host, watched by a reader of main.

uv run python -m scripts.part20_wap init --until 2026-09-29       # gold.fct_pos_line, upload days to D (Spark)
uv run python -m scripts.part20_wap publish --day D [--bug skip-dedup]   # write to `audit`, check, fast-forward main
uv run python -m scripts.part20_wap refs                          # main and audit: snapshot, lines, duplicate lines
uv run python -m scripts.part20_wap trino                         # Trino 484 and the branch (make up-trino)

While `publish` runs, a reader polls main as the analyst (PyIceberg, vended credentials), the way a dashboard would,
and records every snapshot it saw; afterwards each one is scanned for duplicate lines. Needs `make up-spark`.
"""

import argparse
import json
import subprocess
import sys
import threading
import time

import duckdb

from scripts import part09_catalog as cat9
from scripts.part11_single_node import container_peak
from scripts.part16_cdc import ROOT, SPARK_PIPELINE

TABLE = "gold.fct_pos_line"


def submit(args: list[str]) -> int:
    """spark-submit as the pipeline principal (Part 16's catalog settings); print the job's stdout, not Spark's logs."""
    stop, peaks = threading.Event(), [0.0]
    sampler = threading.Thread(target=container_peak, args=("every-record-counts-spark", stop, peaks), daemon=True)
    sampler.start()
    cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-submit", "--master", "local[*,3]"]
    for k, v in SPARK_PIPELINE.items():
        cmd += ["--conf", f"{k}={v}"]
    run = subprocess.run([*cmd, "/opt/lakehouse/jobs/part20_wap.py", *args], cwd=ROOT, capture_output=True, text=True)
    stop.set()
    sampler.join()
    print(run.stdout, end="")
    if run.returncode and "publish: refused" not in run.stdout:  # a Spark error, not an audit that said no
        print("\n".join(x for x in run.stderr.splitlines() if "Error" in x or "Exception" in x)[:3000], file=sys.stderr)
    print(f"spark-submit exit {run.returncode}, Spark container peak {max(peaks):,.0f} MiB")
    return run.returncode


def lines_in(snapshot_id: int | None) -> dict:
    """Lines and duplicate (transaction_id, line_no) pairs in one snapshot of the table, read as the analyst."""
    t = cat9.pyiceberg("analyst").load_table(TABLE)
    con = duckdb.connect()
    con.register("f", t.scan(snapshot_id=snapshot_id, selected_fields=("transaction_id", "line_no")).to_arrow())
    n, dupes = con.execute("SELECT count(*), count(*) - count(DISTINCT (transaction_id, line_no)) FROM f").fetchone()
    return {"snapshot": snapshot_id, "lines": n, "duplicate_lines": dupes}


def refs() -> dict:
    t = cat9.pyiceberg("analyst").load_table(TABLE)
    out = {name: lines_in(r.snapshot_id) for name, r in sorted(t.metadata.refs.items())}
    for name, v in out.items():
        print(f"{name:<6} {json.dumps(v)}")
    return out


def watch(stop: threading.Event, seen: list[tuple[float, int]]) -> None:
    """Load the table every half second, as a reader of main, and note main's snapshot each time."""
    cat = cat9.pyiceberg("analyst")
    started = time.monotonic()
    while not stop.is_set():
        seen.append((time.monotonic() - started, cat.load_table(TABLE).metadata.current_snapshot_id))
        time.sleep(0.5)


def publish(day: str, bug: str | None) -> dict:
    stop, seen = threading.Event(), []
    reader = threading.Thread(target=watch, args=(stop, seen), daemon=True)
    reader.start()
    rc = submit(["--day", day] + (["--bug", bug] if bug else []))
    stop.set()
    reader.join()
    snapshots = list(dict.fromkeys(s for _, s in seen))  # in the order the reader first saw them
    print(f"reader of main: {len(seen)} loads, saw snapshots {snapshots}")
    for s in snapshots:
        print(f"  main snapshot {s}: {json.dumps(lines_in(s))}")
    return {"exit": rc, "main_seen": snapshots}


T = "lakehouse_pipeline.gold.fct_pos_line"
TRINO_BRANCH = [  # reading a branch works; creating one and writing to one don't (Iceberg connector, Trino 484)
    'SELECT name, type, snapshot_id FROM lakehouse_pipeline.gold."fct_pos_line$refs"',
    f"SELECT count(*) FROM {T} FOR VERSION AS OF 'audit'",
    f"CREATE BRANCH audit2 IN TABLE {T}",
    f"INSERT INTO {T} @ audit SELECT * FROM {T} LIMIT 0",
]


def trino() -> None:
    for sql in TRINO_BRANCH:
        rows, err = cat9.trino(sql)
        print(f"{sql}\n  -> {err or rows}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["init", "publish", "refs", "trino"])
    p.add_argument("--until")
    p.add_argument("--day")
    p.add_argument("--bug", choices=["skip-dedup"])
    a = p.parse_args()
    if a.command == "init":
        raise SystemExit(submit(["--init", "--until", a.until]))
    if a.command == "publish":
        raise SystemExit(publish(a.day, a.bug)["exit"])
    trino() if a.command == "trino" else refs()


if __name__ == "__main__":
    main()
