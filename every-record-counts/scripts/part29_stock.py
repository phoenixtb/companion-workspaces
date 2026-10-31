"""Part 29: a stateful Flink job in Java. A stock-out detector, a crash, an upgrade, and the batch answer to check it.

python -m scripts.part29_stock setup      # (make up-flink-lake) opening stock, empty tables and alert topics
python -m scripts.part29_stock build      # the job jar, versions 1 and 2 (Gradle, Java 17)
python -m scripts.part29_stock batch      # the same rule over the raw zone, in DuckDB + Python: the expected answer
python -m scripts.part29_stock run --kill-at 0.4      # replay with a TaskManager killed mid-run, then check
python -m scripts.part29_stock checkpoints [--unaligned]  # the throttled replay: checkpoint sizes and durations
python -m scripts.part29_stock check      # the lake and both alert topics against the batch answer
python -m scripts.part29_stock upgrade --stop-at 0.5  # savepoint, version 2, restore at parallelism 2, then check
python -m scripts.part29_stock nouids --stop-at 0.5   # the same upgrade from a job that set no operator IDs
python -m scripts.part29_stock nouids --same-topology # no uids on either side, same operators: does it restore?
python -m scripts.part29_stock fast [--fetch-bytes N] [--align]  # an unthrottled replay: which lines arrive late?
"""

import argparse
import gzip
import json
import re
import subprocess
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from scripts import part04_star as star
from scripts import part27_backbone as p27
from scripts import part28_flink as p28

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = p27.COMPOSE
FLINK = "http://localhost:8081"
DAY = "2026-08-15"  # the business day the detector covers
UPLOAD_DAY = p27.POS_DAY  # what Part 27 replayed onto the POS topic
OPENING = ROOT / f"data/lab/part29/opening-{DAY}.csv.gz"
STATE = ROOT / "data/lab/flink-state"
CAPTURE = ROOT / "data/lab/part29/last-run.json"
CONFIRM = timedelta(minutes=30)  # the job's --confirm-minutes default
THROTTLE_MS = 2  # per line: stretches the replay to about two minutes
ALERTS, LATE = "stream.stock_alerts", "stream.stock_late_sales"
TOPIC_EOS, TOPIC_ALO = "larkspur.alerts.stock", "larkspur.alerts.stock-at-least-once"
JOB_CLASS = "com.larkspur.stream.StockOutJob"


# ---------- helpers ----------


def rest(path: str, method: str = "GET", body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{FLINK}{path}", data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}


def flink_cli(*args: str, check: bool = True) -> str:
    cmd = [*COMPOSE, "exec", "-T", "flink-jobmanager", "./bin/flink", *args]
    done = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    out = done.stdout + done.stderr
    if check and done.returncode:
        raise SystemExit(out[-3000:])
    return out


def raw_con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute(f"SET VARIABLE raw_root = '{ROOT / 'data/raw'}'")
    c.execute(f"SET VARIABLE day = '{DAY}'")
    c.execute(f"SET VARIABLE upload_day = '{UPLOAD_DAY}'")
    c.execute((ROOT / "sql/raw/views.sql").read_text())
    for f in ("opening_stock.sql", "sale_lines.sql"):
        for stmt in c.extract_statements((ROOT / "sql/part29" / f).read_text()):
            if stmt.query.lstrip().upper().startswith("CREATE"):
                c.execute(stmt.query)
    return c


def stamp(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%H:%M:%S")


def now() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


# ---------- setup ----------

ALERT_SCHEMA = (
    ("store_id", "string"),
    ("sku", "string"),
    ("kind", "string"),
    ("event_ts", "timestamptz"),
    ("detected_ts", "timestamptz"),
    ("level", "int"),
    ("opening", "int"),
    ("job_version", "int"),
)
LATE_SCHEMA = (
    ("store_id", "string"),
    ("sku", "string"),
    ("basket_id", "string"),
    ("qty", "int"),
    ("event_ts", "timestamptz"),
    ("uploaded_at", "timestamptz"),
    ("watermark_ts", "timestamptz"),
)


def iceberg_schema(cols: tuple):
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, NestedField, StringType, TimestamptzType

    types = {"string": StringType(), "int": IntegerType(), "timestamptz": TimestamptzType()}
    return Schema(*(NestedField(i + 1, n, types[t], required=False) for i, (n, t) in enumerate(cols)))


def reset_tables() -> None:
    """Both tables dropped (files too) and created empty, as root: the job principal may write, not drop."""
    cat = p27.catalog()
    fs = star.fs()
    for ident, cols in ((ALERTS, ALERT_SCHEMA), (LATE, LATE_SCHEMA)):
        if cat.table_exists(ident):
            folder = cat.load_table(ident).location().removeprefix("s3://").rstrip("/")
            cat.drop_table(ident)
            fs.invalidate_cache()
            if folder.startswith("larkspur-lake/warehouse/stream/") and fs.exists(folder):
                fs.rm(folder, recursive=True)
        cat.create_table(ident, iceberg_schema(cols), properties={"format-version": "2"})


def reset_topics() -> None:
    p27.kafka("--delete", "--if-exists", "--topic", f"{TOPIC_EOS},{TOPIC_ALO}")
    p27._wait_deleted([TOPIC_EOS, TOPIC_ALO])
    for t in (TOPIC_EOS, TOPIC_ALO):
        # The sink stamps records with their event time (August); with CreateTime, 7-day retention would delete
        # them at once. LogAppendTime makes a record's Kafka timestamp the moment the broker wrote it.
        p27.kafka(
            "--create",
            "--topic",
            t,
            "--partitions",
            "1",
            "--config",
            f"retention.ms={7 * p27.DAY_MS}",
            "--config",
            "message.timestamp.type=LogAppendTime",
        )


def cancel_jobs() -> None:
    for job in rest("/jobs")["jobs"]:
        if job["status"] in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING"):
            rest(f"/jobs/{job['id']}", method="PATCH")


def write_opening() -> int:
    c = raw_con()
    OPENING.parent.mkdir(parents=True, exist_ok=True)
    rows = c.execute("SELECT store_id, sku, opening_qty FROM opening_stock ORDER BY store_id, sku").fetchall()
    with gzip.open(OPENING, "wt") as f:
        f.write("store_id,sku,opening_qty\n")
        f.writelines(f"{s},{k},{q}\n" for s, k, q in rows)
    return len(rows)


def fresh_taskmanager() -> None:
    """A new TaskManager process. In a session cluster every submission loads the job jar in a new classloader, and
    the registry client's threads likely keep old ones alive (no heap dump taken): after about a dozen runs the
    TaskManager died with OutOfMemoryError: Metaspace. One job per cluster (application mode) avoids it; the lab
    recreates the container, which also drops the RocksDB and blob-cache folders a killed TaskManager leaves in /tmp."""
    cmd = [*COMPOSE, "up", "-d", "--no-deps", "--force-recreate", "flink-taskmanager"]
    subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True)
    started = time.monotonic()
    while time.monotonic() - started < 120:
        if rest("/overview").get("slots-available", 0) >= 2:
            return
        time.sleep(1)
    raise SystemExit("no TaskManager registered within 120 s")


def reset(clear_state: bool = True) -> None:
    cancel_jobs()
    fresh_taskmanager()
    reset_tables()
    reset_topics()
    if clear_state:
        subprocess.run(["rm", "-rf", str(STATE / "checkpoints"), str(STATE / "savepoints")], check=True)


def cmd_setup(_: argparse.Namespace) -> None:
    p28.setup_principal()  # Part 28's job principal `flink`: create, read and write tables in `stream`
    n = write_opening()
    reset()
    print(f"opening stock for {DAY}: {n:,} store-SKUs -> {OPENING.relative_to(ROOT)}")
    print(f"tables {ALERTS}, {LATE} empty; topics {TOPIC_EOS}, {TOPIC_ALO} empty")


def build(version: int) -> None:
    subprocess.run(["./gradlew", "-q", ":flink-jobs:shadowJar", f"-PjobVersion={version}"], cwd=ROOT, check=True)


def cmd_build(_: argparse.Namespace) -> None:
    for v in (1, 2):
        build(v)
        jar = ROOT / f"flink-jobs/build/libs/flink-jobs-v{v}.jar"
        print(f"{jar.relative_to(ROOT)}: {jar.stat().st_size / 2**20:.1f} MiB")


# ---------- the batch answer ----------


def expected() -> dict:
    """The detector's rule over the raw zone, line by line in event-time order: what a correct stream must write."""
    c = raw_con()
    opening = dict(
        ((s, k), q) for s, k, q in c.execute("SELECT store_id, sku, opening_qty FROM opening_stock").fetchall()
    )
    sales = defaultdict(lambda: defaultdict(int))  # (store, sku) -> event ms -> units
    rows = c.execute(
        f"""SELECT store_id, sku, epoch_ms(event_ts), qty FROM sale_lines
            WHERE business_date = DATE '{DAY}' AND NOT late"""
    ).fetchall()
    for store, sku, ts, qty in rows:
        sales[(store, sku)][ts] += qty
    confirm = int(CONFIRM.total_seconds() * 1000)
    alerts = []
    for key, by_ts in sales.items():
        level = opening.get(key, 0)
        out_since, alerted, book_error = 0, False, False
        for ts in sorted(by_ts):
            if out_since and not alerted and out_since + confirm < ts:  # the deadline passed before this sale
                alerted = True
                alerts.append((*key, "STOCK_OUT", out_since, out_since + confirm))
            before = level
            level -= by_ts[ts]
            if before <= 0:
                if not alerted:
                    out_since = 0
                if not book_error:
                    book_error = True
                    alerts.append((*key, "BOOK_ERROR", ts, ts))
            elif level <= 0:
                out_since = ts
        if out_since and not alerted:  # end of input: the watermark reaches the end of time, every timer fires
            alerts.append((*key, "STOCK_OUT", out_since, out_since + confirm))
    late = c.execute(
        f"SELECT store_id, sku, basket_id, qty FROM sale_lines WHERE business_date = DATE '{DAY}' AND late"
    ).fetchall()
    return {"alerts": alerts, "late": late, "keys": len(sales), "lines": len(rows)}


def cmd_batch(_: argparse.Namespace) -> None:
    e = expected()
    kinds = Counter(a[2] for a in e["alerts"])
    print(f"on-time lines of {DAY}: {e['lines']:,} over {e['keys']:,} store-SKUs")
    print(
        f"expected alerts: {len(e['alerts']):,} = STOCK_OUT {kinds['STOCK_OUT']:,} + BOOK_ERROR {kinds['BOOK_ERROR']:,}"
    )
    print(f"expected late lines (side output): {len(e['late']):,}")
    # Where the book errors come from: the WMS file and the tills are generated independently in this lab.
    opening = dict(((st, k), q) for st, k, q in raw_con().execute("SELECT * FROM opening_stock").fetchall())
    outs = {(x[0], x[1]) for x in e["alerts"] if x[2] == "STOCK_OUT"}
    book = [x for x in e["alerts"] if x[2] == "BOOK_ERROR"]
    at_zero = sum(1 for x in book if opening.get((x[0], x[1]), 0) <= 0)
    after_out = sum(1 for x in book if (x[0], x[1]) in outs)
    print(
        f"BOOK_ERROR: {at_zero:,} on store-SKUs that opened at zero or below "
        f"({sum(1 for q in opening.values() if q <= 0):,} did), {after_out:,} after a STOCK_OUT, "
        f"{len(book) - at_zero - after_out:,} within the confirm window"
    )


# ---------- running the job ----------


def submit(
    version: int,
    *job_args: str,
    savepoint: str | None = None,
    parallelism: int | None = None,
    allow_non_restored: bool = False,
) -> str:
    args = ["run", "-d"]
    if savepoint:
        args += ["-s", savepoint]
    if allow_non_restored:
        args += ["--allowNonRestoredState"]
    if parallelism:
        args += ["-p", str(parallelism)]
    args += ["-c", JOB_CLASS, f"/opt/flink/lab/jobs/flink-jobs-v{version}.jar", *job_args]
    out = flink_cli(*args, check=False)
    found = re.search(r"JobID ([0-9a-f]{32})", out)
    if not found:
        raise SystemExit(out[-4000:])
    return found.group(1)


def vertices(job_id: str) -> list[dict]:
    return rest(f"/jobs/{job_id}")["vertices"]


def source_records(job_id: str) -> int:
    v = vertices(job_id)[0]
    return int(v["metrics"].get("write-records", 0))


def source_records_per_vertex(job_id: str) -> list[int]:
    return [int(v["metrics"].get("write-records", 0)) for v in vertices(job_id) if v["name"].startswith("Source")]


def total_lines() -> int:
    c = raw_con()
    return c.execute(
        f"""SELECT sum(len(lines)) FROM raw_pos_events WHERE upload_date = DATE '{UPLOAD_DAY}'"""
    ).fetchone()[0]


def wait_until(job_id: str, fraction: float, total: int, timeout: int = 900) -> None:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        job = rest(f"/jobs/{job_id}")
        if job["state"] == "RUNNING" and source_records(job_id) >= fraction * total:
            return
        if job["state"] in ("FINISHED", "FAILED", "CANCELED"):
            raise SystemExit(f"job {job_id} {job['state']} before {fraction:.0%}")
        time.sleep(0.5)
    raise SystemExit(f"job {job_id} did not reach {fraction:.0%}")


def wait_job(job_id: str, timeout: int = 900) -> dict:
    return p28.wait_job(job_id, timeout)


def checkpoint_counts(job_id: str) -> dict:
    return rest(f"/jobs/{job_id}/checkpoints")["counts"]


def backpressure(job_id: str) -> list[dict]:
    """Per task: the share of each second it spent back-pressured, busy or idle (Flink's own task metrics)."""
    out = []
    for v in vertices(job_id):
        m = rest(
            f"/jobs/{job_id}/vertices/{v['id']}/subtasks/metrics"
            "?get=backPressuredTimeMsPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond&agg=max"
        )
        vals = {x["id"]: x.get("max") for x in m}
        out.append(
            {
                "task": v["name"],
                "parallelism": v["parallelism"],
                "backpressured": vals.get("backPressuredTimeMsPerSecond"),
                "busy": vals.get("busyTimeMsPerSecond"),
                "idle": vals.get("idleTimeMsPerSecond"),
            }
        )
    return out


def print_backpressure(rows: list[dict]) -> None:
    print(f"{'task (ms per second, max over subtasks)':58} {'back-pressured':>14} {'busy':>6} {'idle':>6}")
    for r in rows:
        name = r["task"] if len(r["task"]) <= 58 else r["task"][:55] + "..."
        print(f"{name:58} {r['backpressured'] or 0:>14.0f} {r['busy'] or 0:>6.0f} {r['idle'] or 0:>6.0f}")


def kill_taskmanager(job_id: str, restart_after: float = 5.0) -> tuple[list[str], dict]:
    """SIGKILL the only TaskManager; start a fresh one a few seconds later, as Kubernetes would replace the pod; log
    what the job does, in seconds after the kill, until every task runs again from a restored checkpoint."""
    # Kill late in a checkpoint interval (7 s after the last completed checkpoint, with 10 s between checkpoints):
    # everything emitted since that checkpoint is what a restore replays.
    while True:
        before = rest(f"/jobs/{job_id}/checkpoints")
        last = before.get("latest", {}).get("completed") or {}
        if last and time.time() * 1000 - last["latest_ack_timestamp"] >= 7000:
            break
        time.sleep(0.2)
    folder = STATE / "checkpoints" / job_id
    tree: dict[str, list[int]] = {}  # checkpoint folder at the kill: files and bytes per top-level entry
    for f in folder.rglob("*"):
        if f.is_file():
            top = f.relative_to(folder).parts[0]
            tree.setdefault(top, [0, 0])
            tree[top][0] += 1
            tree[top][1] += f.stat().st_size
    t0 = time.monotonic()
    subprocess.run([*COMPOSE, "kill", "flink-taskmanager"], cwd=ROOT, check=True, capture_output=True)
    age = (time.time() * 1000 - last["latest_ack_timestamp"]) / 1000
    log = [
        f"+0.0 s   docker kill (SIGKILL) the TaskManager; last completed checkpoint {last.get('id')}, {age:.1f} s ago"
    ]
    started_tm, seen, restored = False, "RUNNING", None
    while time.monotonic() - t0 < 300:
        if not started_tm and time.monotonic() - t0 >= restart_after:
            subprocess.run([*COMPOSE, "start", "flink-taskmanager"], cwd=ROOT, check=True, capture_output=True)
            log.append(f"+{time.monotonic() - t0:.1f} s   a new TaskManager container starts")
            started_tm = True
        job = rest(f"/jobs/{job_id}")
        if job["state"] != seen:
            seen = job["state"]
            log.append(f"+{time.monotonic() - t0:.1f} s   job {seen}")
        restored = rest(f"/jobs/{job_id}/checkpoints").get("latest", {}).get("restored")
        if restored and all(v["status"] in ("RUNNING", "FINISHED") for v in job["vertices"]) and seen == "RUNNING":
            log.append(
                f"+{time.monotonic() - t0:.1f} s   all tasks running, state restored from checkpoint {restored['id']}"
            )
            break
        time.sleep(0.5)
    return log, tree


def run(
    kill_at: float | None,
    throttle_ms: int = THROTTLE_MS,
    align: bool = False,
    fetch_bytes: int | None = None,
    unaligned: bool = False,
) -> dict:
    reset()
    total = total_lines()
    job_args = ["--throttle-ms", str(throttle_ms), "--align", str(align).lower(), "--unaligned", str(unaligned).lower()]
    if fetch_bytes:
        job_args += ["--fetch-bytes", str(fetch_bytes)]
    started = time.monotonic()
    job_id = submit(1, *job_args)
    result: dict = {"job": job_id, "total_lines": total}
    if kill_at is not None:
        wait_until(job_id, 0.25, total)
        time.sleep(5)  # let the metrics settle on a steady state
        result["backpressure"] = backpressure(job_id)
        wait_until(job_id, kill_at, total)
        result["kill"], result["state_tree"] = kill_taskmanager(job_id)
    job = wait_job(job_id)
    result["state"], result["seconds"] = job["state"], time.monotonic() - started
    result["checkpoints"] = checkpoint_counts(job_id)
    result["restarts"] = int(rest(f"/jobs/{job_id}/metrics?get=numRestarts")[0]["value"])
    entries = rest(f"/jobs/{job_id}/exceptions").get("exceptionHistory", {}).get("entries") or []
    result["failures"] = [f"{e['exceptionName']}: {(e.get('stacktrace') or '').splitlines()[0][:200]}" for e in entries]
    history = rest(f"/jobs/{job_id}/checkpoints")["history"]
    done = [h for h in history if h["status"] == "COMPLETED"]
    result["checkpoint_sizes"] = [
        {
            "id": h["id"],
            "state": h["state_size"],
            "uploaded": h["checkpointed_size"],
            "in-flight": h.get("persisted_data", 0),
            "ms": h["end_to_end_duration"],
        }
        for h in done
    ]
    CAPTURE.write_text(json.dumps(result, indent=1))
    return result


def print_run(r: dict) -> None:
    for line in r.get("kill", []):
        print(line)
    for f in r.get("failures", []):
        print(f"root cause: {f}")
    c = r["checkpoints"]
    print(
        f"job {r['job'][:8]} {r['state']} after {r['seconds']:.0f} s; restarts {r['restarts']}; "
        f"checkpoints completed {c['completed']}, failed {c['failed']}"
    )


def cmd_run(a: argparse.Namespace) -> None:
    r = run(a.kill_at, unaligned=a.unaligned)
    if "backpressure" in r:
        print("at 25% of the replay:")
        print_backpressure(r["backpressure"])
    print_run(r)
    if "state_tree" in r:
        print("checkpoint folder at the kill (data/lab/flink-state/checkpoints/<job id>/):")
        for top, (n, size) in sorted(r["state_tree"].items()):
            print(f"  {top + '/':12} {n:>3} files {size / 1024:>8,.0f} KiB")
    print_check(check())


def cmd_checkpoints(a: argparse.Namespace) -> None:
    r = run(None, unaligned=a.unaligned)
    sizes = sorted(r["checkpoint_sizes"], key=lambda x: x["id"])
    kind = "unaligned" if a.unaligned else "aligned"
    print(
        f"{kind} checkpoints every 10 s, RocksDB, incremental; job {r['job'][:8]} {r['state']} in {r['seconds']:.0f} s"
    )
    print(
        f"{'checkpoint':>10} {'state (KiB)':>12} {'uploaded (KiB)':>14} {'in-flight (KiB)':>15} {'duration (ms)':>13}"
    )
    for x in sizes:
        print(
            f"{x['id']:>10} {x['state'] / 1024:>12,.0f} {x['uploaded'] / 1024:>14,.0f} {x['in-flight'] / 1024:>15,.0f}"
            f" {x['ms']:>13,}"
        )
    durations = sorted(x["ms"] for x in sizes)
    print(f"median duration {durations[len(durations) // 2]:,} ms over {len(sizes)} checkpoints")


# ---------- check: the lake and the topics against the batch answer ----------


def topic_alerts(topic: str, isolation: str) -> list[dict]:
    out = p27.kafka(
        "--topic",
        topic,
        "--from-beginning",
        "--isolation-level",
        isolation,
        "--timeout-ms",
        "8000",
        tool="kafka-console-consumer.sh",
        check=False,
    )
    return [json.loads(x) for x in out.splitlines() if x.startswith("{")]


def lake_files(table) -> tuple[int, int]:
    """Data files the table references, and data files in its folder (the rest were written, never committed)."""
    referenced = {f.file_path for task in table.scan().plan_files() for f in [task.file]}
    folder = table.location().removeprefix("s3://").rstrip("/") + "/data"
    fs = star.fs()
    fs.invalidate_cache()
    stored = [p for p in fs.find(folder) if p.endswith(".parquet")] if fs.exists(folder) else []
    return len(referenced), len(stored)


def check(e: dict | None = None) -> dict:
    e = e or expected()
    cat = p27.catalog()
    c = duckdb.connect()
    t = cat.load_table(ALERTS)
    c.register("a", t.scan().to_arrow())
    lake = c.execute(
        "SELECT store_id, sku, kind, epoch_ms(event_ts), epoch_ms(detected_ts) FROM a WHERE kind <> 'LOW_STOCK'"
    ).fetchall()
    low = c.execute("SELECT count(*) FROM a WHERE kind = 'LOW_STOCK'").fetchone()[0]
    versions = dict(c.execute("SELECT job_version, count(*) FROM a GROUP BY 1").fetchall())
    late_t = cat.load_table(LATE)
    c.register("l", late_t.scan().to_arrow())
    late = c.execute("SELECT store_id, sku, basket_id, qty FROM l").fetchall()
    behind = c.execute("SELECT count(*) FROM l WHERE event_ts <= watermark_ts").fetchone()[0]
    want, got = Counter(e["alerts"]), Counter(lake)
    out = {
        "expected": len(e["alerts"]),
        "lake rows": len(lake),
        "lake distinct": len(got),
        "lake duplicates": sum(n - 1 for n in got.values() if n > 1),
        "missing": sum((want - got).values()),
        "unexpected": sum((got - want).values()),
        "kinds": dict(Counter(x[2] for x in lake)),
        "low stock": low,
        "versions": versions,
        "snapshots": len(t.snapshots()),
        "commits": [
            (
                int(x.summary.additional_properties.get("flink.max-committed-checkpoint-id", -1)),
                int(x.summary.additional_properties.get("added-records", 0)),
            )
            for x in t.snapshots()
        ],
        "files": lake_files(t),
        "late expected": len(e["late"]),
        "late rows": len(late),
        "late behind watermark": behind,
        "late equal": Counter(late) == Counter(e["late"]),
    }
    for topic, isolation in (
        (TOPIC_EOS, "read_committed"),
        (TOPIC_ALO, "read_committed"),
        (TOPIC_EOS, "read_uncommitted"),
    ):
        msgs = topic_alerts(topic, isolation)
        rows = Counter(
            (m["store_id"], m["sku"], m["kind"], m["event_ts"], m["detected_ts"])
            for m in msgs
            if m["kind"] != "LOW_STOCK"
        )
        out[f"{topic} {isolation}"] = {
            "records": len(msgs),
            "distinct": len(rows),
            "duplicates": sum(n - 1 for n in rows.values() if n > 1),
            "missing": sum((want - +rows).values()),  # row by row against the batch answer, ignoring repeats
            "unexpected": sum((+Counter(set(rows)) - want).values()),
        }
    return out


def print_check(r: dict) -> None:
    k = r["kinds"]
    print(f"expected (batch rule over the raw zone): {r['expected']:,} alerts")
    print(
        f"{ALERTS}: {r['lake rows']:,} rows (STOCK_OUT {k.get('STOCK_OUT', 0):,}, BOOK_ERROR "
        f"{k.get('BOOK_ERROR', 0):,}), duplicates {r['lake duplicates']}, missing {r['missing']}, "
        f"unexpected {r['unexpected']}; snapshots {r['snapshots']}"
    )
    commits = r["commits"]
    if commits:
        ids = [c for c, _ in commits]
        print(
            f"  commits: checkpoints {ids[0]}..{ids[-1] if ids[-1] < 2**62 else 'end of input'}, "
            f"{len(ids)} snapshots, checkpoint IDs strictly increasing: {ids == sorted(set(ids))}; "
            f"rows added {sum(n for _, n in commits):,}"
        )
    referenced, stored = r["files"]
    print(f"  data files: {referenced} in the table, {stored} in its folder ({stored - referenced} never committed)")
    if r["low stock"]:
        print(f"  LOW_STOCK (version 2 only): {r['low stock']:,}; rows by job version: {r['versions']}")
    print(
        f"{LATE}: {r['late rows']:,} late lines (expected {r['late expected']:,}; same lines: {r['late equal']}); "
        f"behind the watermark on arrival: {r['late behind watermark']:,}"
    )
    for name in (f"{TOPIC_EOS} read_committed", f"{TOPIC_ALO} read_committed", f"{TOPIC_EOS} read_uncommitted"):
        t = r[name]
        print(
            f"{name}: {t['records']:,} records, {t['distinct']:,} distinct, duplicates {t['duplicates']}, "
            f"missing {t['missing']}, unexpected {t['unexpected']}"
        )


def cmd_check(_: argparse.Namespace) -> None:
    print_check(check())


# ---------- upgrade: savepoint, version 2, restore ----------


def stop_with_savepoint(job_id: str) -> str:
    out = flink_cli("stop", "--type", "canonical", "--savepointPath", "file:///flink-state/savepoints", job_id)
    return re.search(r"Savepoint completed\. Path: (\S+)", out).group(1)


def upgrade(stop_at: float, uids: bool = True, same_topology: bool = False) -> dict:
    reset()
    total = total_lines()
    first = submit(1, "--throttle-ms", str(THROTTLE_MS), "--uids", str(uids).lower())
    wait_until(first, stop_at, total)
    savepoint = stop_with_savepoint(first)
    log = {"v1": first, "savepoint": savepoint, "v1 lines read": source_records(first)}
    if uids:  # version 2 with the same uids; no throttle this time, so one stateless operator fewer
        second = submit(2, savepoint=savepoint, parallelism=2)
    elif same_topology:  # version 2 without uids either, and with the throttle again: the same graph shape
        second = submit(2, "--uids", "false", "--throttle-ms", str(THROTTLE_MS), savepoint=savepoint)
    else:  # version 2 sets uids (and has no throttle): the generated IDs it would need are gone
        refused = flink_cli(
            "run", "-d", "-s", savepoint, "-c", JOB_CLASS, "/opt/flink/lab/jobs/flink-jobs-v2.jar", check=False
        )
        reason = re.search(r"(Failed to rollback.*|Cannot map checkpoint/savepoint state.*)", refused)
        log["refused"] = reason.group(1)[:300] if reason else refused[-600:]
        second = submit(2, savepoint=savepoint, allow_non_restored=True)
    log["v2 parallelism"] = max(v["parallelism"] for v in vertices(second))
    log["v2 drained"] = drain_when_read(second)
    log.update({"v2": second, "v2 state": wait_job(second)["state"]})
    return log


def read_to_end() -> bool:
    """The job's committed offsets (written at each completed checkpoint) at the end of every POS partition. The last
    offset of each partition is a transaction marker (Part 27), which no consumer ever receives."""
    out = p27.kafka("--describe", "--group", "part29-stock-out", tool="kafka-consumer-groups.sh", check=False)
    lags = [ln.split()[5] for ln in out.splitlines() if ln.startswith("part29-stock-out ")]
    return len(lags) == 6 and all(x.isdigit() and int(x) <= 1 for x in lags)


def drain_when_read(job_id: str, timeout: int = 600) -> bool:
    """A bounded Kafka source restored from a savepoint reads to its end and never finishes (FLINK-31006, fixed in
    connector 5.1.0, unreleased). Once it has read everything, `stop --drain` ends it as a live job is retired: the
    watermark jumps to the end of time, every timer fires, the sinks commit in a final savepoint."""
    # "Read everything": the committed offsets reach the end, or, since a split that finishes between two
    # checkpoints never gets its offsets committed, the source has emitted nothing new for 20 s.
    started = time.monotonic()
    last, since = -1, time.monotonic()
    while time.monotonic() - started < timeout:
        if rest(f"/jobs/{job_id}")["state"] == "FINISHED":
            return False
        emitted = sum(source_records_per_vertex(job_id))
        if emitted != last:
            last, since = emitted, time.monotonic()
        if read_to_end() or (emitted > 0 and time.monotonic() - since >= 20):
            # a source that started without restored state (--allowNonRestoredState) is bounded and can finish on
            # its own between the check above and the stop: then Flink no longer finds a running job to stop
            stopped = flink_cli(
                "stop", "--drain", "--savepointPath", "file:///flink-state/savepoints", job_id, check=False
            )
            if "Savepoint completed" in stopped:
                return True
            if rest(f"/jobs/{job_id}")["state"] == "FINISHED":
                return False
            raise SystemExit(stopped[-3000:])
        time.sleep(2)
    raise SystemExit(f"job {job_id} did not read to the end in {timeout} s")


def cmd_upgrade(a: argparse.Namespace) -> None:
    r = upgrade(a.stop_at)
    print(f"v1 {r['v1'][:8]} stopped after {r['v1 lines read']:,} lines; savepoint {r['savepoint']}")
    drained = " after stop --drain" if r["v2 drained"] else ""
    print(f"v2 {r['v2'][:8]} restored at parallelism {r['v2 parallelism']}: {r['v2 state']}{drained}")
    print_check(check())


def cmd_nouids(a: argparse.Namespace) -> None:
    r = upgrade(a.stop_at, uids=False, same_topology=a.same_topology)
    print(f"v1 {r['v1'][:8]} (no uids) stopped after {r['v1 lines read']:,} lines; savepoint {r['savepoint']}")
    drained = " after stop --drain" if r["v2 drained"] else ""
    if a.same_topology:
        print(f"v2 without uids, same operators (throttle included), no -n: {r['v2 state']}{drained}")
    else:
        print(f"v2 with uids, no throttle: refused: {r['refused']}")
        print(f"v2 with --allowNonRestoredState: {r['v2 state']}{drained}")
    print_check(check())


def cmd_fast(a: argparse.Namespace) -> None:
    r = run(None, throttle_ms=0, align=a.align, fetch_bytes=a.fetch_bytes)
    print_run(r)
    e = expected()
    got = check(e)
    print(
        f"fetch bytes {a.fetch_bytes or 'job default (2048)'}, alignment {'on' if a.align else 'off'}: late lines "
        f"{got['late rows']:,} (expected {got['late expected']:,}), {got['late behind watermark']:,} of them "
        f"behind the watermark; alerts missing {got['missing']}, unexpected {got['unexpected']}"
    )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup").set_defaults(fn=cmd_setup)
    sub.add_parser("build").set_defaults(fn=cmd_build)
    sub.add_parser("batch").set_defaults(fn=cmd_batch)
    s = sub.add_parser("run")
    s.add_argument("--kill-at", type=float, default=None, help="kill the TaskManager at this share of the input")
    s.add_argument("--unaligned", action="store_true", help="unaligned checkpoints")
    s.set_defaults(fn=cmd_run)
    sub.add_parser("check").set_defaults(fn=cmd_check)
    s = sub.add_parser("checkpoints")
    s.add_argument("--unaligned", action="store_true")
    s.set_defaults(fn=cmd_checkpoints)
    for name, fn in (("upgrade", cmd_upgrade), ("nouids", cmd_nouids)):
        s = sub.add_parser(name)
        s.add_argument("--stop-at", type=float, default=0.5)
        if name == "nouids":
            s.add_argument("--same-topology", action="store_true", help="version 2 without uids, throttle kept")
        s.set_defaults(fn=fn)
    s = sub.add_parser("fast")
    s.add_argument("--align", action="store_true")
    s.add_argument("--fetch-bytes", type=int, default=None)
    s.set_defaults(fn=cmd_fast)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
