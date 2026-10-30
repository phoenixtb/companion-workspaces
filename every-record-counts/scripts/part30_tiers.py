"""Part 30: the streaming lakehouse. One stock metric at three latencies, with its freshness and its cost measured.

The tiers read the same live topic (`larkspur.pos.live`), each on its own:
  hours    an hourly batch on the host: Kafka -> DuckDB/PyIceberg, one Iceberg commit per run, then the stock table
  minutes  a Flink SQL job committing sale lines to Iceberg every 60 s (Part 28's sink)
  seconds  ClickHouse: a Kafka table engine and a materialized view into MergeTree

python -m scripts.part30_tiers setup                 # (make up-realtime) empty tables, topic, ClickHouse ingestion
python -m scripts.part30_tiers live [--speed 24]     # replay upload day 15 Aug live: all three tiers, metered
python -m scripts.part30_tiers freshness             # produced -> queryable, per tier, from captured timestamps
python -m scripts.part30_tiers cost                  # commits, files, parts, bytes, CPU, memory; priced (projection)
python -m scripts.part30_tiers query                 # the dashboard's question on each tier: answer, time, requests
python -m scripts.part30_tiers check                 # every tier against the batch answer: totals, then row by row
python -m scripts.part30_tiers clock                 # host clock vs the VM's (ClickHouse, the Flink TaskManager)
python -m scripts.part30_tiers compact               # the compaction run's table against the plain one
python -m scripts.part30_tiers hourly                # one run of the hourly batch (the live run schedules it)
python -m scripts.part30_tiers cleanup               # drop Part 30's tables, topic, groups and ClickHouse database

Measurements land in data/lab/part30/*.json. Times are UTC. The producer and the hourly batch run on the host;
ClickHouse and Flink stamp their times in the Docker VM, so `live` records how far the two clocks differ (`clock`).
"""

import argparse
import base64
import gzip
import io
import json
import resource
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa

from scripts import part04_star as star
from scripts import part24_cost as p24
from scripts import part27_backbone as p27
from scripts import part28_flink as p28
from scripts import part29_stock as p29

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "data/lab/part30"
COMPOSE = p27.COMPOSE
FLINK = "http://localhost:8081"
CLICKHOUSE = "http://localhost:8123/"
CH_AUTH = base64.b64encode(b"larkspur:larkspur-lab-secret").decode()
CCOMPAT = "http://localhost:8085/apis/ccompat/v7"
TOPIC = "larkspur.pos.live"
DAY = "2026-08-15"  # the business day the stock metric covers (Part 29's opening stock)
UPLOAD_DAY = "2026-08-15"  # the upload day the replay plays
PREFIX = ""  # tests set "test-" so they never overwrite the post's measurements
GROUPS = ("clickhouse-stock", "clickhouse-stock-default", "stock-hourly", "flink-stock-minute")
LINES_1M, LINES_1M_C = "stream.stock_lines_1m", "stream.stock_lines_1m_compacted"
LINES_1H, LEVEL_1H = "stream.stock_lines_1h", "stream.stock_level_hourly"
TABLES = (LINES_1M, LINES_1M_C, LINES_1H, LEVEL_1H)
CONTAINERS = (
    "flink-taskmanager",
    "flink-jobmanager",
    "clickhouse",
    "kafka",
    "apicurio",
    "seaweedfs",
    "polaris",
    "postgres",
)
ZONES = {"GB": ZoneInfo("Europe/London"), "IN": ZoneInfo("Asia/Kolkata"), "SG": ZoneInfo("Asia/Singapore")}


# ---------- clients ----------


def ch(sql: str, fmt: str = "JSONCompact") -> dict | str:
    url = CLICKHOUSE + "?output_format_json_quote_64bit_integers=0"
    body = sql if not fmt else f"{sql}\nFORMAT {fmt}"
    req = urllib.request.Request(url, data=body.encode(), headers={"Authorization": f"Basic {CH_AUTH}"})
    try:
        text = urllib.request.urlopen(req, timeout=120).read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode().strip()) from None
    return json.loads(text) if fmt == "JSONCompact" else text


def ch_rows(sql: str) -> list[list]:
    return ch(sql)["data"]


def ch_script(path: Path) -> None:
    """A ClickHouse SQL file, one statement at a time (the HTTP interface takes one per request)."""
    text = "\n".join(line for line in path.read_text().splitlines() if not line.lstrip().startswith("--"))
    for stmt in (s.strip() for s in text.split(";")):
        if stmt:
            ch(stmt, fmt="")


def save(name: str, data) -> Path:
    LAB.mkdir(parents=True, exist_ok=True)
    path = LAB / f"{PREFIX}{name}.json"
    path.write_text(json.dumps(data, indent=1, default=str))
    return path


def load(name: str):
    return json.loads((LAB / f"{PREFIX}{name}.json").read_text())


def utc(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%H:%M:%S.%f")[:-3]


def cpu_usec(container: str) -> int:
    """CPU time the container has used since it started (cgroup v2 cpu.stat), in microseconds."""
    out = subprocess.run(
        ["docker", "exec", f"every-record-counts-{container}-1", "cat", "/sys/fs/cgroup/cpu.stat"],
        capture_output=True,
        text=True,
    ).stdout
    for line in out.splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1])
    return 0


def mem_sample() -> dict[str, float]:
    """MiB per lab container, from one `docker stats` sample."""
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"], capture_output=True, text=True
    ).stdout
    units = {"GiB": 1024, "MiB": 1, "KiB": 1 / 1024, "B": 1 / 1048576}
    found = {}
    for line in out.splitlines():
        name, used = line.split("\t")
        if name.startswith("every-record-counts-"):
            num = used.split(" / ")[0]
            unit = num.lstrip("0123456789.")
            found[name.removeprefix("every-record-counts-").removesuffix("-1")] = float(num[: -len(unit)]) * units[unit]
    return found


# ---------- setup ----------

LINE_COLS = (
    ("basket_id", "string"),
    ("line_no", "int"),
    ("store_id", "string"),
    ("sku", "string"),
    ("qty", "int"),
    ("business_date", "date"),
    ("event_ts", "timestamp"),
    ("produced_at", "timestamp"),
)
LEVEL_COLS = (("store_id", "string"), ("sku", "string"), ("opening_qty", "int"), ("sold", "long"), ("level", "long"))


def iceberg_schema(cols: tuple):
    from pyiceberg.schema import Schema
    from pyiceberg.types import DateType, IntegerType, LongType, NestedField, StringType, TimestampType

    types = {
        "string": StringType(),
        "int": IntegerType(),
        "long": LongType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }
    return Schema(*(NestedField(i + 1, n, types[t], required=False) for i, (n, t) in enumerate(cols)))


def drop_tables() -> None:
    cat = p27.catalog()
    fs = star.fs()
    for ident in TABLES:
        if cat.table_exists(ident):
            folder = cat.load_table(ident).location().removeprefix("s3://").rstrip("/")
            cat.drop_table(ident)
            fs.invalidate_cache()
            if folder.startswith("larkspur-lake/warehouse/stream/") and fs.exists(folder):
                fs.rm(folder, recursive=True)


def create_hourly_tables() -> None:
    """The hourly tier's two tables, as root: the lines it lands and the stock table it publishes."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    cat = p27.catalog()
    spec = PartitionSpec(
        PartitionField(source_id=6, field_id=1000, transform=IdentityTransform(), name="business_date")
    )
    cat.create_table(LINES_1H, iceberg_schema(LINE_COLS), partition_spec=spec, properties={"format-version": "2"})
    cat.create_table(LEVEL_1H, iceberg_schema(LEVEL_COLS), properties={"format-version": "2"})


def reset_topic() -> None:
    p27.kafka("--delete", "--if-exists", "--topic", TOPIC)
    p27._wait_deleted([TOPIC])
    for g in GROUPS:
        p27.kafka("--delete", "--group", g, tool="kafka-consumer-groups.sh", check=False)
    # The schema is registered again by the producer, dereferenced (LiveProducer): ClickHouse can't follow references.
    p27.http("DELETE", f"{p27.REGISTRY}/registry/v3/groups/default/artifacts/{TOPIC}-value")
    # 6 partitions keyed by till (Part 27); one day of retention: the replay is a lab artefact.
    p27.kafka("--create", "--topic", TOPIC, "--partitions", "6", "--config", f"retention.ms={p27.DAY_MS}")


def load_opening() -> int:
    if not p29.OPENING.exists():
        p29.write_opening()
    data = gzip.decompress(p29.OPENING.read_bytes())
    url = CLICKHOUSE + "?query=" + urllib.parse.quote("INSERT INTO realtime.opening_stock FORMAT CSVWithNames")
    req = urllib.request.Request(url, data=data, headers={"Authorization": f"Basic {CH_AUTH}"})
    urllib.request.urlopen(req, timeout=120).read()
    return ch_rows("SELECT count() FROM realtime.opening_stock")[0][0]


def cmd_setup(_: argparse.Namespace) -> None:
    p29.cancel_jobs()
    p28.setup_principal()
    drop_tables()
    create_hourly_tables()
    ch("DROP DATABASE IF EXISTS realtime SYNC", fmt="")  # stop ClickHouse's consumers before the topic goes
    reset_topic()
    ch_script(ROOT / "sql/part30/clickhouse/01_ingest.sql")
    print(f"topic {TOPIC} (6 partitions, 1 day); tables {', '.join(TABLES[2:])} empty; ClickHouse database realtime")
    print(f"opening stock in ClickHouse: {load_opening():,} store-SKUs")


# ---------- the batch answer (raw zone) ----------


def replayed(limit: int = 0) -> duckdb.DuckDBPyConnection:
    """Part 29's raw views, plus `replay` = the events the producer sends (sorted by upload time, first N)."""
    c = p29.raw_con()
    lim = f"LIMIT {limit}" if limit else ""
    c.execute(f"""CREATE OR REPLACE VIEW replay AS
        SELECT * FROM raw_pos_events WHERE upload_date = '{UPLOAD_DAY}' ORDER BY uploaded_at {lim}""")
    c.execute("""CREATE OR REPLACE VIEW replay_lines AS
        WITH first_copy AS (
            SELECT * FROM replay QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) = 1)
        SELECT f.basket_id, l.line_no, f.store_id, l.sku, l.qty,
               (f.event_ts::TIMESTAMPTZ AT TIME ZONE s.timezone)::DATE AS business_date
        FROM first_copy f JOIN raw_stores s USING (store_id) CROSS JOIN UNNEST(f.lines) AS u(l)""")
    return c


def metric(c: duckdb.DuckDBPyConnection, lines: str = "lines") -> dict:
    """The dashboard's number from a relation of sale lines, in DuckDB: sql/part30/stock_now.sql."""
    c.execute(f"CREATE OR REPLACE TEMP VIEW stock_lines AS SELECT * FROM {lines}")
    c.execute(f"SET VARIABLE opening_file = '{p29.OPENING}'")
    c.execute(f"SET VARIABLE day = '{DAY}'")
    row = c.execute((ROOT / "sql/part30/stock_now.sql").read_text()).fetchone()
    return dict(zip(("store_skus", "at_or_below_zero", "availability_pct", "units_sold"), row, strict=True))


def expected(limit: int = 0) -> dict:
    c = replayed(limit)
    lines = c.execute("SELECT count(*) FROM replay_lines").fetchone()[0]
    raw_lines = c.execute("SELECT sum(len(lines)) FROM replay").fetchone()[0]
    return {
        "events": c.execute("SELECT count(*) FROM replay").fetchone()[0],
        "lines": lines,
        "lines_with_copies": int(raw_lines),
        **metric(c, "replay_lines"),
    }


# ---------- the hourly batch ----------

_SCHEMAS: dict[int, object] = {}


def decode(value: bytes) -> dict:
    """One Confluent-framed Avro record: magic byte 0, a 4-byte schema ID, the body (Part 27)."""
    import fastavro

    sid = int.from_bytes(value[1:5], "big")
    if sid not in _SCHEMAS:
        with urllib.request.urlopen(f"{CCOMPAT}/schemas/ids/{sid}", timeout=10) as r:
            _SCHEMAS[sid] = fastavro.parse_schema(json.loads(json.load(r)["schema"]))
    return fastavro.schemaless_reader(io.BytesIO(value[5:]), _SCHEMAS[sid])


def read_new(offsets: dict[int, int]) -> tuple[list[dict], dict[int, int]]:
    """Every record after `offsets` up to the end offsets seen at the start: a bounded read, like a batch."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "stock-hourly",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    parts = c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    ends = {p: c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1] for p in parts}
    todo = {p: offsets.get(p, 0) for p in parts if offsets.get(p, 0) < ends[p]}
    c.assign([TopicPartition(TOPIC, p, o) for p, o in todo.items()])
    rows, done = [], dict(offsets)
    while todo:
        for m in c.consume(500, timeout=2):
            if m.error() or m.offset() >= ends[m.partition()]:
                continue
            e = decode(m.value())
            rows.append({"partition": m.partition(), "offset": m.offset(), "produced_ms": m.timestamp()[1], **e})
            done[m.partition()] = m.offset() + 1
            if done[m.partition()] >= ends[m.partition()]:
                todo.pop(m.partition(), None)
    c.close()
    return rows, {p: max(done.get(p, 0), offsets.get(p, 0)) for p in parts}


def hourly() -> dict:
    """One run: read what arrived since the last run, land the new lines (one commit, offsets in its summary, so a
    rerun after a crash resumes exactly), then rebuild the stock table (one more commit). Returns what it did."""
    cpu0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.time()
    cat = p27.catalog()
    lines_t = cat.load_table(LINES_1H)
    snap = lines_t.current_snapshot()
    prev = json.loads(snap.summary.additional_properties.get("larkspur.kafka-offsets", "{}")) if snap else {}
    offsets = {int(p): o for p, o in prev.items()}
    events, new_offsets = read_new(offsets)
    seen = set(lines_t.scan(selected_fields=("basket_id",)).to_arrow()["basket_id"].to_pylist()) if snap else set()
    rows = []
    for e in sorted(events, key=lambda r: (r["partition"], r["offset"])):  # copies share a partition (key: till)
        if e["basket_id"] in seen:
            continue
        seen.add(e["basket_id"])
        local = e["event_ts"].astimezone(ZONES[e["store_id"][:2]]).date()
        for ln in e["lines"]:
            rows.append(
                {
                    "basket_id": e["basket_id"],
                    "line_no": ln["line_no"],
                    "store_id": e["store_id"],
                    "sku": ln["sku"],
                    "qty": ln["qty"],
                    "business_date": local,
                    "event_ts": e["event_ts"].astimezone(UTC).replace(tzinfo=None),
                    "produced_at": datetime.fromtimestamp(e["produced_ms"] / 1000, UTC).replace(tzinfo=None),
                }
            )
    schema = lines_t.schema().as_arrow()
    props = {"larkspur.kafka-offsets": json.dumps({str(p): o for p, o in sorted(new_offsets.items())})}
    if rows:
        lines_t.append(pa.Table.from_pylist(rows, schema=schema), snapshot_properties=props)
    lines_t = cat.load_table(LINES_1H)
    lines_sid = lines_t.current_snapshot().snapshot_id if lines_t.current_snapshot() else None
    c = duckdb.connect()
    c.register("all_lines", lines_t.scan(row_filter=f"business_date = '{DAY}'").to_arrow())
    c.execute(f"SET VARIABLE opening_file = '{p29.OPENING}'")
    level = c.sql(
        """SELECT o.store_id, o.sku, o.opening_qty, coalesce(s.sold, 0)::BIGINT AS sold,
                  (o.opening_qty - coalesce(s.sold, 0))::BIGINT AS level
           FROM read_csv(getvariable('opening_file')) o
           LEFT JOIN (SELECT store_id, sku, sum(qty) AS sold FROM all_lines GROUP BY ALL) s USING (store_id, sku)"""
    ).to_arrow_table()
    level_t = cat.load_table(LEVEL_1H)
    level_t.overwrite(
        level.cast(level_t.schema().as_arrow()), snapshot_properties={"larkspur.lines-snapshot-id": str(lines_sid)}
    )
    cpu1 = resource.getrusage(resource.RUSAGE_SELF)  # ru_maxrss is in bytes on macOS
    return {
        "started": t0,
        "max_rss_mib": cpu1.ru_maxrss / 1048576,
        "seconds": time.time() - t0,
        "events": len(events),
        "new_lines": len(rows),
        "cpu_s": (cpu1.ru_utime - cpu0.ru_utime) + (cpu1.ru_stime - cpu0.ru_stime),
        "lines_snapshot": lines_sid,
        "published_snapshot": cat.load_table(LEVEL_1H).current_snapshot().snapshot_id,
    }


def cmd_hourly(a: argparse.Namespace) -> None:
    r = hourly()
    if a.json:  # the live run starts each hourly run as its own process and reads this line
        print(json.dumps(r))
        return
    print(
        f"hourly run: {r['events']:,} events read, {r['new_lines']:,} new lines landed, stock table rebuilt; "
        f"{r['seconds']:.1f} s, CPU {r['cpu_s']:.1f} s, peak memory {r['max_rss_mib']:.0f} MiB"
    )


def hourly_process() -> dict:
    """One hourly run in a process of its own, as a scheduler would start it: its CPU and memory are its own."""
    t0 = time.time()
    out = subprocess.run([sys.executable, "-m", "scripts.part30_tiers", "hourly", "--json"], cwd=ROOT,
                         capture_output=True, text=True, check=True).stdout  # fmt: skip
    r = json.loads([line for line in out.splitlines() if line.startswith("{")][-1])
    return r | {"process_seconds": time.time() - t0}


# ---------- the minute tier (Flink) ----------


def submit(sql_file: str, interval: str, rewrite_after: int = 10) -> str:
    """Run flink/sql/part30/<sql_file> after tables.sql with the commit (checkpoint) interval given; the job ID."""
    text = (ROOT / "flink/sql/part30" / sql_file).read_text().replace("'60 s'", f"'{interval}'")
    text = text.replace("commit-count' = '10'", f"commit-count' = '{rewrite_after}'")
    tmp = f"/tmp/part30-{sql_file}"
    subprocess.run(
        [*COMPOSE, "exec", "-T", "flink-jobmanager", "sh", "-c", f"cat > {tmp}"],
        input=text,
        text=True,
        check=True,
        cwd=ROOT,
    )
    cmd = [
        *COMPOSE,
        "exec",
        "-T",
        "flink-jobmanager",
        "./bin/sql-client.sh",
        "-i",
        "/opt/flink/lab/sql/part30/tables.sql",
        "-f",
        tmp,
    ]
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True).stdout
    if "Job ID:" not in out:
        raise SystemExit(out[-3000:])
    return out.split("Job ID:")[1].split()[0]


def table_rows(ident: str) -> int:
    t = p27.catalog().load_table(ident)
    s = t.current_snapshot()
    return int(s.summary.additional_properties.get("total-records", 0)) if s else 0


# ---------- the live run ----------


class Sampler(threading.Thread):
    """docker stats every few seconds: peak MiB per container and the highest total at one sample."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.peak: dict[str, float] = defaultdict(float)
        self.peak_total = (0.0, {})
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            s = mem_sample()
            for k, v in s.items():
                self.peak[k] = max(self.peak[k], v)
            if sum(s.values()) > self.peak_total[0]:
                self.peak_total = (sum(s.values()), s)
            self.stop.wait(3)


def ch_lines(table: str = "sale_lines") -> int:
    """Distinct lines visible (FINAL): merges may already have removed some copies, so raw row counts can't tell."""
    return ch_rows(f"SELECT count() FROM realtime.{table} FINAL")[0][0]


def wait_for(what: str, fn, target: int, timeout: float) -> float:
    end = time.time() + timeout
    while time.time() < end:
        if fn() >= target:
            return time.time()
        time.sleep(1)
    raise SystemExit(f"{what}: {fn():,} of {target:,} after {timeout:.0f} s")


def clock(samples: int = 20) -> dict:
    """VM clock minus host clock, in ms: ClickHouse's now64() over HTTP and `date` in the TaskManager, each bracketed by
    two host readings. The sample with the shortest round trip wins; half of it is the uncertainty."""
    out = {}
    probes = {
        "clickhouse": lambda: int(ch_rows("SELECT toUnixTimestamp64Milli(now64(3))")[0][0]) / 1000,
        "flink-taskmanager": lambda: float(
            subprocess.run(
                ["docker", "exec", "every-record-counts-flink-taskmanager-1", "date", "+%s.%N"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout  # fmt: skip
        ),
    }
    for name, probe in probes.items():
        best = None
        for _ in range(samples):
            t0 = time.time()
            vm = probe()
            t1 = time.time()
            if best is None or t1 - t0 < best[1]:
                best = ((vm - (t0 + t1) / 2) * 1000, t1 - t0)
        out[name] = {"offset_ms": round(best[0], 1), "uncertainty_ms": round(best[1] * 500, 1)}
    return out


def cmd_clock(_: argparse.Namespace) -> None:
    for name, c in clock().items():
        print(f"{name:<18} VM clock minus host clock: {c['offset_ms']:+.1f} ms (± {c['uncertainty_ms']:.1f} ms)")


def topic_records() -> int:
    out = p27.kafka("--topic", TOPIC, tool="kafka-get-offsets.sh")
    return sum(int(line.rsplit(":", 1)[1]) for line in out.splitlines() if line.startswith(TOPIC))


def ch_ready(timeout: float = 120) -> float:
    """Seconds until both ClickHouse consumers hold all six partitions (a new group takes a while to join)."""
    t0 = time.time()
    while time.time() < t0 + timeout:
        rows = ch_rows("SELECT table, length(assignments.partition_id) FROM system.kafka_consumers "
                       "WHERE database = 'realtime'")  # fmt: skip
        if len(rows) == 2 and all(n == 6 for _, n in rows):
            return time.time() - t0
        time.sleep(1)
    raise SystemExit(f"ClickHouse consumers not assigned after {timeout:.0f} s: {rows}")


def live(
    speed: float, interval: str, hourly_every: float, limit: int = 0, compacted: bool = False, rewrite_after: int = 10
) -> dict:
    """The replay with every tier running. Hourly runs at start + k * hourly_every seconds while the replay lasts and
    once more when it ends (the run the next scheduled hour would make), so every line is published."""
    subprocess.run(["pkill", "-f", "kafka-backbone:run"], check=False)  # no producer left over from a failed run
    if topic_records():
        raise SystemExit(f"{TOPIC} is not empty: run setup first")
    exp = expected(limit)
    clocks = clock()
    jobs = {"minute": submit("minute.sql", interval)}
    try:
        if compacted:
            jobs["minute_compacted"] = submit("minute_compacted.sql", interval, rewrite_after)
        for j in jobs.values():
            while p29.rest(f"/jobs/{j}")["state"] != "RUNNING":
                time.sleep(1)
        ready = ch_ready()
        sampler = Sampler()
        sampler.start()
        cpu0 = {c: cpu_usec(c) for c in CONTAINERS}
        req0 = p24.s3_requests()
        t0 = time.time()
        producer = subprocess.Popen(
            ["./gradlew", "-q", ":kafka-backbone:run", f"--args=live --topic {TOPIC} --speed {speed} --limit {limit}"],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        runs, k, log = [], 1, []
        while producer.poll() is None:
            if hourly_every and time.time() >= t0 + k * hourly_every:
                runs.append(hourly_process())
                k += 1
            time.sleep(1)
        log = producer.stdout.read().splitlines()
        t_end = time.time()
        print("\n".join(log))
        wait_for("ClickHouse", ch_lines, exp["lines"], 120)
        wait_for("ClickHouse (default flush)", lambda: ch_lines("sale_lines_default"), exp["lines"], 120)
        for name, ident in (("minute", LINES_1M), ("minute_compacted", LINES_1M_C)):
            if name in jobs:
                wait_for(f"Iceberg {ident}", lambda i=ident: table_rows(i), exp["lines"], 300)
        if hourly_every:
            runs.append(hourly_process())  # the next scheduled hour publishes the replay's last minutes
        t1 = time.time()
        cpu1 = {c: cpu_usec(c) for c in CONTAINERS}
        req1 = p24.s3_requests()
        sampler.stop.set()
        sampler.join()
        for j in jobs.values():
            p29.rest(f"/jobs/{j}", method="PATCH")  # cancel: the tiers are measured
        result = {
            "speed": speed,
            "interval": interval,
            "limit": limit,
            "expected": exp,
            "jobs": jobs,
            "started": t0,
            "producer_done": t_end,
            "ended": t1,
            "producer_log": log,
            "hourly_runs": runs,
            "clickhouse_ready_s": ready,
            "clock_before": clocks,
            "clock_after": clock(),
            "cpu_s": {c: (cpu1[c] - cpu0[c]) / 1e6 for c in CONTAINERS},
            "requests": {k: req1.get(k, 0) - req0.get(k, 0) for k in set(req0) | set(req1)},
            "mem_peak_mib": dict(sampler.peak),
            "mem_peak_total": sampler.peak_total,
        }
    except BaseException:  # a failed run leaves no job writing and no producer sending
        p29.cancel_jobs()
        subprocess.run(["pkill", "-f", "kafka-backbone:run"], check=False)
        raise
    save("live-compacted" if compacted else "live", result)
    return result


def cmd_live(a: argparse.Namespace) -> None:
    r = live(a.speed, a.interval, a.hourly_every, a.limit, a.compacted)
    print(
        f"replay {(r['producer_done'] - r['started']) / 60:.1f} min; all tiers caught up "
        f"{r['ended'] - r['producer_done']:.0f} s later; hourly runs {len(r['hourly_runs'])}"
    )
    for name, c in r["clock_after"].items():
        print(f"clock: {name} minus host {c['offset_ms']:+.1f} ms (± {c['uncertainty_ms']:.1f}) after the run, "
              f"{r['clock_before'][name]['offset_ms']:+.1f} ms before")  # fmt: skip
    print("CPU seconds: " + ", ".join(f"{k} {v:.0f}" for k, v in r["cpu_s"].items()))
    print(
        f"memory: highest total {r['mem_peak_total'][0] / 1024:.2f} GiB; peaks "
        + ", ".join(f"{k} {v / 1024:.2f}" for k, v in sorted(r["mem_peak_mib"].items(), key=lambda x: -x[1]))
    )


# ---------- freshness ----------


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def describe(xs: list[float]) -> dict:
    return {"rows": len(xs), "min": min(xs), "p50": statistics.median(xs), "p95": pct(xs, 0.95), "max": max(xs)}


def added_files(t, snapshot_id: int) -> list[str]:
    e = t.inspect.entries(snapshot_id=snapshot_id).to_pylist()
    return [x["data_file"]["file_path"] for x in e if x["status"] == 1 and x["snapshot_id"] == snapshot_id]


def produced_in(t, path: str) -> list[datetime]:
    import pyarrow.parquet as pq

    with t.io.new_input(path).open() as f:
        return pq.read_table(f, columns=["produced_at"])["produced_at"].to_pylist()


def lake_freshness(ident: str) -> list[float]:
    """Seconds from produced to the commit of the snapshot that added the line (when a reader can first see it)."""
    t = p27.catalog().load_table(ident)
    out = []
    for s in t.snapshots():
        if s.summary.operation.value != "append":
            continue
        for path in added_files(t, s.snapshot_id):
            out += [s.timestamp_ms / 1000 - p.replace(tzinfo=UTC).timestamp() for p in produced_in(t, path)]
    return out


def hourly_freshness() -> list[float]:
    """Seconds from produced to the commit of the stock table that first included the line."""
    t = p27.catalog().load_table(LINES_1H)
    level = p27.catalog().load_table(LEVEL_1H)
    published = {}  # lines snapshot -> first stock snapshot built from it
    for s in sorted(level.snapshots(), key=lambda s: s.timestamp_ms):
        sid = s.summary.additional_properties.get("larkspur.lines-snapshot-id")
        if s.summary.operation.value in ("overwrite", "append") and sid and int(sid) not in published:
            published[int(sid)] = s.timestamp_ms
    out = []
    for s in t.snapshots():
        if s.snapshot_id in published:
            for path in added_files(t, s.snapshot_id):
                pub = published[s.snapshot_id] / 1000
                out += [pub - p.replace(tzinfo=UTC).timestamp() for p in produced_in(t, path)]
    return out


def freshness() -> dict:
    res = {}
    for name, table in (("seconds", "sale_lines"), ("seconds, default flush", "sale_lines_default")):
        xs = ch_rows(f"SELECT dateDiff('millisecond', produced_at, inserted_at) FROM realtime.{table}")
        res[name] = describe([x[0] / 1000 for x in xs])
    res["minutes"] = describe(lake_freshness(LINES_1M))
    res["hours"] = describe(hourly_freshness())
    save("freshness", res)
    return res


def fmt_s(s: float) -> str:
    return f"{s:.2f} s" if s < 60 else f"{s / 60:.1f} min"


def cmd_freshness(_: argparse.Namespace) -> None:
    print("produced (Kafka record timestamp) -> queryable, per line")
    print(f"{'tier':<24}{'lines':>8}{'min':>11}{'median':>11}{'p95':>11}{'max':>11}")
    for name, d in freshness().items():
        print(f"{name:<24}{d['rows']:>8,}" + "".join(f"{fmt_s(d[k]):>11}" for k in ("min", "p50", "p95", "max")))


# ---------- cost ----------


def lake_stats(ident: str) -> dict:
    """Commits, live files, every object under the table folder (data, metadata) and their bytes."""
    t = p27.catalog().load_table(ident)
    snaps = t.snapshots()
    ops = Counter(s.summary.operation.value for s in snaps)
    cur = t.current_snapshot().summary.additional_properties if t.current_snapshot() else {}
    fs = star.fs()
    fs.invalidate_cache()
    folder = t.location().removeprefix("s3://")
    kinds = Counter()
    sizes = Counter()
    for path, info in fs.find(folder, detail=True).items():
        k = "data" if "/data/" in path else "metadata"
        kinds[k] += 1
        sizes[k] += info["size"]
    return {
        "commits": len(snaps),
        "operations": dict(ops),
        "live_data_files": int(cur.get("total-data-files", 0)),
        "live_bytes": int(cur.get("total-files-size", 0)),
        "rows": int(cur.get("total-records", 0)),
        "objects": dict(kinds),
        "object_bytes": dict(sizes),
    }


def ch_stats(table: str, since: float) -> dict:
    t = f"realtime.{table}"
    ev = dict(
        ch_rows(
            f"SELECT event_type, count() FROM system.part_log WHERE database = 'realtime' AND "
            f"table = '{table}' AND event_time >= toDateTime({int(since)}) GROUP BY event_type"
        )
    )
    active = ch_rows(
        f"SELECT count(), sum(bytes_on_disk), sum(rows) FROM system.parts WHERE active AND "
        f"database = 'realtime' AND table = '{table}'"
    )[0]
    return {
        "new_parts": ev.get("NewPart", 0),
        "merges": ev.get("MergeParts", 0),
        "active_parts": active[0],
        "bytes": active[1],
        "rows": active[2],
        "rows_final": ch_rows(f"SELECT count() FROM {t} FINAL")[0][0],
    }


def cost() -> dict:
    ch("SYSTEM FLUSH LOGS", fmt="")
    r = load("live")
    hours = (r["producer_done"] - r["started"]) / 3600
    res = {
        "hours": hours,
        "hours_tier": {
            "lines": lake_stats(LINES_1H),
            "stock table": lake_stats(LEVEL_1H),
            "runs": len(r["hourly_runs"]),
            "cpu_s": sum(x["cpu_s"] for x in r["hourly_runs"]),
            "run_seconds": [round(x["seconds"], 1) for x in r["hourly_runs"]],
            "max_rss_mib": max((x["max_rss_mib"] for x in r["hourly_runs"]), default=0),
        },
        "minute_tier": {
            "lines": lake_stats(LINES_1M),
            "cpu_s": r["cpu_s"]["flink-taskmanager"],
            "cpu_s_jobmanager": r["cpu_s"]["flink-jobmanager"],
        },
        "seconds_tier": {
            "sale_lines": ch_stats("sale_lines", r["started"]),
            "sale_lines_default": ch_stats("sale_lines_default", r["started"]),
            "cpu_s": r["cpu_s"]["clickhouse"],
        },
        "shared": {k: r["cpu_s"][k] for k in ("kafka", "apicurio", "seaweedfs", "polaris", "postgres")},
        "mem_peak_mib": r["mem_peak_mib"],
        "requests": r["requests"],
    }
    res["projection"] = project(res)
    save("cost", res)
    return res


def project(m: dict) -> dict:
    """A month of each tier at the lab's volume and Part 24's list prices (cost/prices.json): CPU used, memory held,
    objects written. A projection from one replayed hour, not a bill."""
    prices = json.loads((ROOT / "cost/prices.json").read_text())["prices"]
    h = m["hours"]
    month = 730
    mem = m["mem_peak_mib"]

    def line(cpu_s: float, gib: float, held_h: float, puts: float) -> dict:
        cpu = cpu_s / h * month / 3600 * prices["vcpu_hour"]["usd"]
        ram = gib * held_h * prices["gb_hour"]["usd"] * 1.073741824  # GiB -> GB
        req = puts / h * month * prices["request_put"]["usd"]
        return {
            "vcpu_usd": round(cpu, 2),
            "memory_usd": round(ram, 2),
            "put_usd": round(req, 2),
            "total_usd": round(cpu + ram + req, 2),
        }

    ht = m["hours_tier"]
    runs_per_month = 24 * 30
    run_s = statistics.mean(ht["run_seconds"]) if ht["run_seconds"] else 0
    hours_puts = ht["lines"]["objects"].get("data", 0) + ht["lines"]["objects"].get("metadata", 0)
    hours_puts += ht["stock table"]["objects"].get("data", 0) + ht["stock table"]["objects"].get("metadata", 0)
    mt = m["minute_tier"]["lines"]
    return {
        "hours": line(ht["cpu_s"], ht["max_rss_mib"] / 1024, runs_per_month * run_s / 3600, hours_puts),
        "minutes": line(
            m["minute_tier"]["cpu_s"], mem.get("flink-taskmanager", 0) / 1024, month, sum(mt["objects"].values())
        ),
        "seconds": line(m["seconds_tier"]["cpu_s"], mem.get("clickhouse", 0) / 1024, month, 0),
        "note": "hourly batch: its peak memory while it runs; Flink TaskManager, ClickHouse: their peak all month",
    }


def cmd_cost(a: argparse.Namespace) -> None:
    m = load("cost") if a.saved else cost()  # --saved: print the last measurement again (the tables may be gone)
    print(f"one replayed hour ({m['hours'] * 60:.1f} min of wall clock), upload day {UPLOAD_DAY} at 24x")
    for name, key in (
        ("hours: lines", ("hours_tier", "lines")),
        ("hours: stock table", ("hours_tier", "stock table")),
        ("minutes: lines", ("minute_tier", "lines")),
    ):
        s = m[key[0]][key[1]]
        print(
            f"{name:<22} commits {s['commits']:>4} {s['operations']}  live data files {s['live_data_files']:>4}  "
            f"objects {sum(s['objects'].values()):>4} ({s['objects'].get('metadata', 0)} metadata), "
            f"data {s['object_bytes'].get('data', 0) / 1024:,.0f} KiB, "
            f"metadata {s['object_bytes'].get('metadata', 0) / 1024:,.0f} KiB"
        )
    for t in ("sale_lines", "sale_lines_default"):
        s = m["seconds_tier"][t]
        print(
            f"seconds: {t:<18} new parts {s['new_parts']:>5}, merges {s['merges']:>4}, active parts "
            f"{s['active_parts']}, {s['bytes'] / 1024:,.0f} KiB, rows {s['rows']:,} ({s['rows_final']:,} after FINAL)"
        )
    print(
        f"CPU seconds: hourly batch {m['hours_tier']['cpu_s']:.1f} over {m['hours_tier']['runs']} runs "
        f"({m['hours_tier']['run_seconds']} s); Flink TaskManager {m['minute_tier']['cpu_s']:.0f} "
        f"(JobManager {m['minute_tier']['cpu_s_jobmanager']:.0f}); ClickHouse {m['seconds_tier']['cpu_s']:.0f}; "
        + ", ".join(f"{k} {v:.0f}" for k, v in m["shared"].items())
    )
    print(
        "projected month at lab volume, Part 24 list prices (USD): "
        + "; ".join(
            f"{k} {v['total_usd']:.2f} (CPU {v['vcpu_usd']:.2f}, memory {v['memory_usd']:.2f}, PUTs {v['put_usd']:.2f})"
            for k, v in m["projection"].items()
            if k != "note"
        )
    )


# ---------- the dashboard query on each tier ----------


def lake_metric(ident: str) -> tuple[dict, float]:
    t0 = time.time()
    t = p27.catalog().load_table(ident)
    c = duckdb.connect()
    c.register("lines", t.scan(row_filter=f"business_date = '{DAY}'").to_arrow())
    return metric(c), time.time() - t0


def level_metric() -> tuple[dict, float]:
    t0 = time.time()
    t = p27.catalog().load_table(LEVEL_1H)
    c = duckdb.connect()
    c.register("level", t.scan().to_arrow())
    row = c.execute("""SELECT count(*), count(*) FILTER (WHERE level <= 0),
                              round(100 * count(*) FILTER (WHERE level > 0) / count(*), 2), sum(sold)
                       FROM level""").fetchone()
    return dict(
        zip(("store_skus", "at_or_below_zero", "availability_pct", "units_sold"), row, strict=True)
    ), time.time() - t0


def ch_metric() -> tuple[dict, float]:
    sql = (ROOT / "sql/part30/clickhouse/stock_now.sql").read_text()
    t0 = time.time()
    row = ch_rows(sql.rstrip().rstrip(";"))[0]
    return dict(
        zip(("store_skus", "at_or_below_zero", "availability_pct", "units_sold"), row, strict=True)
    ), time.time() - t0


def timed(fn, repeat: int = 3) -> tuple[dict, float, dict]:
    times = []
    req0 = p24.s3_requests()
    for _ in range(repeat):
        answer, s = fn()
        times.append(s)
    req1 = p24.s3_requests()
    per = {k: (req1.get(k, 0) - req0.get(k, 0)) / repeat for k in set(req0) | set(req1)}
    return answer, statistics.median(times), {k: v for k, v in per.items() if v}


def query() -> dict:
    res = {}
    for name, fn in (
        ("hours (published stock table)", level_metric),
        ("minutes (lines, Iceberg)", lambda: lake_metric(LINES_1M)),
        ("seconds (ClickHouse)", ch_metric),
    ):
        if fn is level_metric and not table_rows(LEVEL_1H):
            continue  # a run without the hourly tier (the compaction run)
        answer, s, req = timed(fn)
        res[name] = {"answer": answer, "median_s": s, "requests": req}
    if p27.catalog().table_exists(LINES_1M_C) and table_rows(LINES_1M_C):
        answer, s, req = timed(lambda: lake_metric(LINES_1M_C))
        res["minutes, compacted"] = {"answer": answer, "median_s": s, "requests": req}
    save("query-compacted" if "minutes, compacted" in res else "query", res)
    return res


def cmd_query(_: argparse.Namespace) -> None:
    print(f"{'tier':<32}{'store-SKUs':>11}{'at/below 0':>11}{'avail %':>9}{'units':>8}{'median':>9}  requests/query")
    for name, d in query().items():
        a = d["answer"]
        print(
            f"{name:<32}{a['store_skus']:>11,}{a['at_or_below_zero']:>11,}{a['availability_pct']:>9}"
            f"{a['units_sold']:>8,}{d['median_s']:>8.3f}s  "
            + (", ".join(f"{k} {v:.0f}" for k, v in sorted(d["requests"].items())) or "none (no object storage)")
        )


# ---------- check ----------


def levels(limit: int = 0) -> dict[str, pa.Table]:
    """Book stock per store and SKU from the batch answer and from each tier: (store_id, sku, level)."""
    cols = ("store_id", "sku", "level")

    def duck(lines: pa.Table | None, c: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
        c = c or duckdb.connect()
        if lines is not None:
            c.register("lines_in", lines)
            c.execute("CREATE OR REPLACE TEMP VIEW stock_lines AS SELECT * FROM lines_in")
        c.execute(f"SET VARIABLE opening_file = '{p29.OPENING}'")
        c.execute(f"SET VARIABLE day = '{DAY}'")
        return c.sql((ROOT / "sql/part30/stock_levels.sql").read_text().rstrip().rstrip(";")).to_arrow_table()

    batch = replayed(limit)
    batch.execute("CREATE OR REPLACE TEMP VIEW stock_lines AS SELECT * FROM replay_lines")
    cat = p27.catalog()
    hours = cat.load_table(LEVEL_1H).scan(selected_fields=cols).to_arrow()
    minutes = cat.load_table(LINES_1M).scan(row_filter=f"business_date = '{DAY}'").to_arrow()
    sql = (ROOT / "sql/part30/clickhouse/stock_levels.sql").read_text().rstrip().rstrip(";")
    seconds = pa.Table.from_pylist([dict(zip(cols, r, strict=True)) for r in ch_rows(sql)])
    return {"batch": duck(None, batch), "hours": hours, "minutes": duck(minutes), "seconds": seconds}


def row_check(lv: dict[str, pa.Table]) -> dict:
    """Rows in a tier and not in the batch answer, and the other way round (EXCEPT both ways): store, SKU, level."""
    c = duckdb.connect()
    for name, t in lv.items():
        c.register(name, t)
    q = (
        "SELECT count(*) FROM (SELECT store_id, sku, level::BIGINT FROM {a} "
        "EXCEPT SELECT store_id, sku, level::BIGINT FROM {b})"
    )
    return {t: {"rows": lv[t].num_rows, "not_in_batch": c.execute(q.format(a=t, b="batch")).fetchone()[0],
                "missing_from_tier": c.execute(q.format(a="batch", b=t)).fetchone()[0]}
            for t in ("hours", "minutes", "seconds")} | {"batch_rows": lv["batch"].num_rows}  # fmt: skip


def check(limit: int | None = None) -> dict:
    limit = load("live")["limit"] if limit is None else limit
    exp = expected(limit)
    got = {"hours": level_metric()[0], "minutes": lake_metric(LINES_1M)[0], "seconds": ch_metric()[0]}
    keys = ("store_skus", "at_or_below_zero", "availability_pct", "units_sold")
    res = {
        "expected": {k: exp[k] for k in keys},
        "tiers": got,
        "equal": {t: all(float(g[k]) == float(exp[k]) for k in keys) for t, g in got.items()},
        "lines": {
            "expected": exp["lines"],
            "minutes": table_rows(LINES_1M),
            "hours": table_rows(LINES_1H),
            "seconds_final": ch_rows("SELECT count() FROM realtime.sale_lines FINAL")[0][0],
        },
        "sold_outside_opening": ch_rows(
            "SELECT count() FROM (SELECT DISTINCT store_id, sku FROM realtime.sale_lines WHERE business_date = "
            f"'{DAY}') s LEFT ANTI JOIN realtime.opening_stock o USING (store_id, sku)"
        )[0][0],
        "row_level": row_check(levels(limit)),
    }
    return res


def cmd_check(_: argparse.Namespace) -> None:
    r = check()
    e = r["expected"]
    print(
        f"batch answer from the raw zone: {e['store_skus']:,} store-SKUs, {e['at_or_below_zero']:,} at or below zero, "
        f"availability {e['availability_pct']}%, {e['units_sold']:,} units sold"
    )
    for t, ok in r["equal"].items():
        print(f"  {t:<8} equal: {ok}  {r['tiers'][t]}")
    print(
        f"lines: expected {r['lines']['expected']:,}; minutes {r['lines']['minutes']:,}, "
        f"hours {r['lines']['hours']:,}, "
        f"seconds after FINAL {r['lines']['seconds_final']:,}; store-SKUs sold outside the opening file: "
        f"{r['sold_outside_opening']}"
    )
    rl = r["row_level"]
    print(f"row by row (store, SKU, level) against the batch's {rl['batch_rows']:,} rows, EXCEPT both ways:")
    for t in ("hours", "minutes", "seconds"):
        x = rl[t]
        print(f"  {t:<8} {x['rows']:,} rows; in the tier, not the batch: {x['not_in_batch']}; "
              f"in the batch, not the tier: {x['missing_from_tier']}")  # fmt: skip


# ---------- compaction ----------


def compact() -> dict:
    res = {t: lake_stats(t) for t in (LINES_1M, LINES_1M_C)}
    t = p27.catalog().load_table(LINES_1M_C)
    res["rewrites"] = [
        {
            k: s.summary.additional_properties.get(k)
            for k in ("added-data-files", "deleted-data-files", "total-data-files")
        }
        | {"at": utc(s.timestamp_ms)}
        for s in t.snapshots()
        if s.summary.operation.value == "replace"
    ]
    save("compact", res)
    return res


def cmd_compact(_: argparse.Namespace) -> None:
    r = compact()
    for t in (LINES_1M, LINES_1M_C):
        s = r[t]
        print(
            f"{t:<34} commits {s['commits']:>3} {s['operations']}, live data files {s['live_data_files']:>3} "
            f"({s['live_bytes'] / 1024:,.0f} KiB), objects in the folder {sum(s['objects'].values())} "
            f"({sum(s['object_bytes'].values()) / 1024:,.0f} KiB), rows {s['rows']:,}"
        )
    for w in r["rewrites"]:
        print(
            f"  rewrite at {w['at']}: {w['deleted-data-files']} files -> {w['added-data-files']}, "
            f"live data files after: {w['total-data-files']}"
        )


# ---------- cleanup ----------


def cmd_cleanup(_: argparse.Namespace) -> None:
    p29.cancel_jobs()
    ch("DROP DATABASE IF EXISTS realtime SYNC", fmt="")
    drop_tables()
    p27.kafka("--delete", "--if-exists", "--topic", TOPIC)
    p27._wait_deleted([TOPIC])
    for g in GROUPS:
        p27.kafka("--delete", "--group", g, tool="kafka-consumer-groups.sh", check=False)
    p27.http("DELETE", f"{p27.REGISTRY}/registry/v3/groups/default/artifacts/{TOPIC}-value")
    print(f"dropped: {', '.join(TABLES)}, topic {TOPIC}, its consumer groups and schema, ClickHouse database realtime")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup").set_defaults(fn=cmd_setup)
    sub.add_parser("clock").set_defaults(fn=cmd_clock)
    lv = sub.add_parser("live")
    lv.add_argument("--speed", type=float, default=24.0, help="replay speed: 24 plays the upload day in an hour")
    lv.add_argument("--interval", default="60 s", help="the minute tier's checkpoint (commit) interval")
    lv.add_argument("--hourly-every", type=float, default=3600.0, help="seconds between hourly batch runs")
    lv.add_argument("--limit", type=int, default=0, help="replay only the first N events")
    lv.add_argument("--compacted", action="store_true", help="also run the minute tier with compaction")
    lv.set_defaults(fn=cmd_live)
    for name, fn in (
        ("freshness", cmd_freshness),
        ("query", cmd_query),
        ("check", cmd_check),
        ("compact", cmd_compact),
        ("cleanup", cmd_cleanup),
    ):
        sub.add_parser(name).set_defaults(fn=fn)
    co = sub.add_parser("cost")
    co.add_argument("--saved", action="store_true", help="print the saved measurement (data/lab/part30/cost.json)")
    co.set_defaults(fn=cmd_cost)
    hr = sub.add_parser("hourly")
    hr.add_argument("--json", action="store_true", help="print the run's measurements as one JSON line")
    hr.set_defaults(fn=cmd_hourly)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
