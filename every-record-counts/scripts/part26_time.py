"""Part 26: one stream, two clocks. Windowed averages of cold-chain readings, simulated, then in Flink SQL.

    python -m scripts.part26_time generate   # the readings, in arrival order, to data/lab/part26/readings.jsonl
    python -m scripts.part26_time windows    # VAN-014's 5-minute averages under five strategies
    python -m scripts.part26_time late       # what the watermark did with the late burst: dropped, updated, side output
    python -m scripts.part26_time crash      # a crash mid-burst under three recovery models
    python -m scripts.part26_time kafka      # (make up-flink) the readings into topic coldchain.readings
    python -m scripts.part26_time flink      # (make up-flink) the same windows in Flink SQL, checked against the above

The simulation is a small event-time window operator written to match Flink's rules: a reading is dropped when
its window's end plus the allowed lateness is at or below the watermark; a window fires when the watermark reaches
its end, and again for every late reading it still accepts; the watermark is the largest event time seen minus the
bound, updated after each reading. Event time is the sensor's clock; processing time here is the bridge's
ingestion time, so every run gives the same answer.
"""

import argparse
import json
import re
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from datagen import coldchain
from datagen.coldchain import Reading

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/lab/part26/readings.jsonl"
TOPIC = "coldchain.readings"
SIZE = timedelta(minutes=5)
LIMIT_C = Decimal("5.0")  # Larkspur's rule: a 5-minute average above 5.0 °C is an excursion
VAN = coldchain.DEAD_ZONE_VAN
SHOWN = [coldchain.DAY.replace(hour=10, minute=m) for m in (10, 15, 20, 25)]
END_OF_INPUT = datetime.max
COMPOSE = ["docker", "compose", "--profile", "core", "--profile", "stream"]

Key = tuple[str, datetime]  # (van, window start)


def window_of(t: datetime) -> datetime:
    return t - timedelta(minutes=t.minute % 5, seconds=t.second, microseconds=t.microsecond)


def avg(total: Decimal, n: int) -> Decimal:
    return (total / n).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass
class Firing:
    at: datetime  # ingestion time of the reading that caused it (processing time)
    key: Key
    avg: Decimal
    n: int
    late: bool  # a late firing: an update to a window that had already fired


@dataclass
class Run:
    """The result of one pass over the stream."""

    firings: list[Firing] = field(default_factory=list)
    side_output: list[Reading] = field(default_factory=list)
    peak_open_windows: int = 0  # most (van, window) aggregates held in state at once

    def final(self) -> dict[Key, Firing]:
        """The last firing per window: what an upsert sink ends up holding."""
        return {f.key: f for f in self.firings}


class EventTimeWindows:
    """Tumbling event-time windows with a bounded-out-of-orderness watermark and allowed lateness."""

    def __init__(self, bound: timedelta, lateness: timedelta = timedelta(0)):
        self.bound, self.lateness = bound, lateness
        self.watermark = datetime.min
        self.state: dict[Key, list] = {}  # key -> [sum, count]
        self.fired: set[Key] = set()
        self.run = Run()

    def snapshot(self) -> tuple:
        return self.watermark, {k: list(v) for k, v in self.state.items()}, set(self.fired)

    def restore(self, snap: tuple) -> None:
        self.watermark, state, fired = snap
        self.state, self.fired = {k: list(v) for k, v in state.items()}, set(fired)

    def _emit(self, at: datetime, key: Key, late: bool) -> None:
        total, n = self.state[key]
        self.run.firings.append(Firing(at, key, avg(total, n), n, late))

    def process(self, r: Reading) -> None:
        key = (r.van_id, window_of(r.event_ts))
        end = key[1] + SIZE
        if end + self.lateness <= self.watermark:
            self.run.side_output.append(r)  # too late even for allowed lateness: dropped (or sent aside)
        else:
            pane = self.state.setdefault(key, [Decimal(0), 0])
            pane[0] += r.temp_c
            pane[1] += 1
            if end <= self.watermark:
                self._emit(r.ingest_ts, key, late=True)  # the window already fired: fire again with the update
        self.run.peak_open_windows = max(self.run.peak_open_windows, len(self.state))
        self.advance(max(self.watermark, r.event_ts - self.bound), r.ingest_ts)

    def advance(self, watermark: datetime, at: datetime) -> None:
        self.watermark = watermark
        for key in sorted(self.state, key=lambda k: (k[1], k[0])):
            end = key[1] + SIZE
            if end <= watermark and key not in self.fired:
                self.fired.add(key)
                self._emit(at, key, late=False)
            if watermark == END_OF_INPUT or end + self.lateness <= watermark:
                del self.state[key]  # past allowed lateness: the state is cleared


def event_time(rs: list[Reading], bound: timedelta, lateness: timedelta = timedelta(0)) -> Run:
    op = EventTimeWindows(bound, lateness)
    for r in rs:
        op.process(r)
    op.advance(END_OF_INPUT, rs[-1].ingest_ts)  # a bounded replay ends: every window fires
    return op.run


def all_data(rs: list[Reading]) -> dict[Key, tuple[Decimal, int]]:
    """Event-time windows computed after every reading has arrived: the answer the stream should converge to."""
    acc: dict[Key, list] = defaultdict(lambda: [Decimal(0), 0])
    for r in rs:
        acc[(r.van_id, window_of(r.event_ts))][0] += r.temp_c
        acc[(r.van_id, window_of(r.event_ts))][1] += 1
    return {k: (avg(s, n), n) for k, (s, n) in acc.items()}


def processing_time(rs: list[Reading]) -> dict[Key, tuple[Decimal, int]]:
    """Windows by arrival time: what a job that groups by 'now' computes."""
    acc: dict[Key, list] = defaultdict(lambda: [Decimal(0), 0])
    for r in rs:
        acc[(r.van_id, window_of(r.ingest_ts))][0] += r.temp_c
        acc[(r.van_id, window_of(r.ingest_ts))][1] += 1
    return {k: (avg(s, n), n) for k, (s, n) in acc.items()}


def decided_at(firings: list[Firing]) -> datetime | None:
    """When two consecutive excursion windows were first both visible to the sink (the quarantine rule)."""
    latest: dict[Key, Firing] = {}
    for f in firings:
        latest[f.key] = f
        if f.key[0] != VAN:
            continue
        flagged = sorted(k[1] for k, g in latest.items() if k[0] == VAN and g.avg > LIMIT_C)
        if any(b - a == SIZE for a, b in zip(flagged, flagged[1:], strict=False)):
            return f.at
    return None


def readings() -> list[Reading]:
    return coldchain.readings()


def strategies(rs: list[Reading]) -> dict[str, tuple[dict[Key, tuple[Decimal, int]], datetime | None, Run | None]]:
    """name -> (final value per window, when the quarantine was decided, the run)."""
    out = {}
    proc = processing_time(rs)
    flagged = sorted(k[1] for k, (a, _) in proc.items() if k[0] == VAN and a > LIMIT_C)
    pair = any(b - a == SIZE for a, b in zip(flagged, flagged[1:], strict=False))
    out["processing time (arrival)"] = (proc, None if not pair else flagged[-1] + SIZE, None)
    out["event time, all data"] = (all_data(rs), None, None)
    for name, bound, lateness in (
        ("watermark 2 min, late dropped", timedelta(minutes=2), timedelta(0)),
        ("watermark 2 min + 10 min lateness", timedelta(minutes=2), timedelta(minutes=10)),
        ("watermark 10 min", timedelta(minutes=10), timedelta(0)),
    ):
        run = event_time(rs, bound, lateness)
        out[name] = ({k: (f.avg, f.n) for k, f in run.final().items()}, decided_at(run.firings), run)
    return out


def cell(values: dict[Key, tuple[Decimal, int]], start: datetime) -> str:
    if (VAN, start) not in values:
        return "—"
    a, n = values[(VAN, start)]
    return f"{a}{'*' if a > LIMIT_C else ' '} ({n})"


def quarantined(values: dict[Key, tuple[Decimal, int]]) -> bool:
    flagged = sorted(k[1] for k, (a, _) in values.items() if k[0] == VAN and a > LIMIT_C)
    return any(b - a == SIZE for a, b in zip(flagged, flagged[1:], strict=False))


def write_jsonl(rs: list[Reading]) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("".join(json.dumps(r.as_json()) + "\n" for r in rs))


def cmd_generate(_: argparse.Namespace) -> None:
    rs = readings()
    write_jsonl(rs)
    s = coldchain.summary(rs)
    where = OUT.relative_to(ROOT)
    print(f"{s['readings']} readings from {s['vans']} vans, 10:00-10:45 UTC, written in arrival order to {where}")
    print(
        f"out of order by jitter: {s['out_of_order_jitter']} (delays up to {s['max_jitter_delay_s']:.0f} s); "
        f"buffered in {VAN}'s dead zone: {s['buffered_in_dead_zone']} (up to {s['max_delay_s']:.0f} s late)"
    )
    print("\nArrival order around 10:24 UTC (event_ts = sensor clock, ingest_ts = bridge clock):")
    print(f"{'van':8} {'seq':>3}  {'event_ts':8}  {'ingest_ts':12}  temp_c")
    lo, hi = coldchain.DAY.replace(hour=10, minute=23, second=55), coldchain.DAY.replace(hour=10, minute=24, second=11)
    for r in rs:
        if lo <= r.ingest_ts < hi:
            print(f"{r.van_id:8} {r.seq:>3}  {r.event_ts:%H:%M:%S}  {r.ingest_ts:%H:%M:%S.%f}"[:-3] + f"  {r.temp_c}")


def cmd_windows(_: argparse.Namespace) -> None:
    rs = readings()
    print(f"{VAN}, 5-minute windows by start (UTC): average °C (readings); * = above {LIMIT_C} °C")
    head = "".join(f"{w:%H:%M}".ljust(14) for w in SHOWN)
    print(f"{'strategy':36}{head}quarantine  decided at")
    for name, (values, at, _) in strategies(rs).items():
        q = "yes" if quarantined(values) else "no"
        when = f"{at:%H:%M:%S}" if at else "-"
        print(f"{name:36}{''.join(cell(values, w).ljust(14) for w in SHOWN)}{q:12}{when}")


def cmd_late(_: argparse.Namespace) -> None:
    rs = readings()
    print(f"{'strategy':36}{'side output':>12}{'late firings':>14}{'rows to sink':>14}{'open windows':>14}")
    for name, (_, _, run) in strategies(rs).items():
        if run is None:
            continue
        late = sum(f.late for f in run.firings)
        print(f"{name:36}{len(run.side_output):>12}{late:>14}{len(run.firings):>14}{run.peak_open_windows:>14}")
    run = strategies(rs)["watermark 2 min, late dropped"][2]
    first, last = run.side_output[0], run.side_output[-1]
    print(
        f"\nwatermark 2 min: side output = {VAN} readings {first.event_ts:%H:%M:%S}-{last.event_ts:%H:%M:%S}, "
        f"all arriving {first.ingest_ts:%H:%M:%S}-{last.ingest_ts:%H:%M:%S}"
    )
    print(f"{VAN}'s firings with 10 min lateness (window: when, average, readings, kind):")
    run = strategies(rs)["watermark 2 min + 10 min lateness"][2]
    rows = [f for f in run.firings if f.key[0] == VAN and f.key[1] in SHOWN[:3]]
    for f in rows[:4] + ([None] if len(rows) > 6 else []) + rows[-2:]:
        if f is None:
            print(f"  ... {len(rows) - 6} more late firings ...")
            continue
        print(f"  {f.key[1]:%H:%M}  {f.at:%H:%M:%S.%f}"[:-3] + f"  {f.avg}  ({f.n})  {'late' if f.late else 'on time'}")


def crash_run(rs: list[Reading], mode: str, crash_after: int, checkpoints: list[int]) -> Run:
    """Process rs, crash after reading index crash_after, recover according to mode, finish.

    Checkpoint i covers readings[:checkpoints[i]]. Modes:
    - at-most-once: offsets are committed as readings are polled; window state lives only in memory.
    - at-least-once: a hand-rolled consumer that writes window state to an external store as each reading is
      applied and commits offsets at each checkpoint, so a restart re-reads from the last commit onto the
      already-updated state. (Not Flink's AT_LEAST_ONCE checkpointing mode, which restores state too.)
    - exactly-once state: state and offset are snapshotted together at each checkpoint; a restart restores both.
    """
    bound, lateness = timedelta(minutes=2), timedelta(minutes=10)
    op = EventTimeWindows(bound, lateness)
    snap, committed = op.snapshot(), 0
    for i, r in enumerate(rs[: crash_after + 1]):
        op.process(r)
        if i + 1 in checkpoints:
            snap, committed = op.snapshot(), i + 1
    sink = op.run
    if mode == "at-most-once":
        op.restore((datetime.min, {}, set()))  # panes, watermark and timers lived in memory: gone
        resume = crash_after + 1
    elif mode == "at-least-once":
        resume = committed  # state as of the crash, offsets as of the last commit
    else:
        op.restore(snap)
        resume = committed
    for r in rs[resume:]:
        op.process(r)
    op.advance(END_OF_INPUT, rs[-1].ingest_ts)
    return sink


def cmd_crash(_: argparse.Namespace) -> None:
    rs = readings()
    # Checkpoint at every whole minute of ingestion time; crash after the 12th reading of the burst.
    checkpoints = [i for i in range(1, len(rs)) if rs[i - 1].ingest_ts.minute != rs[i].ingest_ts.minute]
    burst = [i for i, r in enumerate(rs) if r.ingest_ts >= coldchain.SIGNAL_BACK and r.van_id == VAN]
    crash_after = burst[11]
    last_cp = max(c for c in checkpoints if c <= crash_after)
    print(
        f"checkpoint every minute; last one before the crash after reading {last_cp} (ingested "
        f"{rs[last_cp - 1].ingest_ts:%H:%M:%S}); crash after reading {crash_after + 1} "
        f"({rs[crash_after].ingest_ts:%H:%M:%S.%f}"[:-3]
        + f", 12 readings into {VAN}'s burst)"
    )
    truth = all_data(rs)
    clean = event_time(rs, timedelta(minutes=2), timedelta(minutes=10))
    print("final value per window = the last row the sink got for it (what an upsert sink holds)")
    print(f"\n{'recovery model':20}{'10:10':>14}{'10:15':>14}{'10:20':>14}{'equals no-crash':>17}{'rows to sink':>14}")
    replayed = None
    for mode in ("at-most-once", "at-least-once", "exactly-once state"):
        run = crash_run(rs, mode, crash_after, checkpoints)
        final = {k: (f.avg, f.n) for k, f in run.final().items()}
        same = all(final.get(k) == v for k, v in truth.items())
        print(
            f"{mode:20}" + "".join(cell(final, w).rjust(14) for w in SHOWN[:3]) + f"{str(same):>17}"
            f"{len(run.firings):>14}"
        )
        replayed = run
    cells = "".join(cell(truth, w).rjust(14) for w in SHOWN[:3])
    print(f"{'no crash':20}{cells}{'':>17}{len(clean.firings):>14}")
    rows = [str(f.avg) for f in replayed.firings if f.key == (VAN, SHOWN[0])]
    print(f"\nexactly-once state: rows the sink got for {VAN} 10:10, in order: {', '.join(rows)}")


def cmd_kafka(_: argparse.Namespace) -> None:
    write_jsonl(readings())  # always from the generator, so the topic and the simulation see the same readings
    kt = [*COMPOSE, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092"]
    subprocess.run([*kt, "--delete", "--if-exists", "--topic", TOPIC], check=True, capture_output=True)
    for _ in range(30):  # deletion is asynchronous
        listed = subprocess.run([*kt, "--list"], check=True, capture_output=True, text=True).stdout.split()
        if TOPIC not in listed:
            break
        subprocess.run(["sleep", "1"])
    # One partition: the readings stay in arrival order, and the watermark is the whole fleet's (see the post).
    subprocess.run(
        [*kt, "--create", "--topic", TOPIC, "--partitions", "1", "--config", "retention.ms=604800000"],
        check=True,
        capture_output=True,
    )
    prod = [*COMPOSE, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh"]
    with OUT.open("rb") as f:
        subprocess.run([*prod, "--bootstrap-server", "kafka:29092", "--topic", TOPIC], stdin=f, check=True)
    offsets = subprocess.run(
        [
            *COMPOSE,
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            TOPIC,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    print(f"topic {TOPIC}: 1 partition, end offset {offsets.split(':')[-1]} (partition:offset = {offsets})")


ROW = re.compile(r"^\|\s*\+I\s*\|\s*(\S+)\s*\|\s*(\S+ \S+)\s*\|\s*(\d+)\s*\|\s*([\d.]+)\s*\|")
SQL = "part26/windows.sql"


def flink_sql(script: str = SQL) -> tuple[list[str], list[dict[Key, tuple[Decimal, int]]]]:
    """Run a script with the SQL client in the JobManager; return each query's result table and parsed rows."""
    out = subprocess.run(
        [*COMPOSE, "exec", "-T", "flink-jobmanager", "./bin/sql-client.sh", "-f", f"/opt/flink/lab/sql/{script}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=600,
    ).stdout
    tables, results = [], []
    for chunk in out.split("Received a total of")[:-1]:
        lines = chunk[chunk.index("+----") :].splitlines()
        tables.append("\n".join(line for line in lines if line.startswith(("+-", "|"))))
        rows = {}
        for line in lines:
            m = ROW.match(line)
            if m:
                start = datetime.strptime(m.group(2)[:19], "%Y-%m-%d %H:%M:%S")
                rows[(m.group(1), start)] = (Decimal(m.group(4)), int(m.group(3)))
        results.append(rows)
    return tables, results


def cmd_flink(_: argparse.Namespace) -> None:
    sim = strategies(readings())
    tables, results = flink_sql()
    for table, rows, name in zip(tables, results, ("watermark 2 min, late dropped", "watermark 10 min"), strict=True):
        expected = {k: v for k, v in sim[name][0].items() if k[0] == VAN and k[1] in SHOWN}
        print(f"-- {name}\n{table}")
        print(f"same as the simulation's '{name}': {rows == expected}\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("generate", "windows", "late", "crash", "kafka", "flink"):
        sub.add_parser(name)
    a = p.parse_args()
    {
        "generate": cmd_generate,
        "windows": cmd_windows,
        "late": cmd_late,
        "crash": cmd_crash,
        "kafka": cmd_kafka,
        "flink": cmd_flink,
    }[a.cmd](a)


if __name__ == "__main__":
    main()
