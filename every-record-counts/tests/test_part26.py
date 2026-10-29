"""Part 26: event time vs processing time, watermarks, lateness and recovery, on the cold-chain readings."""

import socket
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from datagen import coldchain
from scripts import part26_time as p26

VAN = coldchain.DEAD_ZONE_VAN
T = coldchain.DAY.replace(hour=10)


def at(minute: int, second: int = 0) -> datetime:
    return T.replace(minute=minute, second=second)


@pytest.fixture(scope="module")
def rs():
    return coldchain.readings()


@pytest.fixture(scope="module")
def strategies(rs):
    return p26.strategies(rs)


def test_generator_is_deterministic_and_documented(rs):
    assert [r.as_json() for r in rs] == [r.as_json() for r in coldchain.readings()]
    assert coldchain.summary(rs) == {
        "readings": 360,
        "vans": 4,
        "out_of_order_jitter": 13,
        "max_jitter_delay_s": 45.0,
        "buffered_in_dead_zone": 23,
        "max_delay_s": 665.0,
        "max_temp_c": "8.9",
    }
    assert [r.ingest_ts for r in rs] == sorted(r.ingest_ts for r in rs)  # arrival order


def test_processing_time_puts_the_excursion_in_the_wrong_window(strategies):
    proc, truth = strategies["processing time (arrival)"][0], strategies["event time, all data"][0]
    assert (VAN, at(15)) not in proc  # nothing arrived from the van between 10:15 and 10:20
    assert proc[(VAN, at(20))] == (Decimal("6.36"), 24)  # the whole burst lands in the window it arrived in
    assert truth[(VAN, at(15))] == (Decimal("6.17"), 10)
    assert truth[(VAN, at(20))] == (Decimal("7.59"), 10)
    assert not p26.quarantined(proc) and p26.quarantined(truth)


def test_two_minute_watermark_drops_the_burst_behind_it(strategies):
    values, decided, run = strategies["watermark 2 min, late dropped"]
    assert [(r.van_id, r.event_ts) for r in run.side_output] == [
        (VAN, at(13) + timedelta(seconds=30 * i)) for i in range(14)
    ]  # 10:13:00-10:19:30: windows the watermark (driven by the other vans) had already closed
    assert values[(VAN, at(10))] == (Decimal("3.17"), 6) and (VAN, at(15)) not in values
    assert decided is None and not p26.quarantined(values)
    assert (len(run.firings), run.peak_open_windows, sum(f.late for f in run.firings)) == (35, 8, 0)
    # Out-of-order arrivals inside the 2-minute bound cost nothing: every other van's windows are exact.
    truth = strategies["event time, all data"][0]
    assert all(values[k] == v for k, v in truth.items() if k[0] != VAN)


def test_allowed_lateness_and_a_longer_watermark_both_converge_to_event_time(strategies):
    truth = strategies["event time, all data"][0]
    late_ok, decided_late, run_late = strategies["watermark 2 min + 10 min lateness"]
    wm10, decided_wm10, run_wm10 = strategies["watermark 10 min"]
    assert late_ok == truth and wm10 == truth
    assert sum(f.late for f in run_late.firings) == 14 and not run_late.side_output
    assert (len(run_late.firings), len(run_wm10.firings)) == (50, 36)  # the price: updates the sink must absorb
    assert (run_late.peak_open_windows, run_wm10.peak_open_windows) == (16, 13)  # ... and state held longer
    first = [f for f in run_late.firings if f.key == (VAN, at(10))][:4]
    assert [(f.at, str(f.avg), f.late) for f in first] == [
        (datetime(2026, 10, 14, 10, 17, 9, 576000), "3.17", False),  # on time, from the other vans' watermark
        (coldchain.SIGNAL_BACK, "3.19", True),
        (coldchain.SIGNAL_BACK + timedelta(milliseconds=100), "3.24", True),
        (coldchain.SIGNAL_BACK + timedelta(milliseconds=200), "3.31", True),
    ]
    assert decided_late == datetime(2026, 10, 14, 10, 27, 2, 87000)
    assert decided_wm10 == datetime(2026, 10, 14, 10, 35, 2, 811000)


def test_only_a_consistent_snapshot_of_state_and_offsets_survives_a_crash(rs):
    checkpoints = [i for i in range(1, len(rs)) if rs[i - 1].ingest_ts.minute != rs[i].ingest_ts.minute]
    burst = [i for i, r in enumerate(rs) if r.ingest_ts >= coldchain.SIGNAL_BACK and r.van_id == VAN]
    truth = p26.all_data(rs)
    results = {}
    runs = {}
    for mode in ("at-most-once", "at-least-once", "exactly-once state"):
        runs[mode] = run = p26.crash_run(rs, mode, burst[11], checkpoints)
        results[mode] = ({k: (f.avg, f.n) for k, f in run.final().items()}, len(run.firings))
    assert results["exactly-once state"][0] == truth
    assert results["at-most-once"][0][(VAN, at(15))] == (Decimal("7.55"), 2)  # lost readings
    assert results["at-least-once"][0][(VAN, at(15))] == (Decimal("6.02"), 18)  # counted twice
    clean = p26.event_time(rs, timedelta(minutes=2), timedelta(minutes=10))
    assert [results[m][1] for m in results] == [49, 62, 63] and len(clean.firings) == 50
    assert results["exactly-once state"][1] - len(clean.firings) == 13  # rows an append-only sink receives twice
    replayed = [str(f.avg) for f in runs["exactly-once state"].firings if f.key == (VAN, at(10))]
    assert replayed == ["3.17", "3.19", "3.24", "3.31", "3.40", "3.19", "3.24", "3.31", "3.40"]  # goes backwards


def flink_up() -> bool:
    for port in (8081, 9092):
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return True


needs_flink = pytest.mark.skipif(not flink_up(), reason="needs `make up-flink` (Kafka + Flink, Part 26)")


@needs_flink
def test_flink_sql_reproduces_the_simulation(strategies):
    p26.cmd_kafka(None)
    _, (wm2, wm10) = p26.flink_sql()
    for rows, name in ((wm2, "watermark 2 min, late dropped"), (wm10, "watermark 10 min")):
        expected = {k: v for k, v in strategies[name][0].items() if k[0] == VAN and k[1] in p26.SHOWN}
        assert rows == expected
    assert wm10 == {k: v for k, v in strategies["event time, all data"][0].items() if k[0] == VAN and k[1] in p26.SHOWN}
