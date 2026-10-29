"""Cold-chain telemetry: chilled-compartment temperature readings from Larkspur's delivery vans (Season 3).

Each van's sensor reads the compartment every 30 seconds and publishes over MQTT; a bridge stamps each reading with
its ingestion time and forwards it to Kafka. `event_ts` is the sensor's clock, `ingest_ts` the bridge's, both UTC.
The readings come out in arrival order, as a consumer of the topic would see them.

Defects, deterministic for the seed:
- network jitter: readings arrive 1-4 seconds after they were taken, a few up to 45 seconds (out of order);
- a dead zone: VAN-014 unloads in an underground loading bay from 10:13 with no signal; the sensor buffers its
  readings and uploads them in one burst when the van drives out at 10:24:05. The compartment warmed up while the
  doors were open, so the burst carries the only evidence of the excursion.
"""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

SEED = 42
DAY = datetime(2026, 10, 14)  # a Wednesday; all times UTC (10:00 UTC is 11:00 in Manchester)
START = DAY.replace(hour=10)
END = DAY.replace(hour=10, minute=45)  # exclusive
INTERVAL = timedelta(seconds=30)
VANS = ("VAN-011", "VAN-014", "VAN-019", "VAN-023")
DEAD_ZONE_VAN = "VAN-014"
DEAD_ZONE_FROM = DAY.replace(hour=10, minute=13)  # doors open, refrigeration unit off with the engine
DOORS_CLOSED = DAY.replace(hour=10, minute=21)  # engine on, refrigeration unit back on
SIGNAL_BACK = DAY.replace(hour=10, minute=24, second=5)  # leaves the bay; the buffer uploads
WARMING_PER_MIN = 0.7  # °C per minute with the doors open
COOLING_PER_MIN = 0.8  # °C per minute once the unit runs again


@dataclass(frozen=True)
class Reading:
    van_id: str
    sensor_id: str
    seq: int
    temp_c: Decimal  # one decimal place, as the sensor reports it
    event_ts: datetime  # when the sensor took the reading (device clock, UTC)
    ingest_ts: datetime  # when the MQTT bridge received it (UTC)

    def as_json(self) -> dict:
        return {
            "van_id": self.van_id,
            "sensor_id": self.sensor_id,
            "seq": self.seq,
            "temp_c": float(self.temp_c),
            "event_ts": self.event_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "ingest_ts": self.ingest_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        }


def _true_temp(van: str, setpoint: float, t: datetime) -> float:
    """The compartment's temperature at t: steady at the setpoint, except VAN-014's open doors in the loading bay."""
    if van != DEAD_ZONE_VAN or t < DEAD_ZONE_FROM:
        return setpoint
    peak = setpoint + WARMING_PER_MIN * (DOORS_CLOSED - DEAD_ZONE_FROM).seconds / 60
    if t < DOORS_CLOSED:
        return setpoint + WARMING_PER_MIN * (t - DEAD_ZONE_FROM).seconds / 60
    return max(setpoint, peak - COOLING_PER_MIN * (t - DOORS_CLOSED).seconds / 60)


def readings(seed: int = SEED) -> list[Reading]:
    """Every reading of the 45 minutes, in arrival (ingest) order."""
    rng = random.Random(seed)
    out: list[Reading] = []
    for i, van in enumerate(VANS):
        setpoint = 3.0 + 0.2 * i
        offset = timedelta(seconds=0 if van == DEAD_ZONE_VAN else 7 * (i + 1))  # vans don't read in lockstep
        t, seq = START + offset, 0
        while t < END:
            seq += 1
            temp = Decimal(str(round(_true_temp(van, setpoint, t) + rng.choice((-0.1, 0.0, 0.0, 0.1)), 1)))
            if van == DEAD_ZONE_VAN and DEAD_ZONE_FROM <= t < SIGNAL_BACK:
                # the buffer drains in sensor order, 100 ms apart
                ingest = SIGNAL_BACK + timedelta(milliseconds=100 * ((t - DEAD_ZONE_FROM) // INTERVAL))
            elif rng.random() < 0.03:
                ingest = t + timedelta(seconds=rng.randint(20, 45))  # a slow cell handover
            else:
                ingest = t + timedelta(milliseconds=rng.randint(1000, 4000))
            out.append(Reading(van, f"{van}-C1", seq, temp, t, ingest))
            t += INTERVAL
    return sorted(out, key=lambda r: (r.ingest_ts, r.van_id, r.seq))


def buffered(r: Reading) -> bool:
    return r.van_id == DEAD_ZONE_VAN and DEAD_ZONE_FROM <= r.event_ts < SIGNAL_BACK


def summary(rs: list[Reading] | None = None) -> dict:
    """Counts for docs/injected-defects.md."""
    rs = readings() if rs is None else rs
    max_seen, out_of_order = datetime.min, 0
    for r in rs:
        out_of_order += r.event_ts < max_seen and not buffered(r)
        max_seen = max(max_seen, r.event_ts)
    return {
        "readings": len(rs),
        "vans": len(VANS),
        "out_of_order_jitter": out_of_order,  # arrived behind a newer reading, not counting the dead zone's burst
        "max_jitter_delay_s": max((r.ingest_ts - r.event_ts).total_seconds() for r in rs if not buffered(r)),
        "buffered_in_dead_zone": sum(buffered(r) for r in rs),
        "max_delay_s": max((r.ingest_ts - r.event_ts).total_seconds() for r in rs),
        "max_temp_c": str(max(r.temp_c for r in rs)),
    }
