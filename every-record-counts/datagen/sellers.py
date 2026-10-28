"""Marketplace sellers: the records behind the mock sellers API (Part 15) and what changes between two pulls.

The API is someone else's system. It returns the current version of each seller, stamped with `updated_at`, and
four things happen between Larkspur's first and second pull that an incremental extract can get wrong:

- ordinary changes and new sellers, stamped correctly;
- changes written by an application node whose clock runs 10 minutes slow, so they are stamped *below* the
  high-water mark the first pull recorded, although they happened after it;
- a bulk fix run straight in the seller database that changes commission rates without touching `updated_at`;
- a seller that is hard-deleted and simply disappears from the API.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import numpy as np

from datagen import settings as s

N_SELLERS = 1200
FIRST_PULL = datetime(2026, 10, 1, 6, 0, tzinfo=UTC)
SECOND_PULL = datetime(2026, 10, 2, 6, 0, tzinfo=UTC)
CLOCK_SKEW = timedelta(minutes=10)
N_NEW, N_UPDATED, N_SKEWED, N_SILENT, N_DELETED = 24, 40, 3, 2, 1
STATUSES = ("active", "active", "active", "active", "suspended", "onboarding")
FIELDS = ("seller_id", "display_name", "country", "city", "category", "status", "commission_bps", "payout_currency")


@dataclass(frozen=True)
class Change:
    """One write to the seller database: `at` is when it happened, `record` what the API returns afterwards."""

    at: datetime
    seller_id: str
    record: dict | None  # None: hard-deleted
    kind: str  # initial, created, updated, skewed, silent, deleted


@dataclass
class Sellers:
    changes: list[Change]  # in the order they happened
    defects: dict[str, int]

    def as_of(self, t: datetime) -> list[dict]:
        """The API's view at time t: the latest version of each seller that still exists."""
        state: dict[str, dict | None] = {}
        for c in self.changes:
            if c.at > t:
                break
            state[c.seller_id] = c.record
        return [r for r in state.values() if r is not None]


def _ts(rng: np.random.Generator, start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=int(rng.integers(0, int((end - start).total_seconds()))))


def build(seed: int) -> Sellers:
    rng = np.random.default_rng([seed, 15])
    countries = list(s.COUNTRIES.values())
    categories = list(s.CATEGORIES)
    changes: list[Change] = []

    def new_record(i: int, created: datetime) -> dict:
        c = countries[int(rng.integers(0, len(countries)))]
        city = str(rng.choice(c.cities))
        return {
            "seller_id": f"MS-{i:05d}",
            "display_name": f"{city} Seller {i}",
            "country": c.code,
            "city": city,
            "category": str(rng.choice(categories)),
            "status": str(rng.choice(STATUSES)),
            "commission_bps": int(rng.integers(6, 19)) * 100,
            "payout_currency": c.currency,
            "created_at": created,
            "updated_at": created,
        }

    def change(rec: dict, at: datetime, kind: str, stamp: datetime | None) -> dict:
        r = dict(rec)
        if kind == "silent":
            r["commission_bps"] = r["commission_bps"] + 100  # bulk fix: updated_at untouched
        else:
            r["status"] = "suspended" if r["status"] == "active" else "active"
            r["updated_at"] = stamp
        return r

    # Sellers that existed before the first pull; some updated since they joined, five in the last ten minutes.
    current: dict[str, dict] = {}
    history_start = datetime(2024, 1, 1, tzinfo=UTC)
    for i in range(1, N_SELLERS + 1):
        rec = new_record(i, _ts(rng, history_start, FIRST_PULL - timedelta(days=1)))
        current[rec["seller_id"]] = rec
        changes.append(Change(rec["created_at"], rec["seller_id"], rec, "initial"))
    ids = sorted(current)
    for sid in rng.choice(ids, size=300, replace=False):
        at = _ts(rng, current[sid]["created_at"], FIRST_PULL - timedelta(minutes=10))
        current[sid] = change(current[sid], at, "updated", at)
        changes.append(Change(at, sid, current[sid], "initial"))
    for sid in rng.choice(ids, size=5, replace=False):
        at = _ts(rng, FIRST_PULL - timedelta(minutes=10), FIRST_PULL)
        current[sid] = change(current[sid], at, "updated", at)
        changes.append(Change(at, sid, current[sid], "initial"))

    # Between the pulls.
    day = (FIRST_PULL, SECOND_PULL - timedelta(minutes=5))
    picked = iter(rng.choice(ids, size=N_UPDATED + N_SKEWED + N_SILENT + N_DELETED, replace=False))
    for n in range(N_NEW):
        at = _ts(rng, *day)
        rec = new_record(N_SELLERS + n + 1, at)
        changes.append(Change(at, rec["seller_id"], rec, "created"))
    for _ in range(N_UPDATED):
        sid = str(next(picked))
        at = _ts(rng, *day)
        changes.append(Change(at, sid, change(current[sid], at, "updated", at), "updated"))
    for _ in range(N_SKEWED):  # just after the first pull, on the node whose clock is slow
        sid = str(next(picked))
        at = _ts(rng, FIRST_PULL, FIRST_PULL + timedelta(minutes=5))
        changes.append(Change(at, sid, change(current[sid], at, "skewed", at - CLOCK_SKEW), "skewed"))
    for _ in range(N_SILENT):
        sid = str(next(picked))
        changes.append(Change(_ts(rng, *day), sid, change(current[sid], FIRST_PULL, "silent", None), "silent"))
    for _ in range(N_DELETED):
        sid = str(next(picked))
        changes.append(Change(_ts(rng, *day), sid, None, "deleted"))

    changes.sort(key=lambda c: (c.at, c.seller_id))
    first = {r["seller_id"]: r for r in Sellers(changes, {}).as_of(FIRST_PULL)}
    hwm = max(r["updated_at"] for r in first.values())
    skewed = [c for c in changes if c.kind == "skewed"]
    assert all(c.record["updated_at"] < hwm for c in skewed), "skewed stamps must fall below the first pull's mark"
    defects = {
        "sellers_at_first_pull": len(first),
        "sellers_created_between_pulls": N_NEW,
        "sellers_updated_between_pulls": N_UPDATED,
        "sellers_updated_by_slow_clock": N_SKEWED,
        "sellers_updated_without_updated_at": N_SILENT,
        "sellers_hard_deleted": N_DELETED,
    }
    return Sellers(changes, defects)
