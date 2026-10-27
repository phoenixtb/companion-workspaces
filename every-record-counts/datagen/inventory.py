"""Daily stock snapshots per store and SKU, and stock adjustments (shrink).

Each store's warehouse management system (WMS) exports one snapshot per trading
day. Stores on the `close_23_30` schedule snapshot at 23:30 local, after the shop
closes. Stores on `after_delivery_06_00` snapshot at 06:00 the next morning,
*after* the overnight delivery has been put away, and label it with the trading
day just closed. `received_qty` is what arrived since the previous snapshot, so
the closing level for those stores is `on_hand_qty - received_qty`.
"""

from dataclasses import dataclass
from datetime import datetime, time, timedelta

import numpy as np
import polars as pl

from datagen import settings as s
from datagen.reference import Reference, utc_offset_seconds


@dataclass
class Inventory:
    snapshots: pl.DataFrame
    adjustments: pl.DataFrame
    metrics: dict[str, float]


def build(seed: int, ref: Reference) -> Inventory:
    rng = np.random.default_rng([seed, 5])
    stores = ref.stores
    fill = dict(zip(ref.suppliers["supplier_id"], ref.suppliers["base_fill_rate"], strict=True))
    product_fill_all = np.array([fill[x] for x in ref.products["supplier_id"].to_list()])
    store_ids, pidx = [], []
    for sid in stores["store_id"].to_list():
        r = ref.store_ranges[sid]
        store_ids.append(np.full(len(r), sid, dtype=object))
        pidx.append(r)
    store_col = np.concatenate(store_ids)
    pidx = np.concatenate(pidx)
    rank = np.concatenate([np.arange(len(ref.store_ranges[sid])) for sid in stores["store_id"].to_list()])
    n = len(pidx)
    product_fill = product_fill_all[pidx]
    demand = np.clip(9.0 * (rank + 1.0) ** -0.55 * rng.uniform(0.6, 1.4, n), 0.3, 14)
    rop = np.ceil(demand * 2.0).astype(np.int64)  # reorder point
    target = np.ceil(demand * 5.0).astype(np.int64)  # order-up-to level
    close = target.copy()

    days = [s.INVENTORY_START + timedelta(days=i) for i in range((s.END - s.INVENTORY_START).days + 1)]
    # Supply outages: in each country, some lines cannot be supplied at all for weeks
    # (a recall, a supplier dispute, a stuck container). Every store in the country runs dry.
    country = np.array([x[:2] for x in store_col])
    outage_start = np.full((len(s.COUNTRIES), s.N_PRODUCTS), 10_000)
    outage_end = np.zeros_like(outage_start)
    for ci in range(len(s.COUNTRIES)):
        hit = rng.random(s.N_PRODUCTS) < s.OUTAGE_PRODUCT_SHARE
        outage_start[ci, hit] = rng.integers(0, len(days) - 14, hit.sum())
        outage_end[ci, hit] = outage_start[ci, hit] + rng.integers(14, 32, hit.sum())
    country_idx = np.array([list(s.COUNTRIES).index(c) for c in country])
    o_start, o_end = outage_start[country_idx, pidx], outage_end[country_idx, pidx]
    closes, received, mornings, next_received = [], [], [], []
    adj_rows = []
    pending = np.zeros(n, dtype=np.int64)
    for day_i, _ in enumerate(days):
        # Overnight delivery of yesterday's order. A supplier shortfall means a short or missed delivery.
        outage = (day_i >= o_start) & (day_i < o_end)
        missed = (rng.random(n) > product_fill) | outage
        partial = rng.uniform(0.3, 0.9, n)
        delivered = np.where(missed, (pending * partial).astype(np.int64) * (rng.random(n) < 0.4) * ~outage, pending)
        morning = close + delivered
        sales = np.minimum(rng.poisson(demand), morning)
        loss = np.where(rng.random(n) < 0.004, rng.integers(1, 4, n), 0)
        loss = np.minimum(loss, morning - sales)
        close = morning - sales - loss
        closes.append(close)
        received.append(delivered)
        mornings.append(morning)
        adj_rows.append(loss)
        pending = np.where(close < rop, target - close, 0)
    # The snapshot after the last day needs the next morning's delivery too.
    last_delivery = np.where(rng.random(n) > product_fill, 0, pending)
    next_received = received[1:] + [last_delivery]

    schedule = dict(zip(stores["store_id"], stores["wms_snapshot_schedule"], strict=True))
    tz = dict(zip(stores["store_id"], stores["timezone"], strict=True))
    morning_store = np.array([schedule[x] == "after_delivery_06_00" for x in store_col])
    zones = sorted(set(tz.values()))
    zone_idx = np.array([zones.index(tz[x]) for x in store_col])
    local_s = np.where(morning_store, 30 * 3600, 23.5 * 3600).astype(np.int64)  # 06:00 next day, or 23:30
    frames = []
    skus = np.array([f"SKU-{i + 1:05d}" for i in range(s.N_PRODUCTS)])[pidx]
    for i, (d, c, nxt) in enumerate(zip(days, closes, next_received, strict=True)):
        on_hand = np.where(morning_store, c + nxt, c)
        recv = np.where(morning_store, nxt, received[i])
        off = np.array([utc_offset_seconds(d, z) for z in zones])[zone_idx]
        midnight = np.datetime64(datetime.combine(d, time(0)), "s")
        snap = midnight + (local_s - off).astype("timedelta64[s]")
        frames.append(
            pl.DataFrame(
                {
                    "business_date": d,
                    "snapshot_ts": pl.Series(snap.astype("datetime64[ms]")).dt.replace_time_zone("UTC"),
                    "store_id": store_col.tolist(),
                    "sku": skus,
                    "on_hand_qty": on_hand,
                    "received_qty": recv,
                    "close_qty": c,  # dropped before writing; kept for metrics
                }
            )
        )
    snaps = pl.concat(frames)

    adjustments = []
    reasons = np.where(ref.products["is_chilled"].to_numpy()[pidx], "expired", "unknown_loss")
    for d, loss in zip(days, adj_rows, strict=True):
        hit = np.flatnonzero(loss > 0)
        if len(hit):
            adjustments.append(
                pl.DataFrame(
                    {
                        "business_date": d,
                        "store_id": store_col[hit].tolist(),
                        "sku": skus[hit],
                        "qty_change": -loss[hit],
                        "reason": np.where(rng.random(len(hit)) < 0.3, "damaged", reasons[hit]),
                    }
                )
            )
    adj = pl.concat(adjustments)
    metrics = availability_metrics(snaps, stores)
    return Inventory(snaps.drop("close_qty"), adj, metrics)


def availability_metrics(snaps: pl.DataFrame, stores: pl.DataFrame) -> dict[str, float]:
    """Q3 stock availability three ways; see docs/injected-defects.md."""
    q3 = snaps.filter(pl.col("business_date").is_between(s.Q3_START, s.Q3_END))
    sched = stores.select("store_id", "wms_snapshot_schedule")
    q3 = q3.join(sched, on="store_id")
    correct = (q3["close_qty"] > 0).mean()
    per_day_reported = (q3["on_hand_qty"] > 0).mean()
    weekly = q3.group_by("store_id", "sku", pl.col("business_date").dt.week().alias("wk")).agg(
        pl.col("on_hand_qty").sum().alias("sum_on_hand")
    )
    legacy = (weekly["sum_on_hand"] > 0).mean()
    return {
        "legacy_weekly_sum": float(legacy),
        "per_day_reported_snapshot": float(per_day_reported),
        "correct_close_of_day": float(correct),
    }
