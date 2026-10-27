"""The 'three numbers, three answers' incident: UK net revenue.

Computes UK (GB) net revenue for Q3 2026 two ways, the legacy board-deck way and
the correct way, and the waterfall between them, one fix at a time. The order of
the fixes is fixed, so the steps always sum exactly to the gap:

    1. dedupe POS events by basket_id (till re-sends after reconnecting)
    2. drop online orders that were cancelled and hard-deleted (the poller never saw the delete)
    3. date refunds by refund date, not by the original purchase date
    4. convert EUR at the rate of the transaction date, not "today's" rate
    5. use the local (Europe/London) business date, not the UTC date

Step 1 is also the calibration knob: the generator picks how many offline-till
baskets were re-sent so that the total gap is TARGET_UK_REVENUE_GAP.
"""

from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl

from datagen import settings as s
from datagen.sales import Sales

STEPS = (
    ("duplicate_pos_events", "Duplicate POS events (till re-sends after reconnecting)"),
    ("hard_deleted_orders", "Cancelled online orders hard-deleted after the poller saw them"),
    ("refund_dating", "Refunds dated by original purchase date instead of refund date"),
    ("fx_today_rate", "EUR orders and refunds converted at the report-day rate"),
    ("utc_business_date", "Quarter boundaries in UTC instead of Europe/London"),
)


@dataclass
class RevenueWaterfall:
    legacy_minor: int
    correct_minor: int
    steps: list[tuple[str, str, int]]  # (key, label, legacy-side amount removed by the fix)
    duplicates_injected: int

    @property
    def gap(self) -> float:
        return (self.legacy_minor - self.correct_minor) / self.correct_minor


def poll_runs(ts: pl.Expr) -> pl.Expr:
    """The legacy poller runs nightly at 01:00 UTC; return the run date that first sees a row."""
    return (ts - pl.duration(hours=1)).dt.date() + pl.duration(days=1)


def poller_extracts(orders: pl.DataFrame) -> pl.DataFrame:
    """What the nightly `updated_at > last_run` job extracted, one row per order per run."""
    placed = (
        orders.with_columns(run=poll_runs(pl.col("created_at")))
        .filter(
            # A cancellation is a hard DELETE. If it happened before the first run, the poller never saw the order.
            pl.col("deleted_at").is_null()
            | (pl.col("deleted_at") > pl.col("run").cast(pl.Datetime("ms", "UTC")) + pl.duration(hours=1))
        )
        .with_columns(status=pl.lit("placed"), updated_at=pl.col("created_at"))
    )
    fulfilled = orders.filter(pl.col("fulfilled_at").is_not_null()).with_columns(
        run=poll_runs(pl.col("fulfilled_at")), status=pl.lit("fulfilled"), updated_at=pl.col("fulfilled_at")
    )
    cols = [
        "run",
        "order_id",
        "customer_id",
        "country",
        "currency",
        "channel",
        "status",
        "total_minor",
        "tender_type",
        "created_at",
        "updated_at",
    ]
    out = pl.concat([placed.select(cols), fulfilled.select(cols)])
    return out.filter(pl.col("run") <= s.REPORT_DATE).rename({"run": "extract_date"})


def fx_table(fx: pl.DataFrame) -> pl.DataFrame:
    """Rates as integers with 8 implied decimals, so conversions are exact and reproducible in SQL."""
    return fx.with_columns(rate_e8=pl.col("rate").str.replace(".", "", literal=True).cast(pl.Int64)).sort("rate_date")


def _convert(amount: pl.Expr, rate_e8: pl.Expr) -> pl.Expr:
    # round half up; amounts are positive, matching SQL ROUND(amount * rate) on DECIMAL
    return (amount * rate_e8 + 50_000_000) // 100_000_000


def _with_rate(df: pl.DataFrame, fx: pl.DataFrame, on: str, out: str) -> pl.DataFrame:
    eur = fx.filter(pl.col("base_currency") == "EUR").select(
        pl.col("rate_date").alias(on), pl.col("rate_e8").alias(out)
    )
    return df.sort(on).join_asof(eur, on=on, strategy="backward")


def _frames(sales: Sales, pos_events: pl.DataFrame, fx: pl.DataFrame):
    tz = s.COUNTRIES["GB"].tz
    latest = fx.filter(pl.col("base_currency") == "EUR").sort("rate_date")["rate_e8"][-1]

    def dates(df: pl.DataFrame, ts: str) -> pl.DataFrame:
        return df.with_columns(utc_date=pl.col(ts).dt.date(), local_date=pl.col(ts).dt.convert_time_zone(tz).dt.date())

    pos = dates(pos_events.filter(pl.col("country") == "GB"), "event_ts")
    orders = sales.orders.filter(pl.col("country") == "GB")
    polled = poller_extracts(orders).sort("updated_at").unique("order_id", keep="last")
    polled = dates(polled, "created_at")
    current = dates(orders.filter(pl.col("deleted_at").is_null()), "created_at")
    refunds = sales.refunds.filter(pl.col("country") == "GB")
    purchased = pl.concat(
        [
            sales.baskets.select(
                pl.col("basket_id").alias("original_reference"), pl.col("event_ts").alias("purchased_at")
            ),
            sales.orders.select(
                pl.col("order_id").alias("original_reference"), pl.col("created_at").alias("purchased_at")
            ),
        ]
    )
    refunds = dates(refunds.join(purchased, on="original_reference", how="left"), "refund_ts").with_columns(
        purchase_utc_date=pl.col("purchased_at").dt.date()
    )
    for name in ("polled", "current"):
        df = polled if name == "polled" else current
        df = _with_rate(_with_rate(df, fx, "utc_date", "rate_utc"), fx, "local_date", "rate_local")
        if name == "polled":
            polled = df
        else:
            current = df
    refunds = _with_rate(_with_rate(refunds, fx, "utc_date", "rate_utc"), fx, "local_date", "rate_local")
    return pos, polled, current, refunds, latest


def revenue(pos, polled, current, refunds, latest, fixed: int) -> int:
    """GB net revenue for Q3 in pence, with the first `fixed` fixes from STEPS applied."""
    in_q3 = lambda c: pl.col(c).is_between(s.Q3_START, s.Q3_END)  # noqa: E731
    date_col = "local_date" if fixed >= 5 else "utc_date"
    rate_col = ("rate_local" if fixed >= 5 else "rate_utc") if fixed >= 4 else None

    def to_gbp(df: pl.DataFrame) -> pl.Expr:
        rate = pl.col(rate_col) if rate_col else pl.lit(latest)
        return pl.when(pl.col("currency") == "EUR").then(_convert(pl.col("amount"), rate)).otherwise(pl.col("amount"))

    p = pos if fixed < 1 else pos.unique("basket_id")
    pos_sum = p.filter(in_q3(date_col))["total_minor"].sum()
    o = (polled if fixed < 2 else current).rename({"total_minor": "amount"})
    online_sum = o.filter(in_q3(date_col)).select(to_gbp(o).sum()).item()
    r = refunds.rename({"amount_minor": "amount"})
    r = r.filter(in_q3("purchase_utc_date")) if fixed < 3 else r.filter(in_q3(date_col))
    refund_sum = r.select(to_gbp(r).sum()).item() if r.height else 0
    return int(pos_sum + (online_sum or 0) - refund_sum)


def calibrate(seed: int, sales: Sales, fx_raw: pl.DataFrame) -> tuple[pl.DataFrame, RevenueWaterfall]:
    rng = np.random.default_rng([seed, 4])
    fx = fx_table(fx_raw)
    b = sales.baskets
    events = b.with_columns(
        event_id=pl.Series([f"{x:016x}" for x in rng.integers(0, 2**63, b.height)]),
        duplicate_of=pl.lit(None, pl.String),
    )
    pos, polled, current, refunds, latest = _frames(sales, events, fx)
    correct = revenue(pos, polled, current, refunds, latest, 5)
    after_dedupe = revenue(pos, polled, current, refunds, latest, 1)
    need = round(s.TARGET_UK_REVENUE_GAP * correct) - (after_dedupe - correct)
    if need <= 0:
        raise ValueError(f"non-duplicate defects already exceed the target gap ({after_dedupe - correct} pence)")

    tz = s.COUNTRIES["GB"].tz
    pool = b.filter(
        (pl.col("country") == "GB")
        & pl.col("offline_upload")
        & pl.col("event_ts").dt.date().is_between(s.Q3_START, s.Q3_END)
        & pl.col("event_ts").dt.convert_time_zone(tz).dt.date().is_between(s.Q3_START, s.Q3_END)
    )
    pool = pool.with_columns(order=pl.Series(rng.permutation(pool.height))).sort("order")
    csum = np.cumsum(pool["total_minor"].to_numpy())
    k = int(np.searchsorted(csum, need))
    if k >= len(csum):
        raise ValueError("not enough offline baskets to calibrate the duplicate defect; raise --scale")
    if k > 0 and abs(csum[k - 1] - need) < abs(csum[k] - need):
        k -= 1
    gb_dupes = pool.head(k + 1)["basket_id"]
    # Other countries: about 15% of offline baskets were re-sent; nobody calibrates those.
    other = b.filter((pl.col("country") != "GB") & pl.col("offline_upload"))
    other_dupes = other.filter(pl.Series(rng.random(other.height) < 0.15))["basket_id"]
    dupe_ids = pl.concat([gb_dupes, other_dupes])
    dupes = events.filter(pl.col("basket_id").is_in(dupe_ids.implode()))
    resend = rng.uniform(60, 1800, dupes.height)
    dupes = dupes.with_columns(
        duplicate_of=pl.col("event_id"),
        event_id=pl.Series([f"{x:016x}" for x in rng.integers(0, 2**63, dupes.height)]),
        uploaded_at=pl.col("uploaded_at") + pl.duration(milliseconds=pl.Series((resend * 1000).astype(np.int64))),
    )
    events = pl.concat([events, dupes]).sort("uploaded_at")

    pos = pos_for(events)
    totals = [revenue(pos, polled, current, refunds, latest, i) for i in range(6)]
    steps = [(key, label, totals[i] - totals[i + 1]) for i, (key, label) in enumerate(STEPS)]
    return events, RevenueWaterfall(totals[0], totals[5], steps, int(dupes.filter(pl.col("country") == "GB").height))


def pos_for(events: pl.DataFrame) -> pl.DataFrame:
    tz = s.COUNTRIES["GB"].tz
    return events.filter(pl.col("country") == "GB").with_columns(
        utc_date=pl.col("event_ts").dt.date(), local_date=pl.col("event_ts").dt.convert_time_zone(tz).dt.date()
    )


def report_run_rate(fx_raw: pl.DataFrame) -> tuple[date, str]:
    eur = fx_raw.filter(pl.col("base_currency") == "EUR").sort("rate_date")
    return eur["rate_date"][-1], eur["rate"][-1]
