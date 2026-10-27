"""Sales: POS baskets, online orders, payments, refunds and PSP settlements.

Everything is generated in integer minor units of the transaction currency.
Timestamps are UTC; the store or country time zone decides the business date.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import numpy as np
import polars as pl

from datagen import settings as s
from datagen.reference import Reference, utc_offset_seconds

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
DOW_FACTOR = np.array([0.95, 0.92, 0.95, 1.0, 1.1, 1.25, 1.1])  # Monday..Sunday
TILLS = {"hyper": 12, "super": 6, "express": 3}


def _sec(dt: datetime) -> int:
    return int((dt - EPOCH).total_seconds())


def _to_ts(seconds: np.ndarray) -> pl.Series:
    """Seconds since EPOCH (int64, may carry milliseconds as fraction) -> UTC datetime series."""
    ms = (np.asarray(seconds, dtype=np.float64) * 1000).round().astype(np.int64)
    base = int(EPOCH.timestamp() * 1000)
    return pl.Series(ms + base).cast(pl.Datetime("ms")).dt.replace_time_zone("UTC")


@dataclass
class Sales:
    baskets: pl.DataFrame  # one row per real basket
    basket_lines: pl.DataFrame
    orders: pl.DataFrame  # one row per online order ever placed (incl. later hard-deleted)
    order_lines: pl.DataFrame
    payments: pl.DataFrame
    refunds: pl.DataFrame


class PriceBook:
    """Vectorised 'price in force at time t' lookup per country."""

    def __init__(self, ref: Reference):
        self.books = {}
        for c in s.COUNTRIES:
            df = ref.price_changes.filter(pl.col("country") == c).sort("product_id", "effective_from")
            pid = df["product_id"].to_numpy().astype(np.int64)
            t = np.array([_sec(x) for x in df["effective_from"].to_list()], dtype=np.int64)
            key = pid * 1_000_000_000 + t
            self.books[c] = (key, df["price_minor"].to_numpy())

    def price(self, country: str, product_idx: np.ndarray, t_sec: np.ndarray) -> np.ndarray:
        key, price = self.books[country]
        q = (product_idx.astype(np.int64) + 1) * 1_000_000_000 + t_sec.astype(np.int64)
        return price[np.searchsorted(key, q, side="right") - 1]


class PromoBook:
    """Which promotion version applies to a product at time t (largest discount wins)."""

    def __init__(self, ref: Reference):
        sku_to_idx = {sku: i for i, sku in enumerate(ref.products["sku"].to_list())}
        cat = ref.products["category"].to_numpy()
        scope: dict[str, set[int]] = {}
        for promo_id, kind, value in ref.promotion_scope.iter_rows():
            idx = scope.setdefault(promo_id, set())
            if kind == "category":
                idx.update(np.flatnonzero(cat == value).tolist())
            else:
                idx.add(sku_to_idx[value])
        self.windows: dict[str, list[tuple[str, int, np.ndarray, int, int]]] = {c: [] for c in s.COUNTRIES}
        v = ref.promotion_versions.sort("promo_id", "version")
        for promo in v.partition_by("promo_id"):
            rows = promo.to_dicts()
            members = np.zeros(s.N_PRODUCTS, dtype=bool)
            members[list(scope[rows[0]["promo_id"]])] = True
            for i, r in enumerate(rows):
                start = _sec(r["valid_from"]) if i == 0 else _sec(r["updated_at"])
                end = _sec(rows[i + 1]["updated_at"]) if i + 1 < len(rows) else _sec(r["valid_to"])
                self.windows[r["country"]].append((r["promo_id"], r["discount_pct"], members, start, end))

    def apply(self, country: str, product_idx: np.ndarray, t_sec: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        best = np.zeros(len(product_idx), dtype=np.int64)
        promo = np.full(len(product_idx), None, dtype=object)
        for promo_id, pct, members, start, end in self.windows[country]:
            hit = members[product_idx] & (t_sec >= start) & (t_sec < end) & (pct > best)
            best[hit] = pct
            promo[hit] = promo_id
        return best, promo


def _time_of_day(rng: np.random.Generator, n: int, open_s: np.ndarray, close_s: np.ndarray) -> np.ndarray:
    """Seconds after local midnight: lunch and evening peaks, clipped to opening hours."""
    peak = np.where(rng.random(n) < 0.45, rng.normal(12.5 * 3600, 7200, n), rng.normal(18.5 * 3600, 5400, n))
    uniform = rng.uniform(open_s, close_s)
    t = np.where(rng.random(n) < 0.15, uniform, peak)
    return np.clip(t, open_s, close_s - 60).round(3)


def _lines(rng, n_parents: int, pick_product, mean_extra: float = 1.6) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n_lines = 1 + rng.poisson(mean_extra, n_parents)
    parent = np.repeat(np.arange(n_parents), n_lines)
    line_no = np.concatenate([np.arange(1, k + 1) for k in n_lines]) if n_parents else np.array([], int)
    product_idx = pick_product(parent)
    return parent, line_no, product_idx


def _qty(rng, n):
    return rng.choice([1, 2, 3], size=n, p=[0.76, 0.17, 0.07])


def build(seed: int, scale: float, ref: Reference, store_fraction: float = 1.0) -> Sales:
    rng = np.random.default_rng([seed, 2])
    prices, promos = PriceBook(ref), PromoBook(ref)
    launched = np.array(
        [_sec(datetime.combine(d, datetime.min.time(), tzinfo=UTC)) for d in ref.products["launched_on"].to_list()]
    )
    baskets, b_lines = _pos(rng, scale, ref, prices, promos, launched)
    # Online volume shrinks with the store sample too, so the channel mix stays the same.
    orders, o_lines = _online(rng, scale * store_fraction, ref, prices, promos, launched)
    payments = _payments(rng, baskets, orders)
    refunds = _refunds(rng, baskets, b_lines, orders, o_lines)
    return Sales(baskets, b_lines, orders, o_lines, payments, refunds)


def _pos(rng, scale, ref: Reference, prices: PriceBook, promos: PromoBook, launched):
    stores = ref.stores
    basket_frames, line_frames = [], []
    days = [s.START + timedelta(days=i) for i in range((s.END - s.START).days + 1)]
    for c in s.COUNTRIES.values():
        st = stores.filter(pl.col("country") == c.code)
        ids = st["store_id"].to_list()
        size = np.array([s.FORMATS[f][1] for f in st["format"].to_list()])
        open_h = np.array([int(x[:2]) for x in st["opens_at"].to_list()]) * 3600
        close_h = np.array([int(x[:2]) for x in st["closes_at"].to_list()]) * 3600
        ranges = [ref.store_ranges[i] for i in ids]
        range_len = np.array([len(r) for r in ranges])
        range_mat = np.zeros((len(ids), range_len.max()), dtype=np.int64)
        for i, r in enumerate(ranges):
            range_mat[i, : len(r)] = r
        tills = np.array([TILLS[f] for f in st["format"].to_list()])
        tenders = list(c.tender_mix)
        for d in days:
            lam = c.baskets_per_store_day * size * DOW_FACTOR[d.weekday()] * scale
            n_per_store = rng.poisson(lam)
            n = int(n_per_store.sum())
            if n == 0:
                continue
            store_i = np.repeat(np.arange(len(ids)), n_per_store)
            seq = np.concatenate([np.arange(1, k + 1) for k in n_per_store if k])
            tod = _time_of_day(rng, n, open_h[store_i], close_h[store_i])
            midnight = _sec(datetime.combine(d, datetime.min.time(), tzinfo=UTC)) - utc_offset_seconds(d, c.tz)
            t = midnight + tod

            def pick(parent, store_i=store_i, range_len=range_len, range_mat=range_mat):
                si = store_i[parent]
                pos = np.floor(rng.random(len(parent)) ** 2.2 * range_len[si]).astype(np.int64)
                return range_mat[si, pos]

            parent, line_no, pidx = _lines(rng, n, pick)
            lt = t[parent]
            not_yet = launched[pidx] > lt  # never sell a product before its launch day
            pidx = np.where(not_yet, range_mat[store_i[parent], 0], pidx)
            qty = _qty(rng, len(pidx))
            unit = prices.price(c.code, pidx, lt.astype(np.int64))
            pct, promo_id = promos.apply(c.code, pidx, lt.astype(np.int64))
            discount = (qty * unit * pct + 50) // 100
            amount = qty * unit - discount
            total = np.bincount(parent, weights=amount, minlength=n).astype(np.int64)
            ymd = d.strftime("%Y%m%d")
            basket_ids = [f"{ids[i]}-{ymd}-{k:05d}" for i, k in zip(store_i, seq, strict=True)]
            till_no = 1 + (rng.random(n) * tills[store_i]).astype(int)
            basket_frames.append(
                pl.DataFrame(
                    {
                        "basket_id": basket_ids,
                        "store_id": [ids[i] for i in store_i],
                        "till_id": [f"{ids[i]}-T{k:02d}" for i, k in zip(store_i, till_no, strict=True)],
                        "country": c.code,
                        "currency": c.currency,
                        "event_ts": _to_ts(t),
                        "total_minor": total,
                        "tender_type": rng.choice(tenders, size=n, p=list(c.tender_mix.values())),
                        "loyalty_id": pl.Series(
                            [
                                f"L{c.code}{x:07d}" if member else None
                                for x, member in zip(rng.integers(1, 400_000, n), rng.random(n) < 0.45, strict=True)
                            ],
                            dtype=pl.String,
                        ),
                    }
                )
            )
            line_frames.append(
                pl.DataFrame(
                    {
                        "basket_id": [basket_ids[p] for p in parent],
                        "line_no": line_no,
                        "sku": [f"SKU-{p + 1:05d}" for p in pidx],
                        "qty": qty,
                        "unit_price_minor": unit,
                        "discount_minor": discount,
                        "promo_id": promo_id.tolist(),
                        "amount_minor": amount,
                    },
                    schema_overrides={"promo_id": pl.String},
                )
            )
    baskets = pl.concat(basket_frames)
    # Offline tills: a few baskets are buffered and uploaded hours later.
    n = baskets.height
    offline = rng.random(n) < s.OFFLINE_TILL_SHARE
    lag = np.where(offline, rng.uniform(2 * 3600, 16 * 3600, n), rng.uniform(2, 90, n))
    baskets = baskets.with_columns(
        offline_upload=pl.Series(offline),
        uploaded_at=(pl.col("event_ts") + pl.duration(milliseconds=pl.Series((lag * 1000).astype(np.int64)))),
    )
    return baskets, pl.concat(line_frames)


def _online(rng, scale, ref: Reference, prices: PriceBook, promos: PromoBook, launched):
    order_frames, line_frames = [], []
    days = [s.START + timedelta(days=i) for i in range((s.END - s.START).days + 1)]
    popular = np.argsort(rng.random(s.N_PRODUCTS))[:1200]  # the online range
    online_tenders = {
        "IN": {"card": 0.4, "upi": 0.6},
        "GB": {"card": 0.7, "wallet": 0.3},
        "SG": {"card": 0.6, "wallet": 0.4},
    }
    for c in s.COUNTRIES.values():
        seq = 0
        for d in days:
            n = int(rng.poisson(c.online_orders_per_day * DOW_FACTOR[d.weekday()] * scale))
            if n == 0:
                continue
            # Online shopping runs round the clock, peaking at lunch and late evening.
            tod = np.where(rng.random(n) < 0.35, rng.normal(13 * 3600, 7200, n), rng.normal(21.5 * 3600, 6000, n))
            tod = np.mod(np.where(rng.random(n) < 0.12, rng.uniform(0, 86400, n), tod), 86400).round(3)
            midnight = _sec(datetime.combine(d, datetime.min.time(), tzinfo=UTC)) - utc_offset_seconds(d, c.tz)
            t = midnight + tod
            parent, line_no, pidx = _lines(
                rng, n, lambda p: popular[np.floor(rng.random(len(p)) ** 2.0 * len(popular)).astype(np.int64)], 2.4
            )
            lt = t[parent]
            pidx = np.where(launched[pidx] > lt, popular[0], pidx)
            qty = _qty(rng, len(pidx))
            unit = prices.price(c.code, pidx, lt.astype(np.int64))
            currency = np.full(n, c.currency, dtype=object)
            if c.code == "GB":
                eur = rng.random(n) < s.GB_EUR_ORDER_SHARE
                currency[eur] = "EUR"
                unit = np.where(eur[parent], (unit * 116 + 50) // 100, unit)  # the EUR price list
            pct, promo_id = promos.apply(c.code, pidx, lt.astype(np.int64))
            discount = (qty * unit * pct + 50) // 100
            amount = qty * unit - discount
            total = np.bincount(parent, weights=amount, minlength=n).astype(np.int64)
            order_ids = [f"O{c.code}{seq + i + 1:08d}" for i in range(n)]
            seq += n
            cancelled = rng.random(n) < s.ONLINE_CANCEL_RATE
            cancel_after = rng.uniform(600, 20 * 3600, n)
            fulfil_after = rng.uniform(4 * 3600, 48 * 3600, n)
            order_frames.append(
                pl.DataFrame(
                    {
                        "order_id": order_ids,
                        "customer_id": [f"C{c.code}{x:07d}" for x in rng.integers(1, 250_000, n)],
                        "country": c.code,
                        "currency": currency.tolist(),
                        "channel": rng.choice(["web", "app"], size=n, p=[0.45, 0.55]),
                        "created_at": _to_ts(t),
                        "total_minor": total,
                        "tender_type": rng.choice(
                            list(online_tenders[c.code]), size=n, p=list(online_tenders[c.code].values())
                        ),
                        "cancelled": cancelled,
                        "deleted_at": _to_ts(t + cancel_after),
                        "fulfilled_at": _to_ts(t + fulfil_after),
                    }
                ).with_columns(
                    deleted_at=pl.when(pl.col("cancelled")).then(pl.col("deleted_at")),
                    fulfilled_at=pl.when(~pl.col("cancelled")).then(pl.col("fulfilled_at")),
                )
            )
            line_frames.append(
                pl.DataFrame(
                    {
                        "order_id": [order_ids[p] for p in parent],
                        "line_no": line_no,
                        "sku": [f"SKU-{p + 1:05d}" for p in pidx],
                        "qty": qty,
                        "unit_price_minor": unit,
                        "discount_minor": discount,
                        "promo_id": promo_id.tolist(),
                        "amount_minor": amount,
                    },
                    schema_overrides={"promo_id": pl.String},
                )
            )
    return pl.concat(order_frames), pl.concat(line_frames)


def _payments(rng, baskets: pl.DataFrame, orders: pl.DataFrame) -> pl.DataFrame:
    """The payment ledger: one row per payment, for every PSP tender in store and online."""
    pos = baskets.filter(pl.col("tender_type").is_in(list(s.PSP_TENDERS))).select(
        pl.lit("pos").alias("source"),
        pl.col("basket_id").alias("reference"),
        "country",
        "currency",
        "tender_type",
        pl.col("total_minor").alias("amount_minor"),
        pl.col("event_ts").alias("authorised_at"),
        pl.col("event_ts").alias("captured_at"),
        pl.lit(None, dtype=pl.Datetime("ms", "UTC")).alias("voided_at"),
    )
    online = orders.select(
        pl.lit("online").alias("source"),
        pl.col("order_id").alias("reference"),
        "country",
        "currency",
        "tender_type",
        pl.col("total_minor").alias("amount_minor"),
        pl.col("created_at").alias("authorised_at"),
        pl.col("fulfilled_at").alias("captured_at"),
        pl.col("deleted_at").alias("voided_at"),
    )
    p = pl.concat([pos, online]).sort("authorised_at")
    return p.with_columns(
        payment_id=pl.format("PAY{}", pl.int_range(1, p.height + 1).cast(pl.String).str.zfill(9)),
        status=pl.when(pl.col("voided_at").is_not_null()).then(pl.lit("voided")).otherwise(pl.lit("captured")),
    ).select(
        "payment_id",
        "source",
        "reference",
        "country",
        "currency",
        "tender_type",
        "amount_minor",
        "status",
        "authorised_at",
        "captured_at",
        "voided_at",
    )


def _refunds(rng, baskets, b_lines, orders, o_lines) -> pl.DataFrame:
    end_s = _sec(datetime.combine(s.END + timedelta(days=1), datetime.min.time(), tzinfo=UTC)) - 9 * 3600
    frames = []
    sources = (
        (
            "pos",
            baskets.rename({"basket_id": "reference", "event_ts": "purchased_at"}),
            b_lines.rename({"basket_id": "reference"}),
            s.REFUND_RATE_POS,
        ),
        (
            "online",
            orders.filter(~pl.col("cancelled"))
            .rename({"order_id": "reference", "created_at": "purchased_at"})
            .with_columns(store_id=pl.lit(None, pl.String)),
            o_lines.rename({"order_id": "reference"}),
            s.REFUND_RATE_ONLINE,
        ),
    )
    for kind, parents, lines, rate in sources:
        pick = parents.filter(pl.Series(rng.random(parents.height) < rate))
        n = pick.height
        lag = np.minimum(rng.exponential(9 * 86400, n) + 3600, s.REFUND_MAX_LAG_DAYS * 86400)
        purchased = np.array([_sec(x) for x in pick["purchased_at"].to_list()], dtype=np.float64)
        refund_t = purchased + lag
        keep = refund_t < end_s
        pick, refund_t = pick.filter(pl.Series(keep)), refund_t[keep]
        # Full refund for most; otherwise the first line only.
        first_line = lines.filter(pl.col("line_no") == 1).select("reference", first_amount="amount_minor")
        pick = pick.join(first_line, on="reference", how="left")
        full = rng.random(pick.height) < 0.6
        frames.append(
            pick.select(
                pl.lit(kind).alias("original_channel"),
                pl.col("reference").alias("original_reference"),
                "country",
                "store_id",
                "currency",
                "tender_type",
                pl.Series("refund_ts", _to_ts(refund_t)),
                pl.when(pl.Series(full))
                .then(pl.col("total_minor"))
                .otherwise(pl.col("first_amount"))
                .alias("amount_minor"),
                pl.Series(
                    "reason",
                    rng.choice(["damaged", "changed_mind", "wrong_item", "late_delivery", "quality"], size=pick.height),
                ),
            )
        )
    r = pl.concat(frames).filter(pl.col("amount_minor") > 0).sort("refund_ts")
    return r.with_columns(
        refund_id=pl.format("RF{}", pl.int_range(1, r.height + 1).cast(pl.String).str.zfill(8))
    ).select("refund_id", pl.exclude("refund_id"))


def add_weekdays(d: date, n: int) -> date:
    while n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d
