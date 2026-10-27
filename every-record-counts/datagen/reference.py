"""Reference data: stores, products, prices, promotions, FX rates, suppliers."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from datagen import settings as s

SUBCATEGORIES = {
    "Fresh Produce": ["Fruit", "Vegetables", "Salads", "Herbs"],
    "Dairy & Chilled": ["Milk", "Cheese", "Yoghurt", "Ready Meals"],
    "Bakery": ["Bread", "Cakes", "Morning Goods"],
    "Pantry": ["Rice & Grains", "Pasta", "Sauces", "Spices", "Snacks", "Breakfast"],
    "Beverages": ["Tea & Coffee", "Soft Drinks", "Juice", "Water"],
    "Household": ["Cleaning", "Laundry", "Paper"],
    "Personal Care": ["Hair Care", "Skin Care", "Oral Care"],
    "Home & Kitchen": ["Cookware", "Storage", "Tableware"],
    "Electronics": ["Audio", "Accessories", "Small Appliances"],
    "Apparel": ["Menswear", "Womenswear", "Kidswear"],
}
BRANDS = [
    "Larkspur",
    "Larkspur Select",
    "Meadow",
    "Northfield",
    "Kaveri",
    "Lumen",
    "Harbour",
    "Tandem",
    "Orchid",
    "Brightwell",
    "Saffron Co",
    "Pebble",
    "Atlas",
    "Juniper",
    "Monsoon",
    "Fernhill",
]


def local_to_utc(d: date, t: time, tz: str) -> datetime:
    return datetime.combine(d, t, tzinfo=ZoneInfo(tz)).astimezone(UTC)


def utc_offset_seconds(d: date, tz: str) -> int:
    """UTC offset of a zone at local noon on a date (no DST switch happens at noon)."""
    return int(datetime.combine(d, time(12), tzinfo=ZoneInfo(tz)).utcoffset().total_seconds())


@dataclass
class Reference:
    stores: pl.DataFrame
    products: pl.DataFrame  # current attributes (end of window), one row per product
    product_snapshots: pl.DataFrame  # daily catalogue extract
    price_changes: pl.DataFrame
    promotion_versions: pl.DataFrame
    promotion_scope: pl.DataFrame
    fx_rates: pl.DataFrame
    suppliers: pl.DataFrame
    store_ranges: dict[str, np.ndarray]  # store_id -> product indices, most popular first


def build(seed: int, store_fraction: float) -> Reference:
    rng = np.random.default_rng([seed, 1])
    stores = _stores(rng, store_fraction)
    suppliers = _suppliers(rng)
    products, snapshots = _products(rng, suppliers)
    prices = _price_changes(rng, products)
    promo_versions, promo_scope = _promotions(rng, products)
    fx = _fx_rates(rng)
    ranges = _ranges(rng, stores, products)
    return Reference(stores, products, snapshots, prices, promo_versions, promo_scope, fx, suppliers, ranges)


def _stores(rng: np.random.Generator, store_fraction: float) -> pl.DataFrame:
    rows = []
    formats = list(s.FORMATS)
    shares = [s.FORMATS[f][0] for f in formats]
    for c in s.COUNTRIES.values():
        n = max(4, round(c.stores * store_fraction))
        fmt = rng.choice(formats, size=n, p=shares)
        city = rng.choice(c.cities, size=n)
        is_24h = rng.random(n) < s.SHARE_24H
        morning = rng.random(n) < s.SHARE_MORNING_SNAPSHOT
        opened = [date(2005, 1, 1) + timedelta(days=int(x)) for x in rng.integers(0, 7000, n)]
        for i in range(n):
            rows.append(
                {
                    "store_id": f"{c.code}-{i + 1:04d}",
                    "country": c.code,
                    "currency": c.currency,
                    "timezone": c.tz,
                    "city": str(city[i]),
                    "format": str(fmt[i]),
                    "open_24h": bool(is_24h[i]),
                    "opens_at": "00:00" if is_24h[i] else ("07:00" if fmt[i] != "express" else "06:00"),
                    "closes_at": "24:00" if is_24h[i] else ("23:00" if fmt[i] != "hyper" else "22:00"),
                    "wms_snapshot_schedule": "after_delivery_06_00" if morning[i] else "close_23_30",
                    "opened_on": opened[i],
                }
            )
    return pl.DataFrame(rows)


def _suppliers(rng: np.random.Generator) -> pl.DataFrame:
    names = [
        "Northern Dairies",
        "Deccan Agro",
        "Thames Foods",
        "Lion City Trading",
        "Kaveri Mills",
        "Brightwell Home",
        "Atlas Electronics",
        "Monsoon Textiles",
        "Harbour Beverages",
        "Fernhill Bakery",
    ]
    rows = []
    for i in range(s.N_SUPPLIERS):
        rows.append(
            {
                "supplier_id": f"S{i + 1:03d}",
                "supplier_name": f"{names[i % len(names)]} {i // len(names) + 1}",
                "feed_format": "xlsx" if i % 8 == 3 else "csv",
                "base_fill_rate": round(float(rng.uniform(0.86, 0.99)), 3),
                "on_time_rate": round(float(rng.uniform(0.80, 0.98)), 3),
            }
        )
    return pl.DataFrame(rows)


def _products(rng: np.random.Generator, suppliers: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    cats = list(s.CATEGORIES)
    shares = np.array([s.CATEGORIES[c][0] for c in cats])
    cat = rng.choice(cats, size=s.N_PRODUCTS, p=shares / shares.sum())
    sub = [str(rng.choice(SUBCATEGORIES[c])) for c in cat]
    brand = rng.choice(BRANDS, size=s.N_PRODUCTS)
    lo = np.array([s.CATEGORIES[c][1] for c in cat])
    hi = np.array([s.CATEGORIES[c][2] for c in cat])
    base = np.exp(rng.uniform(np.log(lo), np.log(hi))).round().astype(np.int64)
    base = np.maximum(base // 10 * 10 + 9, 19)  # prices end in 9
    chilled = rng.random(s.N_PRODUCTS) < np.array([s.CATEGORIES[c][3] for c in cat])
    supplier_ids = suppliers["supplier_id"].to_numpy()
    supplier = rng.choice(supplier_ids, size=s.N_PRODUCTS)
    # 6% of products launch during the window; the rest exist from the start.
    launch_offset = np.where(
        rng.random(s.N_PRODUCTS) < 0.06, rng.integers(10, (s.END - s.START).days - 10, s.N_PRODUCTS), -400
    )
    products = pl.DataFrame(
        {
            "product_id": np.arange(1, s.N_PRODUCTS + 1),
            "sku": [f"SKU-{i:05d}" for i in range(1, s.N_PRODUCTS + 1)],
            "product_name": [f"{b} {sb} {i}" for i, (b, sb) in enumerate(zip(brand, sub, strict=True), start=1)],
            "category": cat,
            "subcategory": sub,
            "brand": brand,
            "is_chilled": chilled,
            "supplier_id": supplier,
            "base_price_gbp_minor": base,
            "launched_on": [s.START + timedelta(days=int(x)) for x in launch_offset],
        }
    )

    # The 1 August catalogue reorganisation: 4% of products move subcategory or category.
    reorg_day = date(2026, 8, 1)
    moved = rng.random(s.N_PRODUCTS) < 0.04
    new_cat = np.where(moved & (rng.random(s.N_PRODUCTS) < 0.3), rng.choice(cats, size=s.N_PRODUCTS), cat)
    new_sub = [str(rng.choice(SUBCATEGORIES[nc])) if m else sb for m, nc, sb in zip(moved, new_cat, sub, strict=True)]
    after = products.with_columns(category=pl.Series(new_cat), subcategory=pl.Series(new_sub))

    days = [s.START + timedelta(days=i) for i in range((s.END - s.START).days + 1)]
    frames = []
    for d in days:
        src = after if d >= reorg_day else products
        # The catalogue extract runs just after midnight, before the day's launches go live.
        frames.append(
            src.filter(pl.col("launched_on") < d)
            .select(
                "product_id", "sku", "product_name", "category", "subcategory", "brand", "is_chilled", "supplier_id"
            )
            .with_columns(snapshot_date=pl.lit(d))
        )
    snapshots = pl.concat(frames)
    return after, snapshots


def _price_changes(rng: np.random.Generator, products: pl.DataFrame) -> pl.DataFrame:
    rows = []
    base = products["base_price_gbp_minor"].to_numpy()
    ids = products["product_id"].to_numpy()
    window_days = (s.END - s.START).days
    for c in s.COUNTRIES.values():
        local = base * c.price_factor
        local = np.where(local >= 1000, (local // 100) * 100 + 99, (local // 10) * 10 + 9).astype(np.int64)
        start = local_to_utc(date(2026, 1, 1), time(0), c.tz)
        for pid, price in zip(ids, local, strict=True):
            rows.append((int(pid), c.code, c.currency, int(price), start))
        changed = rng.random(len(ids)) < 0.12
        for pid, price in zip(ids[changed], local[changed], strict=True):
            for _ in range(int(rng.integers(1, 3))):
                d = s.START + timedelta(days=int(rng.integers(1, window_days)))
                mid_day = rng.random() < 0.4
                t = time(13, 0) if mid_day else time(0, 0)
                price = int(max(9, round(price * rng.uniform(0.85, 1.15) / 10) * 10 - 1))
                rows.append((int(pid), c.code, c.currency, price, local_to_utc(d, t, c.tz)))
    df = pl.DataFrame(rows, schema=["product_id", "country", "currency", "price_minor", "effective_from"], orient="row")
    return df.sort("country", "product_id", "effective_from").unique(
        ["country", "product_id", "effective_from"], keep="last", maintain_order=True
    )


def _promotions(rng: np.random.Generator, products: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    versions, scope = [], []
    cats = list(s.CATEGORIES)
    window_days = (s.END - s.START).days
    countries = list(s.COUNTRIES.values())
    for i in range(s.N_PROMOTIONS):
        c = countries[i % len(countries)]
        promo_id = f"PR-{i + 1:04d}"
        start_day = s.START + timedelta(days=int(rng.integers(0, window_days - 10)))
        length = int(rng.integers(7, 22))
        valid_from = local_to_utc(start_day, time(0), c.tz)
        valid_to = local_to_utc(start_day + timedelta(days=length), time(0), c.tz)
        pct = int(rng.choice([10, 15, 20, 25, 30]))
        created = valid_from - timedelta(days=int(rng.integers(2, 10)))
        if rng.random() < 0.35:
            category = str(rng.choice(cats[:7]))
            name = f"{category} week"
            scope.append((promo_id, "category", category))
        else:
            n = int(rng.integers(5, 30))
            skus = rng.choice(products["sku"].to_numpy(), size=n, replace=False)
            name = f"Multi-buy {i + 1}"
            scope.extend((promo_id, "sku", str(x)) for x in skus)
        versions.append((promo_id, 1, c.code, name, pct, valid_from, valid_to, created))
        if rng.random() < 0.3:  # edited after launch: the discount changes mid-flight
            edited = valid_from + timedelta(hours=int(rng.integers(20, max(21, (length - 1) * 24))))
            new_pct = int(max(5, pct + rng.choice([-10, -5, 5, 10])))
            versions.append((promo_id, 2, c.code, name, new_pct, valid_from, valid_to, edited))
    v = pl.DataFrame(
        versions,
        orient="row",
        schema=["promo_id", "version", "country", "promo_name", "discount_pct", "valid_from", "valid_to", "updated_at"],
    )
    sc = pl.DataFrame(scope, orient="row", schema=["promo_id", "scope_type", "scope_value"])
    return v, sc


def _fx_rates(rng: np.random.Generator) -> pl.DataFrame:
    """Daily reference rates, GBP per unit of currency, published on weekdays only."""
    start, end = s.START - timedelta(days=7), s.REPORT_DATE
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    days = [d for d in days if d.weekday() < 5]
    rows = []
    for ccy, level, vol in (("INR", 0.00885, 0.003), ("SGD", 0.575, 0.0025), ("EUR", 0.852, 0.0028)):
        steps = rng.normal(0, vol, len(days))
        if ccy == "EUR":  # sterling strengthens into October, so "today's rate" is the lowest of the quarter
            steps += np.linspace(0, -0.0012, len(days))
        path = level * np.exp(np.cumsum(steps))
        rows.extend((d, ccy, "GBP", f"{r:.8f}") for d, r in zip(days, path, strict=True))
    return pl.DataFrame(rows, orient="row", schema=["rate_date", "base_currency", "quote_currency", "rate"])


def _ranges(rng: np.random.Generator, stores: pl.DataFrame, products: pl.DataFrame) -> dict[str, np.ndarray]:
    """Each store ranges a subset of products, ordered by popularity (Zipf-like)."""
    popularity = rng.permutation(s.N_PRODUCTS)  # global popularity order of product indices
    out = {}
    for row in stores.iter_rows(named=True):
        size = s.FORMATS[row["format"]][2]
        # Take mostly from the popular head, with some local variety.
        head = popularity[: int(size * 0.7)]
        tail = rng.choice(popularity[int(size * 0.7) :], size=size - len(head), replace=False)
        out[row["store_id"]] = np.concatenate([head, tail])
    return out
