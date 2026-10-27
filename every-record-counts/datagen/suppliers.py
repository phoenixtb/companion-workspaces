"""Purchase orders (Larkspur's procurement system) and supplier delivery feeds (outsiders' files)."""

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import polars as pl

from datagen import settings as s
from datagen.reference import Reference

RENAMING_SUPPLIER = "S017"
RENAME_FROM = date(2026, 8, 20)
DMY_SUPPLIERS = {"S002", "S007", "S012", "S022", "S027", "S032", "S037"}  # send dates as DD/MM/YYYY


@dataclass
class Supply:
    po_lines: pl.DataFrame
    deliveries: pl.DataFrame  # the clean truth, one row per delivered PO line
    feed_rows: pl.DataFrame  # what suppliers actually sent, defects included, as strings
    metrics: dict[str, float]
    defect_counts: dict[str, int]


def build(seed: int, ref: Reference) -> Supply:
    rng = np.random.default_rng([seed, 6])
    products = ref.products.select("sku", "supplier_id")
    sup = ref.suppliers
    rows = []
    po_seq = 0
    mondays = [s.START + timedelta(days=i) for i in range((s.Q3_END - s.START).days + 1)]
    mondays = [d for d in mondays if d.weekday() == 0]
    by_supplier = {k[0]: g["sku"].to_numpy() for k, g in products.group_by("supplier_id", maintain_order=True)}
    lead = {sid: int(rng.integers(3, 8)) for sid in sup["supplier_id"]}
    for d in mondays:
        for sid in sup["supplier_id"].to_list():
            skus = by_supplier.get(sid)
            if skus is None or len(skus) == 0:
                continue
            for dc in ("DC-IN", "DC-GB", "DC-SG"):
                po_seq += 1
                k = int(min(len(skus), rng.integers(6, 18)))
                chosen = rng.choice(skus, size=k, replace=False)
                for line_no, sku in enumerate(chosen, start=1):
                    rows.append(
                        (
                            f"PO{po_seq:07d}",
                            line_no,
                            sid,
                            dc,
                            str(sku),
                            d,
                            d + timedelta(days=lead[sid]),
                            int(rng.integers(5, 60)) * 12,
                        )
                    )
    po = pl.DataFrame(
        rows,
        orient="row",
        schema=["po_number", "po_line", "supplier_id", "dc_id", "sku", "po_date", "expected_date", "ordered_qty"],
    )
    fill = dict(zip(sup["supplier_id"], sup["base_fill_rate"], strict=True))
    on_time = dict(zip(sup["supplier_id"], sup["on_time_rate"], strict=True))
    n = po.height
    f = np.array([fill[x] for x in po["supplier_id"].to_list()])
    ot = np.array([on_time[x] for x in po["supplier_id"].to_list()])
    missing = rng.random(n) < 0.04
    short = (~missing) & (rng.random(n) > f)
    ordered = po["ordered_qty"].to_numpy()
    delivered = np.where(short, (ordered * rng.uniform(0.4, 0.95, n)).astype(np.int64), ordered)
    late_days = np.where(rng.random(n) < ot, 0, rng.integers(1, 6, n))
    expected = po["expected_date"].to_list()
    delivery_date = [e + timedelta(days=int(x)) for e, x in zip(expected, late_days, strict=True)]
    deliveries = po.with_columns(delivered_qty=pl.Series(delivered), delivery_date=pl.Series(delivery_date)).filter(
        pl.Series(~missing) & (pl.col("delivery_date") <= s.END)
    )

    feed, counts = _feed(rng, deliveries)
    metrics = _fill_rates(po, deliveries)
    return Supply(po, deliveries, feed, metrics, counts)


def _feed(rng, deliveries: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, int]]:
    d = deliveries.with_columns(
        delivery_date_str=pl.when(pl.col("supplier_id").is_in(list(DMY_SUPPLIERS)))
        .then(pl.col("delivery_date").dt.strftime("%d/%m/%Y"))
        .otherwise(pl.col("delivery_date").dt.strftime("%Y-%m-%d")),
        delivered_qty_str=pl.col("delivered_qty").cast(pl.String),
        po_line_str=pl.col("po_line").cast(pl.String),
        file_date=pl.col("delivery_date"),
    )
    n = d.height
    # Malformed rows: impossible dates, text in numeric columns, negative quantities, missing PO numbers.
    kind = np.where(rng.random(n) < 0.004, rng.integers(0, 4, n), -1)
    d = d.with_columns(
        delivery_date_str=pl.when(pl.Series(kind == 0)).then(pl.lit("2026-09-31")).otherwise("delivery_date_str"),
        delivered_qty_str=pl.when(pl.Series(kind == 1))
        .then(pl.lit("n/a"))
        .when(pl.Series(kind == 2))
        .then(pl.format("-{}", pl.col("delivered_qty_str")))
        .otherwise("delivered_qty_str"),
        po_number=pl.when(pl.Series(kind == 3)).then(pl.lit("")).otherwise("po_number"),
    )
    # Duplicates: some rows sent twice, half of them again in the next day's file.
    dup = d.filter(pl.Series(rng.random(n) < 0.006))
    next_day = pl.Series(rng.random(dup.height) < 0.5)
    dup = dup.with_columns(
        file_date=pl.when(next_day).then(pl.col("file_date") + pl.duration(days=1)).otherwise("file_date")
    )
    feed = pl.concat([d, dup]).sort("supplier_id", "file_date", "po_number", "po_line")
    counts = {
        "malformed_rows": int((kind >= 0).sum()),
        "duplicate_rows": dup.height,
        "renamed_column_files": feed.filter(
            (pl.col("supplier_id") == RENAMING_SUPPLIER) & (pl.col("file_date") >= RENAME_FROM)
        )
        .select("file_date")
        .n_unique(),
    }
    return feed.select(
        "supplier_id", "file_date", "po_number", "po_line_str", "sku", "delivered_qty_str", "delivery_date_str", "dc_id"
    ), counts


def _fill_rates(po: pl.DataFrame, deliveries: pl.DataFrame) -> dict[str, float]:
    """Q3 fill rate and on-time-in-full (OTIF), by expected date, from clean data."""
    q3 = po.filter(pl.col("expected_date").is_between(s.Q3_START, s.Q3_END))
    j = q3.join(
        deliveries.select("po_number", "po_line", "delivered_qty", "delivery_date"),
        on=["po_number", "po_line"],
        how="left",
    )
    fill = j["delivered_qty"].fill_null(0).sum() / j["ordered_qty"].sum()
    otif = j.select(
        ((pl.col("delivered_qty") == pl.col("ordered_qty")) & (pl.col("delivery_date") <= pl.col("expected_date")))
        .fill_null(False)
        .mean()
    ).item()
    return {"fill_rate_q3": float(fill), "otif_q3": float(otif)}
