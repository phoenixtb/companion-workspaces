"""Write the generated world as landed raw files, in the formats each source really sends."""

import gzip
import io
import shutil
from datetime import UTC, datetime, time
from pathlib import Path

import openpyxl
import polars as pl

from datagen import settings as s
from datagen.incident import poller_extracts
from datagen.suppliers import RENAME_FROM, RENAMING_SUPPLIER

ISO_MS = "%Y-%m-%dT%H:%M:%S%.3fZ"
SNAPSHOT_AT = datetime.combine(s.REPORT_DATE, time(1), tzinfo=UTC)  # the OLTP extract runs at 01:00 UTC


def _ts(col: str) -> pl.Expr:
    return pl.col(col).dt.convert_time_zone("UTC").dt.strftime(ISO_MS)


def _decimal(col: str) -> pl.Expr:
    """Minor units -> '1234.56' without ever touching floating point."""
    a = pl.col(col).abs()
    return pl.format(
        "{}{}.{}",
        pl.when(pl.col(col) < 0).then(pl.lit("-")).otherwise(pl.lit("")),
        a // 100,
        (a % 100).cast(pl.String).str.zfill(2),
    )


def _gz_ndjson(path: Path, frames: list[pl.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    for f in frames:
        if f.height:
            f.write_ndjson(buf)
    with gzip.open(path, "wb", compresslevel=6) as out:
        out.write(buf.getvalue())


def write_all(out: Path, ref, sales, events, inventory, supply, clicks, settlements) -> None:
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    _reference(out, ref)
    _pos(out, sales, events)
    _oltp(out, sales)
    _inventory(out, inventory)
    _suppliers(out, supply, ref)
    _clickstream(out, clicks)
    _psp(out, settlements)


def _reference(out: Path, ref) -> None:
    (out / "reference/stores").mkdir(parents=True)
    ref.stores.write_csv(out / "reference/stores/stores.csv")
    (out / "reference/suppliers").mkdir(parents=True)
    ref.suppliers.select("supplier_id", "supplier_name", "feed_format").write_csv(
        out / "reference/suppliers/suppliers.csv"
    )
    for (d,), g in ref.product_snapshots.group_by("snapshot_date"):
        p = out / f"catalog/products/snapshot_date={d}/products.parquet"
        p.parent.mkdir(parents=True)
        g.drop("snapshot_date").write_parquet(p)
    (out / "pricing/price_changes").mkdir(parents=True)
    ref.price_changes.write_parquet(out / "pricing/price_changes/price_changes.parquet")
    (out / "promotions").mkdir(parents=True)
    ref.promotion_versions.with_columns(
        _ts("valid_from").alias("valid_from"), _ts("valid_to").alias("valid_to"), _ts("updated_at").alias("updated_at")
    ).write_csv(out / "promotions/promotion_versions.csv")
    ref.promotion_scope.write_csv(out / "promotions/promotion_scope.csv")
    (out / "fx").mkdir(parents=True)
    ref.fx_rates.write_csv(out / "fx/eod_rates.csv")


def _pos(out: Path, sales, events: pl.DataFrame) -> None:
    lines = sales.basket_lines.group_by("basket_id", maintain_order=True).agg(
        lines=pl.struct("line_no", "sku", "qty", "unit_price_minor", "discount_minor", "promo_id")
    )
    ev = events.join(lines, on="basket_id", how="left").select(
        "event_id",
        pl.lit("basket_completed").alias("event_type"),
        pl.lit(1).alias("schema_version"),
        "store_id",
        "till_id",
        "basket_id",
        _ts("event_ts").alias("event_ts"),
        _ts("uploaded_at").alias("uploaded_at"),
        "currency",
        "loyalty_id",
        "lines",
        pl.concat_list(
            pl.struct(pl.col("tender_type").alias("type"), pl.col("total_minor").alias("amount_minor"))
        ).alias("tenders"),
        pl.col("uploaded_at").dt.date().alias("_d"),
    )
    for (d,), g in ev.group_by("_d"):
        _gz_ndjson(out / f"pos/events/upload_date={d}/pos-events-{d}.jsonl.gz", [g.drop("_d")])


def _oltp(out: Path, sales) -> None:
    for (d,), g in poller_extracts(sales.orders).group_by("extract_date"):
        p = out / f"oltp/orders_poll/extract_date={d}/orders.parquet"
        p.parent.mkdir(parents=True)
        g.drop("extract_date").write_parquet(p)
    snap = SNAPSHOT_AT
    snap_dir = f"snapshot_date={s.REPORT_DATE}"
    alive = sales.orders.filter(pl.col("deleted_at").is_null() & (pl.col("created_at") <= snap))
    orders = (
        alive.with_columns(
            fulfilled_at=pl.when(pl.col("fulfilled_at") <= snap).then(pl.col("fulfilled_at")),
        )
        .with_columns(
            status=pl.when(pl.col("fulfilled_at").is_not_null()).then(pl.lit("fulfilled")).otherwise(pl.lit("placed")),
            updated_at=pl.coalesce("fulfilled_at", "created_at"),
        )
        .select(
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
            "fulfilled_at",
        )
    )
    lines = sales.order_lines.join(orders.select("order_id"), on="order_id", how="semi")
    payments = (
        sales.payments.filter(pl.col("authorised_at") <= snap)
        .with_columns(
            captured_at=pl.when(pl.col("captured_at") <= snap).then(pl.col("captured_at")),
            voided_at=pl.when(pl.col("voided_at") <= snap).then(pl.col("voided_at")),
        )
        .with_columns(
            status=pl.when(pl.col("voided_at").is_not_null())
            .then(pl.lit("voided"))
            .when(pl.col("captured_at").is_not_null())
            .then(pl.lit("captured"))
            .otherwise(pl.lit("authorised"))
        )
    )
    refunds = sales.refunds.filter(pl.col("refund_ts") <= snap)
    for name, df in (("orders", orders), ("order_lines", lines), ("payments", payments), ("refunds", refunds)):
        p = out / f"oltp/{name}/{snap_dir}/{name}.parquet"
        p.parent.mkdir(parents=True)
        df.write_parquet(p)


def _inventory(out: Path, inv) -> None:
    for (d,), g in inv.snapshots.group_by("business_date"):
        p = out / f"inventory/snapshots/business_date={d}/stock.parquet"
        p.parent.mkdir(parents=True)
        g.drop("business_date").sort("store_id", "sku").write_parquet(p)
    for (d,), g in inv.adjustments.group_by("business_date"):
        p = out / f"inventory/adjustments/business_date={d}/adjustments.parquet"
        p.parent.mkdir(parents=True)
        g.drop("business_date").write_parquet(p)


def _suppliers(out: Path, supply, ref) -> None:
    p = out / "procurement/purchase_orders/po_lines.parquet"
    p.parent.mkdir(parents=True)
    supply.po_lines.write_parquet(p)
    fmt = dict(zip(ref.suppliers["supplier_id"], ref.suppliers["feed_format"], strict=True))
    for (sid, d), g in supply.feed_rows.group_by("supplier_id", "file_date"):
        qty_header = "Qty Delivered" if sid == RENAMING_SUPPLIER and d >= RENAME_FROM else "Delivered Qty"
        table = g.select(
            pl.col("po_number").alias("PO Number"),
            pl.col("po_line_str").alias("PO Line"),
            pl.col("sku").alias("SKU"),
            pl.col("delivered_qty_str").alias(qty_header),
            pl.col("delivery_date_str").alias("Delivery Date"),
            pl.col("dc_id").alias("DC"),
        )
        stem = out / f"suppliers/deliveries/{sid}/{sid}_{d:%Y%m%d}"
        stem.parent.mkdir(parents=True, exist_ok=True)
        if fmt.get(sid) == "xlsx":
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Deliveries"
            ws.append(table.columns)
            for row in table.iter_rows():
                ws.append(list(row))
            wb.save(stem.with_suffix(".xlsx"))
        else:
            table.write_csv(stem.with_suffix(".csv"))


def _clickstream(out: Path, clicks) -> None:
    ev = clicks.events.with_columns(
        _d=pl.col("event_ts").dt.date(),
        event_ts=_ts("event_ts"),
        _renamed=pl.col("page_path").is_not_null(),
        _consent=pl.col("consent_analytics").is_not_null(),
        _app=pl.col("app_version").is_not_null(),
    )
    base = ["event_id", "event_type", "event_ts", "anonymous_id", "user_id", "platform", "user_agent", "country"]
    for (d,), day in ev.group_by("_d"):
        frames = []
        for (renamed, consent, app), g in day.group_by("_renamed", "_consent", "_app"):
            cols = [*base, "page_path" if renamed else "page", "sku", "order_id"]
            if app:
                cols.append("app_version")
            sel = [pl.col(c) for c in cols]
            if consent:  # the field only exists once the release that added it is live
                sel.append(
                    pl.struct(pl.col("consent_analytics").alias("analytics"), pl.col("consent_ads").alias("ads")).alias(
                        "consent"
                    )
                )
            frames.append(g.sort("event_ts").select(sel))
        _gz_ndjson(out / f"clickstream/events/event_date={d}/events-{d}.jsonl.gz", frames)


def _psp(out: Path, settlements: pl.DataFrame) -> None:
    files = settlements.with_columns(
        transaction_ts=_ts("transaction_ts"),
        presentment_amount=_decimal("presentment_minor"),
        gross_amount=_decimal("gross_minor"),
        fee_amount=_decimal("fee_minor"),
        net_amount=_decimal("net_minor"),
    )
    for (c, d), g in files.group_by("country", "settlement_date"):
        p = out / f"psp/settlements/country={c}/kestrel_{c}_{d:%Y%m%d}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        g.select(
            "psp_reference",
            "merchant_reference",
            "record_type",
            "payment_method",
            "transaction_ts",
            "settlement_date",
            "presentment_currency",
            "presentment_amount",
            "settlement_currency",
            "gross_amount",
            "fee_amount",
            "net_amount",
        ).write_csv(p)
