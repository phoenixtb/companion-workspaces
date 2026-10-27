"""The defect manifest: what was injected, how often, and what it does to each headline number."""

import json
from datetime import date
from pathlib import Path

import polars as pl

from datagen import settings as s
from datagen.clickstream import BOT_UA_PATTERN, CONSENT_FROM
from datagen.clickstream import RENAME_FROM as CLICK_RENAME_FROM
from datagen.incident import RevenueWaterfall, poller_extracts
from datagen.suppliers import DMY_SUPPLIERS, RENAME_FROM, RENAMING_SUPPLIER


def build(args: dict, ref, sales, events, waterfall: RevenueWaterfall, inv, supply, clicks, psp) -> dict:
    gb = pl.col("country") == "GB"
    orders = sales.orders
    deleted = orders.filter(pl.col("deleted_at").is_not_null())
    polled_ids = poller_extracts(orders)["order_id"].unique()
    purchased = pl.concat(
        [
            sales.baskets.select(pl.col("basket_id").alias("ref"), pl.col("event_ts").alias("bought")),
            orders.select(pl.col("order_id").alias("ref"), pl.col("created_at").alias("bought")),
        ]
    )
    refunds = sales.refunds.join(purchased, left_on="original_reference", right_on="ref")
    tz_expr = {c.code: c.tz for c in s.COUNTRIES.values()}
    local_vs_utc = 0
    for code, tz in tz_expr.items():
        b = sales.baskets.filter(pl.col("country") == code)
        local_vs_utc += b.filter(
            pl.col("event_ts").dt.date() != pl.col("event_ts").dt.convert_time_zone(tz).dt.date()
        ).height
    offline = sales.baskets.filter(pl.col("offline_upload"))
    late_cross = 0
    for code, tz in tz_expr.items():
        o = offline.filter(pl.col("country") == code)
        late_cross += o.filter(
            pl.col("uploaded_at").dt.convert_time_zone(tz).dt.date()
            != pl.col("event_ts").dt.convert_time_zone(tz).dt.date()
        ).height
    launch = ref.products.filter(pl.col("launched_on") > s.START).select("sku", "launched_on")
    launch_day_lines = (
        sales.basket_lines.join(sales.baskets.select("basket_id", "event_ts", "country"), on="basket_id")
        .join(launch, on="sku")
        .filter(pl.col("event_ts").dt.date() == pl.col("launched_on"))
        .height
    )
    mid_day = ref.price_changes.filter(
        pl.col("effective_from") > pl.datetime(2026, 1, 2, time_zone="UTC")
    ).with_columns(
        local_hour=pl.struct("effective_from", "country").map_elements(
            lambda r: r["effective_from"].astimezone(_zone(tz_expr[r["country"]])).hour, return_dtype=pl.Int32
        )
    )
    fx_days = ref.fx_rates.filter(pl.col("base_currency") == "EUR")["rate_date"].to_list()
    window_days = [s.START.fromordinal(x) for x in range(s.START.toordinal(), s.END.toordinal() + 1)]
    gb_psp = psp.filter(gb & pl.col("settlement_date").is_between(s.Q3_START, s.Q3_END))
    by_type = {
        r["record_type"]: r
        for r in gb_psp.group_by("record_type")
        .agg(pl.len().alias("rows"), pl.col("gross_minor").sum(), pl.col("fee_minor").sum())
        .to_dicts()
    }

    m = {
        "generator": args,
        "window": {
            "start": str(s.START),
            "end": str(s.END),
            "quarter": [str(s.Q3_START), str(s.Q3_END)],
            "report_date": str(s.REPORT_DATE),
        },
        "volumes": {
            "stores": ref.stores.height,
            "products": ref.products.height,
            "pos_baskets": sales.baskets.height,
            "pos_events": events.height,
            "pos_lines": sales.basket_lines.height,
            "online_orders": orders.height,
            "refunds": sales.refunds.height,
            "payments": sales.payments.height,
            "inventory_snapshot_rows": inv.snapshots.height,
            "clickstream_events": clicks.events.height,
            "po_lines": supply.po_lines.height,
            "supplier_feed_rows": supply.feed_rows.height,
            "psp_settlement_rows": psp.height,
        },
        "uk_net_revenue_q3": {
            "currency": "GBP",
            "legacy_board_deck_minor": waterfall.legacy_minor,
            "correct_minor": waterfall.correct_minor,
            "gap_share": round(waterfall.gap, 6),
            "steps": [
                {"key": k, "label": label, "amount_minor": a, "share_of_correct": round(a / waterfall.correct_minor, 6)}
                for k, label, a in waterfall.steps
            ],
        },
        "stock_availability_q3": {k: round(v, 6) for k, v in inv.metrics.items()},
        "conversion_q3": {
            "product_team": round(clicks.metrics["product_conversion"], 5),
            "marketing": round(clicks.metrics["marketing_conversion"], 5),
        },
        "supplier_fill_q3": {k: round(v, 4) for k, v in supply.metrics.items()},
        "defects": {
            "pos_duplicate_events_total": events.height - sales.baskets.height,
            "pos_duplicate_events_gb": waterfall.duplicates_injected,
            "pos_offline_baskets": offline.height,
            "pos_late_uploads_crossing_local_midnight": late_cross,
            "pos_baskets_utc_date_differs_from_local": local_vs_utc,
            "online_orders_hard_deleted": deleted.height,
            "online_orders_hard_deleted_seen_by_poller": deleted.filter(
                pl.col("order_id").is_in(polled_ids.implode())
            ).height,
            "online_orders_hard_deleted_seen_by_poller_gb": deleted.filter(
                gb & pl.col("order_id").is_in(polled_ids.implode())
            ).height,
            "gb_online_orders_in_eur": orders.filter(gb & (pl.col("currency") == "EUR")).height,
            "refunds_for_purchases_in_an_earlier_month": refunds.filter(
                pl.col("refund_ts").dt.month() != pl.col("bought").dt.month()
            ).height,
            "price_changes_in_window": mid_day.height,
            "price_changes_mid_day": mid_day.filter(pl.col("local_hour") == 13).height,
            "promotions_edited_after_launch": ref.promotion_versions.filter(pl.col("version") > 1).height,
            "products_moved_in_1_aug_reorg": _reorg_count(ref),
            "products_launched_in_window": launch.height,
            "pos_lines_sold_on_launch_day_before_catalogue_lists_them": launch_day_lines,
            "calendar_days_without_fx_rate": sum(1 for d in window_days if d not in set(fx_days)),
            "stores_snapshot_after_delivery": ref.stores.filter(
                pl.col("wms_snapshot_schedule") == "after_delivery_06_00"
            ).height,
            "inventory_adjustment_rows": inv.adjustments.height,
            "supplier_feed_malformed_rows": supply.defect_counts["malformed_rows"],
            "supplier_feed_duplicate_rows": supply.defect_counts["duplicate_rows"],
            "supplier_files_with_renamed_column": supply.defect_counts["renamed_column_files"],
            "clickstream_bot_hits": clicks.defect_counts["bot_hits"],
            "clickstream_sdk_retry_duplicates": clicks.defect_counts["sdk_retry_duplicates"],
            "clickstream_events_with_page_path": clicks.defect_counts["events_with_page_path"],
            "clickstream_events_with_consent": clicks.defect_counts["events_with_consent"],
        },
        "psp_gb_q3_settled": {
            k: {"rows": v["rows"], "gross_minor": v["gross_minor"], "fee_minor": v["fee_minor"]}
            for k, v in sorted(by_type.items())
        },
    }
    return m


def _reorg_count(ref) -> int:
    before = ref.product_snapshots.filter(pl.col("snapshot_date") == date(2026, 7, 31)).select(
        "product_id", "category", "subcategory"
    )
    after = ref.product_snapshots.filter(pl.col("snapshot_date") == date(2026, 8, 1)).select(
        "product_id", "category", "subcategory"
    )
    j = before.join(after, on="product_id", suffix="_new")
    return j.filter(
        (pl.col("category") != pl.col("category_new")) | (pl.col("subcategory") != pl.col("subcategory_new"))
    ).height


def _zone(name: str):
    from zoneinfo import ZoneInfo

    return ZoneInfo(name)


def _gbp(minor: int) -> str:
    sign = "−" if minor < 0 else ""
    return f"{sign}£{abs(minor) / 100:,.2f}"


def write_json(m: dict, path: Path) -> None:
    path.write_text(json.dumps(m, indent=2, default=str) + "\n")


def write_markdown(m: dict, path: Path) -> None:
    r = m["uk_net_revenue_q3"]
    d = m["defects"]
    st = m["stock_availability_q3"]
    cv = m["conversion_q3"]
    sp = m["supplier_fill_q3"]
    g = m["generator"]
    rows = "\n".join(
        f"| {i} | {x['label']} | {_gbp(x['amount_minor'])} | {x['share_of_correct']:+.4%} |"
        for i, x in enumerate(r["steps"], start=1)
    )
    psp_rows = "\n".join(
        f"| {k} | {v['rows']:,} | {_gbp(v['gross_minor'])} | {_gbp(v['fee_minor'])} |"
        for k, v in m["psp_gb_q3_settled"].items()
    )
    text = f"""# Injected defects

Generated by `datagen` (seed {g["seed"]}, scale {g["scale"]}, store fraction {g["store_fraction"]}). Every
number below comes from that run. `tests/test_datagen.py` re-computes the two UK revenue figures with
plain SQL over the files and checks the waterfall sums to the gap. Change the seed or scale
and the counts change; the UK revenue gap stays calibrated to {s.TARGET_UK_REVENUE_GAP:.1%}.

Window: {m["window"]["start"]} to {m["window"]["end"]}. Reporting quarter: Q3 2026
({m["window"]["quarter"][0]} to {m["window"]["quarter"][1]}). The legacy board-deck numbers were pulled on
{m["window"]["report_date"]}, using every file that had landed by then.

These are documented defects, not noise. Each one is something real systems do, and each one is
fixed in a specific part of the series.

## Headline number 1: UK net revenue, Q3 2026

| | GBP |
|---|---|
| Legacy board-deck number | {_gbp(r["legacy_board_deck_minor"])} |
| Correct number | {_gbp(r["correct_minor"])} |
| Gap | {_gbp(r["legacy_board_deck_minor"] - r["correct_minor"])} ({r["gap_share"]:+.3%} of correct) |

**Definitions.** Net revenue is the value of POS baskets and online orders, after discounts, minus
refunds, VAT-inclusive (the lab ignores VAT). The correct number counts each basket once
(`basket_id`), excludes orders that were cancelled and deleted, dates every sale and refund by its own
Europe/London business date, and converts euros at the rate in force on that date (the latest rate
published on or before it; there are no weekend rates). The legacy number sums raw POS events, reads
online orders from the nightly `updated_at` poller extracts, dates refunds by the original purchase,
converts euros at the latest rate on the report day, and cuts the quarter at UTC midnight.

**Waterfall.** Apply the fixes in this order and each step's amount is exact; the steps sum to the gap.

| Step | Fix | Removes from the legacy number | Share of correct |
|---|---|---|---|
{rows}

Larkspur UK has been cashless since 2024, so every UK payment goes through the PSP and the settlement
files cover all UK revenue. Settlements still differ from net revenue for legitimate reasons: PSP fees,
chargebacks, and timing (sales settle two weekdays after capture, online orders are captured at
fulfilment, so late-September sales settle in October and late-June sales in July). GB settlement rows
dated in Q3:

| Record type | Rows | Gross | Fees |
|---|---|---|---|
{psp_rows}

Fixed in: Part 4 (refunds as their own fact), Part 5 (FX and business dates), Part 6 (finding the
duplicates), Part 16 (CDC sees the deletes), Part 21 (the full reconciliation against settlement files).

## Headline number 2: stock availability, Q3 2026

| Method | Availability |
|---|---|
| Legacy dashboard: share of store-SKU-weeks where `SUM(on_hand_qty)` over the week is above zero | {st["legacy_weekly_sum"]:.1%} |
| Per day, using the snapshot as reported | {st["per_day_reported_snapshot"]:.1%} |
| Correct: per day, closing stock (`on_hand_qty - received_qty` for after-delivery stores) | {st["correct_close_of_day"]:.1%} |

Two defects. `on_hand_qty` is semi-additive: you can't add it up across days, and doing so hides any
stock-out that didn't last the whole week. And {d["stores_snapshot_after_delivery"]} stores snapshot at 06:00 after the
overnight delivery instead of at close, so their snapshot shows stock that was not on the shelf the day
before. A share of lines per country also hit 2–4 week supply outages, the empty shelves store
managers complained about. Fixed in: Part 4 (periodic snapshot facts), Part 5 (snapshot time).

## Headline number 3: online conversion, Q3 2026

| Team | Session definition | Bots | Conversion |
|---|---|---|---|
| Product | 30 minutes of inactivity per `anonymous_id` | excluded (user agent matches `{BOT_UA_PATTERN.replace("|", chr(92) + "|")}`) | {cv["product_team"]:.2%} |
| Marketing | one per `anonymous_id` per UTC calendar day | included | {cv["marketing"]:.2%} |

Marketing's daily sessions merge a buyer's earlier research visit into the buying visit, which raises
its rate; its {d["clickstream_bot_hits"]:,} bot hits (crawlers keep no cookies, so every hit is a new visitor) lower it
more. Fixed in: Part 6 (sessionization), Part 20 (the contract defines a session), Part 31 (one
metric definition).

## Supplier fill rate, Q3 2026 (clean data)

Fill rate (delivered / ordered, by expected date): {sp["fill_rate_q3"]:.1%}. On time in full: {sp["otif_q3"]:.1%}.

## Every injected defect

| Defect | Count | Where | Effect |
|---|---|---|---|
| Duplicate POS events after a till reconnects (new `event_id`, same `basket_id`) | {d["pos_duplicate_events_total"]:,} ({d["pos_duplicate_events_gb"]:,} UK) | `raw/pos/events/` | Revenue step 1 |
| Offline tills uploading hours late | {d["pos_offline_baskets"]:,} baskets, {d["pos_late_uploads_crossing_local_midnight"]:,} land on a later local date | `raw/pos/events/upload_date=…` | No effect on the quarter (the report ran after everything landed); breaks naive daily loads (Parts 2, 18) |
| POS baskets whose UTC date differs from the local business date | {d["pos_baskets_utc_date_differs_from_local"]:,} | all POS events | Revenue step 5 for UK online orders and 24-hour stores |
| Online orders cancelled before fulfilment, then hard-deleted | {d["online_orders_hard_deleted"]:,} ({d["online_orders_hard_deleted_seen_by_poller"]:,} were seen by the poller first; {d["online_orders_hard_deleted_seen_by_poller_gb"]:,} UK) | `raw/oltp/orders_poll/` keeps them, `raw/oltp/orders/` doesn't | Revenue step 2 |
| UK online orders paid in euros | {d["gb_online_orders_in_eur"]:,} | `raw/oltp/orders/` | Revenue step 4 |
| Refunds for purchases made in an earlier month | {d["refunds_for_purchases_in_an_earlier_month"]:,} | `raw/oltp/refunds/` | Revenue step 3 |
| Price changes during the window | {d["price_changes_in_window"]:,} ({d["price_changes_mid_day"]:,} at 13:00 local, mid-trading) | `raw/pricing/price_changes/` | Point-in-time joins (Part 4) |
| Promotions edited after launch (discount changed mid-flight) | {d["promotions_edited_after_launch"]:,} | `raw/promotions/promotion_versions.csv` | As-of joins (Parts 4, 6) |
| Products moved in the 1 August catalogue reorganisation | {d["products_moved_in_1_aug_reorg"]:,} | `raw/catalog/products/snapshot_date=…` | SCD Type 2 (Part 4) |
| POS lines sold on a product's launch day, before the catalogue extract lists it | {d["pos_lines_sold_on_launch_day_before_catalogue_lists_them"]:,} | POS vs catalogue | Early-arriving facts (Part 4) |
| Calendar days without an FX rate (weekends) | {d["calendar_days_without_fx_rate"]:,} | `raw/fx/eod_rates.csv` | As-of joins, not equi-joins (Part 5) |
| Stores snapshotting stock after the overnight delivery | {d["stores_snapshot_after_delivery"]:,} | `raw/inventory/snapshots/` | Stock availability |
| Stock adjustments (shrink: damage, expiry, unknown loss) | {d["inventory_adjustment_rows"]:,} | `raw/inventory/adjustments/` | Shrink (Part 4) |
| Malformed supplier rows (impossible date, `n/a`, negative quantity, missing PO) | {d["supplier_feed_malformed_rows"]:,} | `raw/suppliers/deliveries/` | Ingestion and contracts (Parts 15, 20) |
| Duplicated supplier rows (same file or re-sent next day) | {d["supplier_feed_duplicate_rows"]:,} | `raw/suppliers/deliveries/` | Fill rate overstated if loaded naively |
| Supplier {RENAMING_SUPPLIER} renames `Delivered Qty` to `Qty Delivered` from {RENAME_FROM} | {d["supplier_files_with_renamed_column"]:,} files | `raw/suppliers/deliveries/{RENAMING_SUPPLIER}/` | Schema drift (Parts 15, 20) |
| Suppliers sending `DD/MM/YYYY` dates | {len(DMY_SUPPLIERS)} suppliers | `raw/suppliers/deliveries/` | Parsing (Part 15) |
| Clickstream bot hits | {d["clickstream_bot_hits"]:,} | `raw/clickstream/events/` | Conversion |
| Clickstream SDK retries (identical event resent) | {d["clickstream_sdk_retry_duplicates"]:,} | `raw/clickstream/events/` | Event counts, not sessions |
| `page` renamed to `page_path` (web on {CLICK_RENAME_FROM}, app as users update to 5.0) | {d["clickstream_events_with_page_path"]:,} events use the new name | `raw/clickstream/events/` | Schema drift (Parts 6, 20) |
| `consent` block added on {CONSENT_FROM} | {d["clickstream_events_with_consent"]:,} events carry it | `raw/clickstream/events/` | Schema evolution |

Sources added in later seasons (marketplace sellers API, sensor telemetry, support tickets) bring their
own defects; they are documented here when their output modes are added.
"""
    path.write_text(text)
