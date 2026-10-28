"""Part 14: the design review's back-of-envelope sizing, derived from the lab's own files.

uv run python -m scripts.part14_sizing estimate   # volumes, bytes per row, storage over retention, files, scans, 9 a.m.
uv run python -m scripts.part14_sizing incident   # where the three headline numbers stand at the end of Season 1

`estimate` reads the raw zone on disk (`make data`) and Iceberg metadata through Polaris (`make up-core`; Part 10's
part10.pos_monthly and Part 7's part07.clicks_v2). It plans scans from manifests and reads no data files.
"""

import argparse
import gzip
import json
import math
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"
MIB, GIB, TIB = 2**20, 2**30, 2**40

SCALE = 50  # the lab is ~1/50 of Larkspur's real volume: ~30 vs ~1,500 baskets per store per day (Part 10)

# The review's inputs from Farah's team. Assumptions stated in the post, not measurements.
SALES_YEARS = 7  # sales lines and refunds kept online for finance
CLICKS_MONTHS = 25  # clickstream: two years of year-on-year comparisons, plus a month
GROWTH = 0.30  # volume growth per year, used only for the "what would change it" triggers
VIEWERS, TILES = 300, 12  # store managers at 9 a.m., tiles per dashboard (Part 13)

# Measured or documented in earlier parts.
TARGET_FILE = 512 * MIB  # Iceberg's write.target-file-size-bytes default (Part 10)
PARTITION_TARGET = GIB  # the playbook's ~1 GB per partition (Part 10)
LATE_DAYS = 2  # Part 2: an upload may hold sales up to 2 business days old
QPS_32_CLIENTS = {  # Part 13, step 6: range over three saved runs at 32 concurrent clients
    "Trino over Iceberg": (34.9, 39.3),
    "ClickHouse serving table": (1399.0, 1671.0),
}
USD_PER_TIB = 6.25  # BigQuery on-demand, Iowa (Part 13): a per-byte yardstick, not Larkspur's bill
DAYS_PER_YEAR = 365.25
DAYS_PER_MONTH = DAYS_PER_YEAR / 12

# The questions Parts 10, 11 and 13 measured, as Iceberg filters plus the columns each one reads.
QUERIES = {
    "revenue (Sep)": (
        "business_date >= '2026-09-01' and business_date <= '2026-09-30'",
        ["country", "currency", "business_date", "amount_minor"],
    ),
    "store promo (12 mo)": (
        "store_id = 'GB-0042' and discount_minor > 0 and business_date >= '2025-10-01'",
        ["store_id", "business_date", "discount_minor"],
    ),
    "store tile (4 wk)": (
        "store_id = 'GB-0042' and business_date >= '2026-09-03' and business_date <= '2026-09-30'",
        ["store_id", "business_date", "amount_minor"],
    ),
    "history (4 yr)": ("True", ["business_date", "country", "currency", "amount_minor", "discount_minor"]),
}


# ---------- measurements ----------
def manifest() -> dict:
    return json.loads((RAW / "_manifest.json").read_text())


def gz_stats(folder: Path) -> dict:
    """Compressed bytes, uncompressed bytes and lines of every .jsonl.gz under a folder, plus its partition count."""
    files = sorted(folder.glob("*/*.jsonl.gz"))
    gz = raw = lines = 0
    for f in files:
        gz += f.stat().st_size
        with gzip.open(f, "rb") as fh:
            while chunk := fh.read(8 * MIB):
                raw += len(chunk)
                lines += chunk.count(b"\n")
    return {"partitions": len({f.parent for f in files}), "gz": gz, "json": raw, "lines": lines}


def snapshot_stats() -> dict:
    import pyarrow.parquet as pq

    files = sorted((RAW / "inventory/snapshots").glob("*/stock.parquet"))
    rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    return {"days": len(files), "rows": rows, "bytes": sum(f.stat().st_size for f in files)}


def table_stats(name: str) -> dict:
    """Rows and bytes of a table's live data files, from its manifests."""
    from scripts.part07_iceberg import catalog

    files = [f for f in catalog().load_table(name).inspect.files().to_pylist() if f["content"] == 0]
    rows, size = sum(f["record_count"] for f in files), sum(f["file_size_in_bytes"] for f in files)
    return {"files": len(files), "rows": rows, "bytes": size}


def planned_scans() -> dict:
    """Files and column bytes each query would read from pos_monthly, after partition and file-statistics pruning."""
    from scripts.part07_iceberg import catalog

    table = catalog().load_table("part10.pos_monthly")
    field = {f.name: f.field_id for f in table.schema().fields}
    out = {}
    for name, (where, columns) in QUERIES.items():
        files = [task.file for task in table.scan(row_filter=where).plan_files()]
        out[name] = {
            "files": len(files),
            "file_bytes": sum(f.file_size_in_bytes for f in files),
            "column_bytes": sum(f.column_sizes.get(field[c], 0) for f in files for c in columns),
        }
    return out


def measure() -> dict:
    m = manifest()
    start, end = (date.fromisoformat(m["window"][k]) for k in ("start", "end"))
    return {
        "pos_days": (end - start).days + 1,
        "pos_lines": m["volumes"]["pos_lines"],
        "pos_raw": gz_stats(RAW / "pos/events"),
        "clicks_raw": gz_stats(RAW / "clickstream/events"),
        "snapshot": snapshot_stats(),
        "sales_table": table_stats("part10.pos_monthly"),
        "clicks_table": table_stats("part07.clicks_v2"),
        "scans": planned_scans(),
    }


# ---------- arithmetic ----------
def rebuild_bytes_per_night(day_bytes: float, month_partitions: bool) -> float:
    """Average bytes a nightly partition overwrite rewrites over a year: each run rebuilds the business dates its
    upload touched (the day and LATE_DAYS before it) by replacing every partition they fall in (Parts 2 and 12)."""
    total, runs = 0.0, [date(2026, 1, 1) + timedelta(n) for n in range(365)]
    for run in runs:
        touched = {run - timedelta(k) for k in range(LATE_DAYS + 1)}
        if not month_partitions:
            total += len(touched) * day_bytes
            continue
        for month_start in {d.replace(day=1) for d in touched}:  # a month partition holds every day loaded so far
            month_end = (month_start + timedelta(32)).replace(day=1) - timedelta(1)
            total += (min(run, month_end) - month_start).days + 1
    return total / len(runs) * (day_bytes if month_partitions else 1)


def years_until(now: float, threshold: float) -> float:
    return math.log(threshold / now) / math.log(1 + GROWTH)


def estimate(x: dict) -> dict:
    pos, clicks, snap = x["pos_raw"], x["clicks_raw"], x["snapshot"]
    lines_day = x["pos_lines"] / x["pos_days"]
    clicks_day = clicks["lines"] / clicks["partitions"]
    sales_b = x["sales_table"]["bytes"] / x["sales_table"]["rows"]
    clicks_b = x["clicks_table"]["bytes"] / x["clicks_table"]["rows"]
    snap_b = snap["bytes"] / snap["rows"]
    snap_day = snap["rows"] / snap["days"]
    per_day = {  # full-volume bytes per day; snapshots scale with store-SKUs, not baskets (x1)
        "sales lines (Iceberg)": lines_day * SCALE * sales_b,
        "clickstream (Iceberg)": clicks_day * SCALE * clicks_b,
        "stock snapshot (x1)": snap_day * snap_b,
        "raw POS (gzip JSONL)": pos["gz"] / x["pos_days"] * SCALE,
        "raw clickstream (gzip)": clicks["gz"] / clicks["partitions"] * SCALE,
    }
    keep = {
        "sales lines (Iceberg)": SALES_YEARS * DAYS_PER_YEAR,
        "clickstream (Iceberg)": CLICKS_MONTHS * DAYS_PER_MONTH,
        "stock snapshot (x1)": SALES_YEARS * DAYS_PER_YEAR,
        "raw POS (gzip JSONL)": SALES_YEARS * DAYS_PER_YEAR,  # raw is kept as long as what is rebuilt from it (Part 2)
        "raw clickstream (gzip)": CLICKS_MONTHS * DAYS_PER_MONTH,
    }
    return {
        "rows": {
            "POS lines": (lines_day, pos["gz"] / x["pos_lines"], pos["json"] / x["pos_lines"], sales_b),
            "clickstream events": (
                clicks_day,
                clicks["gz"] / clicks["lines"],
                clicks["json"] / clicks["lines"],
                clicks_b,
            ),
            "stock snapshot rows": (snap_day, None, None, snap_b),  # raw zstd Parquet, not an Iceberg table,
        },
        "per_day": per_day,
        "retained": {k: per_day[k] * keep[k] for k in per_day},
        "keep_days": keep,
        "rebuild": {
            "month": rebuild_bytes_per_night(per_day["sales lines (Iceberg)"], True),
            "day": rebuild_bytes_per_night(per_day["sales lines (Iceberg)"], False),
        },
        "triggers": {
            "sales day partition reaches 1 GiB": years_until(per_day["sales lines (Iceberg)"], PARTITION_TARGET),
            "clickstream day reaches 1 GiB": years_until(per_day["clickstream (Iceberg)"], PARTITION_TARGET),
        },
    }


# ---------- reports ----------
def print_estimate() -> dict:
    x = measure()
    e = estimate(x)
    print(
        f"lab basis: {x['pos_days']} POS business days, {x['clicks_raw']['partitions']} clickstream days, "
        f"{x['snapshot']['days']} stock snapshots (seed 42); full volume = lab x {SCALE} (snapshots x 1)"
    )
    print(f"\n{'per row':<21}{'lab/day':>10}{'full/day':>12}{'raw gz B':>10}{'raw JSON B':>12}{'Parquet B':>11}")
    for name, (per_day, gz, js, pq) in e["rows"].items():
        full = per_day * (1 if "stock" in name else SCALE)
        raw = f"{gz:>10.1f}{js:>12.1f}" if gz else f"{'-':>10}{'-':>12}"
        print(f"{name:<21}{per_day:>10,.0f}{full:>12,.0f}{raw}{pq:>11.2f}")
    _, gz, js, pq = e["rows"]["POS lines"]
    print(f"POS lines: raw JSON is {js / pq:.0f}x the Iceberg Parquet bytes, gzip {gz / pq:.1f}x")

    print(f"\n{'full volume':<24}{'per day':>11}{'per month':>11}{'kept':>11}{'at retention':>14}")
    for name, b in e["per_day"].items():
        days = e["keep_days"][name]
        kept = f"{days / DAYS_PER_YEAR:.0f} years" if days > 400 * 3 else f"{days / DAYS_PER_MONTH:.0f} months"
        print(
            f"{name:<24}{b / MIB:>7.1f} MiB{b * DAYS_PER_MONTH / GIB:>7.2f} GiB{kept:>11}"
            f"{e['retained'][name] / GIB:>10.1f} GiB"
        )
    print(f"{'total':<24}{'':>33}{sum(e['retained'].values()) / GIB:>10.1f} GiB")

    month = e["per_day"]["sales lines (Iceberg)"] * DAYS_PER_MONTH
    print("\nsales lines, files into the open month partition (one data file per commit, Part 10)")
    print(f"{'commit cadence':<18}{'files/day':>10}{'files/month':>13}{'after compaction':>18}")
    for label, commits in [("nightly batch", 1), ("hourly", 24), ("every 5 minutes", 288)]:
        print(f"{label:<18}{commits:>10,}{commits * DAYS_PER_MONTH:>13,.0f}{math.ceil(month / TARGET_FILE):>18}")
    r = e["rebuild"]
    print(
        f"nightly rebuild of {LATE_DAYS + 1} business dates, average rewritten: month partitions "
        f"{r['month'] / GIB:.2f} GiB, day partitions {r['day'] / MIB:.0f} MiB"
    )

    print(f"\nscans planned on part10.pos_monthly (manifests only), then x {SCALE}")
    print(f"{'query':<21}{'files':>6}{'lab file MiB':>14}{'lab column MiB':>16}{'full column GiB':>17}")
    for name, s in x["scans"].items():
        print(
            f"{name:<21}{s['files']:>6}{s['file_bytes'] / MIB:>14.1f}{s['column_bytes'] / MIB:>16.2f}"
            f"{s['column_bytes'] * SCALE / GIB:>17.2f}"
        )

    burst = VIEWERS * TILES
    tile = x["scans"]["store tile (4 wk)"]["column_bytes"] * SCALE
    print(f"\n9 a.m.: {VIEWERS} managers x {TILES} tiles = {burst:,} queries")
    for name, (lo, hi) in QPS_32_CLIENTS.items():
        rate = f"{lo:,g}-{hi:,g} q/s"
        print(f"  {name:<26}{rate:>17} at 32 clients -> {burst / hi:.1f}-{burst / lo:.1f} s to drain")
    morning = burst * tile
    print(f"  every tile from the lines at full volume: at most {morning / GIB:.1f} GiB of columns a morning")
    print(
        f"  per-byte yardstick at USD {USD_PER_TIB}/TiB: a month of such mornings USD "
        f"{morning * 30 / TIB * USD_PER_TIB:.0f}, the 4-year history query USD "
        f"{x['scans']['history (4 yr)']['column_bytes'] * SCALE / TIB * USD_PER_TIB:.2f} a run"
    )

    print(f"\nat {GROWTH:.0%} growth a year")
    for name, years in e["triggers"].items():
        print(f"  {name:<36}in {years:.1f} years")
    return {"measured": x, "estimate": e}


# Where each revenue step was found or fixed in Season 1 (progress log, incident tracker).
STEP_STATUS = {
    "duplicate_pos_events": ("found, Part 6", "prevent in Part 21"),
    "hard_deleted_orders": ("open", "Part 16 (CDC)"),
    "refund_dating": ("fixed, Part 4", ""),
    "fx_today_rate": ("fixed, Part 5", ""),
    "utc_business_date": ("fixed, Part 5", ""),
}


def incident() -> dict:
    m = manifest()
    uk = m["uk_net_revenue_q3"]
    gap = uk["legacy_board_deck_minor"] - uk["correct_minor"]
    print(
        f"UK net revenue Q3: board deck {uk['legacy_board_deck_minor'] / 100:,.2f}, "
        f"correct {uk['correct_minor'] / 100:,.2f}"
    )
    print(f"{'step':<64}{'GBP':>12}  {'Season 1':<15}{'Season 2'}")
    accounted = 0
    for s in uk["steps"]:
        status, next_ = STEP_STATUS[s["key"]]
        accounted += s["amount_minor"] if status != "open" else 0
        print(f"{s['label']:<64}{s['amount_minor'] / 100:>12,.2f}  {status:<15}{next_}".rstrip())
    total = sum(s["amount_minor"] for s in uk["steps"])
    print(f"{'sum of steps = gap':<64}{total / 100:>12,.2f}  ({total / uk['correct_minor']:.3%} of correct)")
    print(f"{'explained in Season 1':<64}{accounted / 100:>12,.2f}")
    print(f"{'still open':<64}{(gap - accounted) / 100:>12,.2f}")
    st, cv = m["stock_availability_q3"], m["conversion_q3"]
    print(
        f"stock availability: {st['legacy_weekly_sum']:.1%} -> {st['per_day_reported_snapshot']:.1%} (Part 4) "
        f"-> {st['correct_close_of_day']:.1%} (Part 5): fixed"
    )
    print(
        f"conversion: {cv['product_team']:.2%} vs {cv['marketing']:.2%}: explained (Part 6); one definition in 20, 31"
    )
    return {"gap": gap, "total": total, "accounted": accounted}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["estimate", "incident"])
    a = p.parse_args()
    {"estimate": print_estimate, "incident": incident}[a.command]()
