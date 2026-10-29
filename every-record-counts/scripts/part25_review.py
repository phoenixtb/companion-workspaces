"""Part 25: Design Review II. The batch platform sized from its own tables, and the quarter's sign-off: every figure in
the board pack recomputed from the lakehouse, then Part 21's proposed restatement approved, once.

uv run python -m scripts.part25_review estimate                       # rows and bytes a day per source: lab, x50, x500
uv run python -m scripts.part25_review signoff                        # recompute the board pack; changes nothing
uv run python -m scripts.part25_review signoff --approve --by NAME    # ... and approve the restatement if all pass
uv run python -m scripts.part25_review history                        # the restatement table's commits, proposal first

Core only (`make up-core`): PyIceberg through Polaris (Part 9's principals, vended credentials) and DuckDB on the host.
"""

import argparse
import contextlib
import io
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from pyiceberg.expressions import And, EqualTo

from scripts import part09_catalog as cat9
from scripts import part17_dbt as lab17
from scripts import part20_contracts as lab20
from scripts import part21_reconcile as lab21

ROOT = Path(__file__).resolve().parents[1]
BOARD_PACK = ROOT / "governance/board-pack-2026-q3.json"
FULL, TEN_X = 50, 500  # the lab is ~1:50 of Larkspur (Part 0); 10x Larkspur is 500 times the lab

# ---------- 1. sizing: what a day of each source is, in rows and Iceberg bytes ----------

# (label, table, day expression, row filter in SQL, scales with baskets?). CDC counts streamed changes, not the
# initial snapshot reads (`_op = 'r'`), dated by the business timestamps: Part 16's replay committed them all in one day
SOURCES = [
    ("POS events (raw)", "raw.pos_uploads", "upload_date", None, True),
    ("POS dispositions (silver)", "silver.pos_event_dispositions", "upload_date", None, True),
    ("sale lines (gold fact)", "gold.fct_sales_line", "CAST(event_ts AS DATE)", None, True),
    ("order changes (CDC)", "raw.orders_changes", "CAST(updated_at AS DATE)", "_op <> 'r'", True),
    (
        "payment changes (CDC)",
        "raw.payments_changes",
        "CAST(greatest(authorised_at, captured_at, voided_at) AS DATE)",
        "_op <> 'r'",
        True,
    ),
    ("refund changes (CDC)", "raw.refunds_changes", "CAST(refund_ts AS DATE)", "_op <> 'r'", True),
    ("PSP settlement rows", "raw.psp_settlements", "settlement_date", None, True),
    ("clickstream events", "raw.clickstream_events", "event_date", None, True),
    ("stock snapshot rows", "raw.stock_snapshots", "business_date", None, False),
    ("supplier delivery rows", "raw.supplier_deliveries", "file_date", None, False),
]


def estimate() -> list[dict]:
    """Rows a day from each table's own dates; bytes a row from the current snapshot's summary (no data files read
    beyond the one or two columns that give the day)."""
    cat = cat9.pyiceberg("pipeline")
    out = []
    for label, name, day, where, scales in SOURCES:
        t = cat.load_table(name)
        summary = t.current_snapshot().summary
        size, records = int(summary["total-files-size"]), int(summary["total-records"])
        cols = {f.name for f in t.schema().fields if f.name in day or (where and f.name in where)}
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        con.register("t", t.scan(selected_fields=tuple(sorted(cols))).to_arrow())
        rows, days = con.execute(
            f"SELECT count(*), count(DISTINCT {day}) FROM t {'WHERE ' + where if where else ''}"
        ).fetchone()
        con.close()
        per_day = rows / days
        factor = (FULL, TEN_X) if scales else (1, 10)
        out.append(
            {
                "source": label,
                "table": name,
                "lab_rows_per_day": round(per_day),
                "days": days,
                "bytes_per_row": size / records,
                "full_rows_per_day": round(per_day * factor[0]),
                "ten_x_rows_per_day": round(per_day * factor[1]),
                "full_mib_per_day": per_day * factor[0] * size / records / 2**20,
                "ten_x_mib_per_day": per_day * factor[1] * size / records / 2**20,
            }
        )
    print("rows and Iceberg bytes a day; full = lab x 50 (stock, suppliers x 1), 10x Larkspur = full x 10")
    head = ("source", "lab/day", "days", "B/row", "full/day", "MiB", "10x/day", "MiB")
    print("{:<27} {:>9} {:>5} {:>6} {:>11} {:>7} {:>12} {:>8}".format(*head))
    for r in out:
        print(
            f"{r['source']:<27} {r['lab_rows_per_day']:>9,} {r['days']:>5} {r['bytes_per_row']:>6.1f} "
            f"{r['full_rows_per_day']:>11,} {r['full_mib_per_day']:>7.1f} {r['ten_x_rows_per_day']:>12,} "
            f"{r['ten_x_mib_per_day']:>8.1f}"
        )
    full = sum(r["full_mib_per_day"] for r in out)
    ten = sum(r["ten_x_mib_per_day"] for r in out)
    print(f"{'all of the above':<27} {'':>9} {'':>5} {'':>6} {'':>11} {full:>7.1f} {'':>12} {ten:>8.1f}")
    biggest = max(out, key=lambda r: r["ten_x_mib_per_day"])
    print(f"largest day at 10x: {biggest['source']}, {biggest['ten_x_mib_per_day'] / 1024:.2f} GiB")
    return out


# ---------- 2. the sign-off: recompute every figure in the board pack, then approve the restatement ----------


def quiet(fn, *args):
    """Run one of Part 21's steps without its printout; the sign-off prints one line per check."""
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*args)


def gold_sql(path: Path) -> list[list]:
    """Part 17's check queries (written for Trino as the analyst) on DuckDB over PyIceberg scans of the same tables."""
    cat = cat9.pyiceberg("analyst")
    con = duckdb.connect()
    needed = {
        "rpt_daily_net_revenue": ("country", "business_date", "net_minor"),
        "fct_stock_daily": ("date_key", "on_hand_qty", "closing_on_hand_qty"),
        "dim_date": ("date_key", "quarter"),
        "fct_po_line": ("expected_date_key", "ordered_qty", "delivered_qty", "days_late"),
    }
    for table, cols in needed.items():
        con.register(table, cat.load_table(f"gold.{table}").scan(selected_fields=cols).to_arrow())
    return [con.execute(s.replace("lakehouse_analyst.gold.", "")).fetchall() for s in lab17.statements(path)]


def latest_controls() -> tuple[int, int, int]:
    """Part 21's control rows, the latest verdict for each check, day and country: (rows, run days, failures)."""
    con = duckdb.connect()
    con.register("c", lab21.catalog().load_table(lab21.CONTROLS).scan().to_arrow())
    return con.execute(
        """SELECT count(*), (SELECT count(DISTINCT run_day) FROM c), count(*) FILTER (WHERE status = 'FAIL')
           FROM (SELECT status FROM c
                 QUALIFY row_number() OVER (PARTITION BY check_name, scope_date, country
                                            ORDER BY checked_at DESC) = 1)"""
    ).fetchone()


def restatement(table: str = lab21.RESTATEMENTS) -> dict:
    t = lab21.catalog().load_table(table)
    rows = t.scan(row_filter=And(EqualTo("period", "2026-Q3"), EqualTo("country", "GB"))).to_arrow().to_pylist()
    if len(rows) != 1:
        raise SystemExit(f"{table}: expected one 2026-Q3 GB restatement, found {len(rows)}; run Part 21's restate")
    return rows[0]


def checks(table: str = lab21.RESTATEMENTS) -> list[tuple[str, str, bool]]:
    """(check, what was found, passed) for every figure the board pack states."""
    pack = json.loads(BOARD_PACK.read_text())
    want = pack["uk_net_revenue_q3_minor"]
    found = []
    w = quiet(lab21.waterfall)
    found.append(
        ("waterfall: board deck -> correct",
         f"{lab21.money(w['correct'])}, unexplained {lab21.money(w['unexplained'])}",
         w["correct"] == want and w["unexplained"] == 0)
    )  # fmt: skip
    b = quiet(lab21.bridge)
    found.append(
        ("bridge: PSP settled -> gold", f"{lab21.money(b['supported'])} supported, {b['bugs']} bug records",
         b["supported"] == b["gold"] == want and b["bugs"] == 0)
    )  # fmt: skip
    refs = {ref or "main": lab21.q3_by_ref(ref)["GB"] for ref in (lab21.QUARTER_TAG, "published", None)}
    found.append(
        ("report: tag, published, main", " / ".join(lab21.money(v) for v in refs.values()),
         set(refs.values()) == {want})
    )  # fmt: skip
    rows, run_days, failures = latest_controls()
    found.append(("daily controls, latest verdicts", f"{rows:,} rows over {run_days} run days, {failures} FAIL",
                  failures == 0 and rows > 0))  # fmt: skip
    r = restatement(table)
    found.append(
        ("restatement in " + table, f"{lab21.money(r['as_published_minor'])} -> {lab21.money(r['restated_minor'])}, "
         f"{r['status']}", r["restated_minor"] == want and r["as_published_minor"] == pack["board_deck_minor"])
    )  # fmt: skip
    c = quiet(lab20.conversion)
    found.append(
        ("conversion (sessions contract)", f"{c['converted']:,} / {c['sessions']:,} = {c['conversion_pct']:.2f}%",
         c["conversion_pct"] == pack["conversion_pct"])
    )  # fmt: skip
    _revenue, stock, fill = gold_sql(ROOT / "sql/part17/check.sql")
    at_close = round(100 * stock[0][1], 1)
    found.append(("stock availability at close", f"{at_close}% (as reported {100 * stock[0][0]:.1f}%)",
                  at_close == pack["stock_available_at_close_pct"]))  # fmt: skip
    fill_pct, otif_pct = round(100 * fill[0][0], 1), round(100 * fill[0][1], 1)
    found.append(("supplier fill rate / OTIF", f"{fill_pct}% / {otif_pct}%",
                  (fill_pct, otif_pct) == (pack["supplier_fill_pct"], pack["supplier_otif_pct"])))  # fmt: skip
    return found


def approve(found: list[tuple[str, str, bool]], by: str, table: str = lab21.RESTATEMENTS) -> dict:
    """Move the restatement from proposed to approved: one commit, only if every check passed, never twice. The
    approval's who and when go into the snapshot summary; the proposal stays readable in the table's history. An
    approved restatement is never re-written: if its checks fail later, that is reported for a new restatement."""
    failed = [name for name, _, ok in found if not ok]
    t = lab21.catalog().load_table(table)
    row = restatement(table)
    if (
        row["status"] == "approved"
    ):  # status first: an approval stands; checks that drift later are reported, not hidden
        s = next(s for s in reversed(t.snapshots()) if "larkspur.approved-by" in s.summary.additional_properties)
        p = s.summary.additional_properties
        if failed:
            print(f"approved earlier by {p['larkspur.approved-by']} (snapshot {s.snapshot_id}); checks now fail: "
                  f"{', '.join(failed)}; nothing written")  # fmt: skip
            return {"status": "approved", "snapshot": s.snapshot_id, "new_commit": False, "drift": failed}
        print(
            f"already approved by {p['larkspur.approved-by']} at {p['larkspur.approved-at']}"
            f" (snapshot {s.snapshot_id}); nothing written"
        )
        return {"status": "approved", "snapshot": s.snapshot_id, "new_commit": False, "drift": []}
    if failed:
        raise SystemExit(f"not approved: {len(failed)} check(s) failed: {', '.join(failed)}")
    if row["status"] != "proposed":
        raise SystemExit(f"status is {row['status']!r}; only a proposed restatement can be approved")
    at = datetime.now(UTC).replace(microsecond=0).isoformat()
    record = t.scan(row_filter=And(EqualTo("period", "2026-Q3"), EqualTo("country", "GB"))).to_arrow()
    record = record.set_column(record.schema.get_field_index("status"), "status", [["approved"]])
    before = {s.snapshot_id for s in t.snapshots()}
    t.overwrite(
        record.cast(t.schema().as_arrow()),
        overwrite_filter=And(EqualTo("period", "2026-Q3"), EqualTo("country", "GB")),
        snapshot_properties={
            "larkspur.approved-by": by,
            "larkspur.approved-at": at,
            "larkspur.approval-checks": f"{len(found)} of {len(found)} passed",
        },
    )
    t = lab21.catalog().load_table(table)
    new = [s for s in t.snapshots() if s.snapshot_id not in before]
    print(f"approved by {by} at {at}: {len(new)} new snapshot(s) on {table}, now {t.current_snapshot().snapshot_id}")
    return {"status": "approved", "snapshot": t.current_snapshot().snapshot_id, "new_commit": True, "drift": []}


def history(table: str = lab21.RESTATEMENTS) -> list[tuple]:
    """Each snapshot of the restatement table: when, what, the row's status as of then, and who approved it."""
    t = lab21.catalog().load_table(table)
    out = []
    for s in t.snapshots():
        status = t.scan(snapshot_id=s.snapshot_id, selected_fields=("status",)).to_arrow().column("status").to_pylist()
        p = s.summary.additional_properties
        at = datetime.fromtimestamp(s.timestamp_ms / 1000, UTC).isoformat(timespec="seconds")
        out.append((s.snapshot_id, at, s.summary.operation.value, status, p.get("larkspur.approved-by", "")))
        print(
            f"{s.snapshot_id:>20}  {at}  {s.summary.operation.value:<9} {','.join(status) or '(none)':<9} {out[-1][4]}"
        )
    return out


def signoff(approve_as: str | None = None, table: str = lab21.RESTATEMENTS) -> dict:
    started = time.monotonic()
    found = checks(table)
    print("Q3 2026 board pack, recomputed from the lakehouse")
    for name, what, ok in found:
        print(f"{'pass' if ok else 'FAIL'}  {name:<33} {what}")
    print(f"{sum(ok for *_, ok in found)} of {len(found)} passed; {time.monotonic() - started:.1f} s")
    result = {"checks": found}
    if approve_as:
        result |= approve(found, approve_as, table)
    return result


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["estimate", "signoff", "history"])
    p.add_argument("--approve", action="store_true")
    p.add_argument("--by")
    a = p.parse_args()
    if a.command == "estimate":
        estimate()
    elif a.command == "history":
        history()
    else:
        if a.approve and not a.by:
            raise SystemExit("--approve needs --by NAME")
        r = signoff(a.by if a.approve else None)
        sys.exit(0 if all(ok for *_, ok in r["checks"]) else 1)


if __name__ == "__main__":
    main()
