"""Part 21: every record counts. Settlement files landed with audit columns, POS events accepted once by their natural
key, the UK revenue waterfall and the settlement bridge computed record by record, and a daily control table that
gates publishing.

uv run python -m scripts.part21_reconcile land-psp [--until D]   # PSP settlement files -> raw.psp_settlements
uv run python -m scripts.part21_reconcile accept --day D | --all  # dispositions -> silver.pos_event_dispositions
uv run python -m scripts.part21_reconcile waterfall               # board deck -> correct, five steps vs the manifest
uv run python -m scripts.part21_reconcile bridge                  # PSP settled -> gold net revenue, every record
uv run python -m scripts.part21_reconcile control --day D         # gold.daily_controls for upload day D; exit 1 on FAIL
uv run python -m scripts.part21_reconcile inject resend|hold|restore --day D   # Hands-On: change one upload day
uv run python -m scripts.part21_reconcile explain                 # Trino plan: one day of gold's input view
uv run python -m scripts.part21_reconcile restate                 # tag the reconciled quarter; as published vs now
uv run python -m scripts.part21_reconcile logs TASK --day D         # a task's output in the newest Airflow run for D
uv run python -m scripts.part21_reconcile reset                   # drop Part 21's tables and the tag

Inside the Airflow containers the endpoints come from LAB_* variables (compose.yaml), as in Part 19.
"""

import argparse
import functools
import gzip
import hashlib
import json
import operator
import shutil
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Or

from scripts import part09_catalog as cat9

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql/part21"
RAW = ROOT / "data/raw"
LAB = ROOT / "data/lab/part21"
PSP_FILES = RAW / "psp/settlements"
PSP_TABLE = "raw.psp_settlements"
DISPOSITIONS = "silver.pos_event_dispositions"
CONTROLS = "gold.daily_controls"
REPORT = "gold.rpt_daily_net_revenue_v2"
Q3 = (date(2026, 7, 1), date(2026, 9, 30))
DEDUP_WINDOW_DAYS = 3  # re-sends arrive 1-30 minutes after the first copy (Part 6); late uploads within a day (Part 18)
CLOSED_AFTER_DAYS = 1  # a UTC day's uploads are complete by the end of the next upload day (Part 18's lateness)
CONTROL_WINDOW_DAYS = 7  # every run re-checks the last week of closed days: a loss found late still fails


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def catalog():
    return cat9.pyiceberg("pipeline")


def register(con, name: str, table: str, row_filter=None, columns: tuple[str, ...] = ("*",), ref: str | None = None):
    """A PyIceberg scan (deletes applied) as a DuckDB view: `columns` and `row_filter` are pushed into the scan."""
    t = catalog().load_table(table)
    snap = t.metadata.snapshot_by_name(ref).snapshot_id if ref else None
    scan = (
        t.scan(row_filter=row_filter, selected_fields=columns, snapshot_id=snap)
        if row_filter is not None
        else (t.scan(selected_fields=columns, snapshot_id=snap))
    )
    con.register(name, scan.to_arrow())


OPS = {
    "=": (EqualTo, pc.equal),
    ">=": (GreaterThanOrEqual, pc.greater_equal),
    "<": (LessThan, pc.less),
    "<=": (LessThanOrEqual, pc.less_equal),
}


def scan(table: str, columns: tuple[str, ...], *any_of: list[tuple]) -> pa.Table:
    """Rows of `table` matching any of the condition groups (each a list of (column, op, value), all must hold), with
    only `columns`. The Iceberg filter prunes files; then each file is streamed one batch at a time and filtered, so a
    task holds a batch, not a whole file (PyIceberg's to_arrow() held over 1 GB for one day of the stock fact). Files
    with delete files go through PyIceberg, which applies the deletes."""
    t = catalog().load_table(table)

    def ice(group):
        exprs = [OPS[op][0](c, v.isoformat() if isinstance(v, date) else v) for c, op, v in group]
        return exprs[0] if len(exprs) == 1 else And(*exprs)

    def arrow(group, present):  # a condition on a column the file doesn't store was settled by partition pruning
        exprs = [OPS[op][1](pc.field(c), pa.scalar(v)) for c, op, v in group if c in present]
        return functools.reduce(operator.and_, exprs, pc.scalar(True))

    row_filter = AlwaysTrue() if not any_of else ice(any_of[0]) if len(any_of) == 1 else Or(*map(ice, any_of))
    tasks = list(t.scan(row_filter=row_filter).plan_files())
    if any(task.delete_files for task in tasks):
        return t.scan(row_filter=row_filter, selected_fields=columns).to_arrow()
    needed = list(dict.fromkeys([*columns, *(c for g in any_of for c, _, _ in g)]))
    schema = t.schema().select(*columns).as_arrow()  # one schema for every file, whichever writer wrote it
    parts = []
    for task in tasks:
        with t.io.new_input(task.file.file_path).open() as f:
            pf = pq.ParquetFile(f)
            present = [c for c in needed if c in pf.schema_arrow.names]
            if not set(columns) <= set(present):  # a partition column not stored in the file: PyIceberg fills it in
                return t.scan(row_filter=row_filter, selected_fields=columns).to_arrow()
            keep = functools.reduce(operator.or_, (arrow(g, present) for g in any_of)) if any_of else None
            for batch in pf.iter_batches(batch_size=65_536, columns=present):
                part = pa.Table.from_batches([batch])
                parts.append((part if keep is None else part.filter(keep)).select(schema.names).cast(schema))
    return pa.concat_tables(parts) if parts else schema.empty_table()


def statements(path: Path) -> list[str]:
    text = "\n".join(line for line in path.read_text().splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def money(minor: int | None) -> str:
    return "" if minor is None else f"{minor / 100:,.2f}"


# ---------- 1. PSP settlement files -> raw.psp_settlements, with audit columns and a per-file control ----------

PSP_SCHEMA = pa.schema(
    [
        ("country", pa.string()),
        ("settlement_date", pa.date32()),
        ("psp_reference", pa.string()),
        ("merchant_reference", pa.string()),
        ("record_type", pa.string()),
        ("payment_method", pa.string()),
        ("transaction_ts", pa.timestamp("us", tz="UTC")),
        ("presentment_currency", pa.string()),
        ("presentment_minor", pa.int64()),
        ("settlement_currency", pa.string()),
        ("gross_minor", pa.int64()),
        ("fee_minor", pa.int64()),
        ("net_minor", pa.int64()),
        ("_batch_id", pa.string()),  # the file: one PSP batch
        ("_source_file", pa.string()),
        ("_source_offset", pa.int64()),  # line number in the file (the header is line 1)
        ("_file_md5", pa.string()),
        ("_ingested_at", pa.timestamp("us", tz="UTC")),  # ingestion time, kept apart from transaction_ts
    ]
)
TO_MINOR = "CAST(CAST({c} AS DECIMAL(18, 2)) * 100 AS BIGINT)"


def psp_files(until: date | None) -> list[Path]:
    """Settlement files in the raw zone, oldest first; with `until`, only those dated (settled) on or before it."""
    files = sorted(PSP_FILES.glob("country=*/kestrel_*.csv"), key=lambda f: (f.stem[-8:], f.stem))
    return [f for f in files if until is None or datetime.strptime(f.stem[-8:], "%Y%m%d").date() <= until]


def read_psp_file(con, path: Path, ingested_at: datetime) -> tuple[pa.Table, dict]:
    """One settlement file, typed, with its audit columns; and the file's own control totals, taken from the text."""
    raw = path.read_bytes()
    text = pacsv.read_csv(
        pa.BufferReader(raw),
        convert_options=pacsv.ConvertOptions(
            strings_can_be_null=False, column_types={n: pa.string() for n in raw.split(b"\n", 1)[0].decode().split(",")}
        ),
    )
    md5 = hashlib.md5(raw).hexdigest()
    con.register("f", text)
    typed = (
        con.sql(f"""
        SELECT '{path.parent.name.split("=")[1]}' AS country, settlement_date::DATE AS settlement_date,
               psp_reference, merchant_reference, record_type, payment_method,
               transaction_ts::TIMESTAMPTZ AS transaction_ts,
               presentment_currency, {TO_MINOR.format(c="presentment_amount")} AS presentment_minor,
               settlement_currency, {TO_MINOR.format(c="gross_amount")} AS gross_minor,
               {TO_MINOR.format(c="fee_amount")} AS fee_minor, {TO_MINOR.format(c="net_amount")} AS net_minor,
               '{path.stem}' AS _batch_id, '{path.relative_to(RAW)}' AS _source_file,
               (row_number() OVER () + 1)::BIGINT AS _source_offset, '{md5}' AS _file_md5,
               TIMESTAMPTZ '{ingested_at.isoformat()}' AS _ingested_at
        FROM f""")
        .to_arrow_table()
        .cast(PSP_SCHEMA)
    )
    lines = raw.count(b"\n") - 1  # data lines, counted on the bytes, not by the parser
    gross = con.sql(f"SELECT coalesce(sum({TO_MINOR.format(c='gross_amount')}), 0) FROM f").fetchall()[0][0]
    return typed, {"rows": lines, "gross_minor": gross, "md5": md5}


def land_psp(until: date | None = None) -> dict:
    """Land every settlement file not yet in the table, or whose bytes changed (a corrected file replaces its batch).
    Idempotent: a second run finds nothing new. Then check source against target, file by file."""
    cat = catalog()
    if not cat.table_exists(PSP_TABLE):
        t = cat.create_table(PSP_TABLE, schema=PSP_SCHEMA)
        with t.update_spec() as spec:
            spec.add_identity("settlement_date")
    t = cat.load_table(PSP_TABLE)
    con = connect()
    con.register("seen", t.scan(selected_fields=("_batch_id", "_file_md5")).to_arrow())
    landed = dict(con.sql("SELECT DISTINCT _batch_id, _file_md5 FROM seen").fetchall())
    now = datetime.now(UTC).replace(microsecond=0)
    new, controls = [], {}
    for path in psp_files(until):
        md5 = hashlib.md5(path.read_bytes()).hexdigest()
        if landed.get(path.stem) == md5:
            continue
        data, ctl = read_psp_file(con, path, now)
        new.append(data)
        controls[path.stem] = ctl
    started = time.monotonic()
    if new:
        t.overwrite(pa.concat_tables(new), overwrite_filter=In("_batch_id", list(controls)))
    out = {
        "files_seen": len(psp_files(until)),
        "files_landed": len(controls),
        "rows_landed": sum(c["rows"] for c in controls.values()),
        "seconds": round(time.monotonic() - started, 1),
    }
    # Source to target, per file: the file's line count, gross total and checksum against what the table now holds.
    if controls:
        t = catalog().load_table(PSP_TABLE)
        con.register(
            "landed",
            t.scan(
                row_filter=In("_batch_id", list(controls)), selected_fields=("_batch_id", "gross_minor", "_file_md5")
            ).to_arrow(),
        )
        got = {
            b: (n, g, m)
            for b, n, g, m in con.sql(
                "SELECT _batch_id, count(*), sum(gross_minor), any_value(_file_md5) FROM landed GROUP BY 1"
            ).fetchall()
        }
        bad = [b for b, c in controls.items() if got.get(b) != (c["rows"], c["gross_minor"], c["md5"])]
        out["files_mismatched"] = bad
    print(json.dumps(out))
    if out.get("files_mismatched"):
        raise SystemExit(f"source-to-target mismatch: {out['files_mismatched']}")
    return out


# ---------- 2. POS events -> one disposition each (accepted, resend, collision), keyed by basket_id ----------


def upload_days() -> list[date]:
    return sorted(date.fromisoformat(p.name.split("=")[1]) for p in (RAW / "pos/events").glob("upload_date=*"))


def decided_days(t) -> set[date]:
    return {r["partition"]["upload_date"] for r in t.inspect.partitions().to_pylist()}


def accept_one(cat, day: date) -> dict:
    """Decide upload day `day` against the accepted events of the dedup window's earlier days, then replace the day's
    dispositions in one commit. A re-run decides the same way (same inputs) and replaces the day with itself."""
    con = connect()
    batch = cat.load_table("raw.pos_uploads").scan(row_filter=EqualTo("upload_date", day.isoformat())).to_arrow()
    con.register("batch", batch)
    window = And(
        GreaterThanOrEqual("upload_date", (day - timedelta(days=DEDUP_WINDOW_DAYS)).isoformat()),
        LessThan("upload_date", day.isoformat()),
        EqualTo("disposition", "accepted"),
    )
    if cat.table_exists(DISPOSITIONS):
        before = (
            cat.load_table(DISPOSITIONS)
            .scan(row_filter=window, selected_fields=("basket_id", "event_id", "payload_hash"))
            .to_arrow()
        )
        con.register("accepted_before", before)
    else:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE accepted_before (basket_id VARCHAR, event_id VARCHAR, payload_hash VARCHAR)"
        )
    decided = con.sql((SQL / "accept.sql").read_text().strip().rstrip(";"))  # noqa: F841 (DuckDB reads it by name)
    out = con.sql(
        f"""SELECT *, 'pos-upload-{day}' AS _batch_id, now()::TIMESTAMPTZ AS _decided_at FROM decided"""
    ).to_arrow_table()
    if not cat.table_exists(DISPOSITIONS):
        t = cat.create_table(DISPOSITIONS, schema=out.schema)
        with t.update_spec() as spec:
            spec.add_identity("upload_date")
    t = cat.load_table(DISPOSITIONS)
    t.overwrite(out.cast(t.schema().as_arrow()), overwrite_filter=EqualTo("upload_date", day.isoformat()))
    counts = dict(con.sql("SELECT disposition, count(*) FROM out GROUP BY 1").fetchall())
    return {"upload_date": str(day), "received": batch.num_rows, **counts}


def fingerprints(table: str) -> dict[date, tuple[int, int]]:
    """Per upload day: how many events, and a hash total of their event IDs."""
    con = connect()
    con.register("t", catalog().load_table(table).scan(selected_fields=("upload_date", "event_id")).to_arrow())
    rows = con.sql("SELECT upload_date, count(*), sum(hash(event_id) % 1000000007) FROM t GROUP BY 1").fetchall()
    return {d: (n, int(h)) for d, n, h in rows}


def stale_days() -> list[date]:
    """Upload days whose raw events don't match their dispositions (count or hash total): never decided, or the raw
    partition changed since (a re-landed or restored upload). Days with no raw events are reported, never wiped."""
    raw = fingerprints("raw.pos_uploads")
    decided = fingerprints(DISPOSITIONS) if catalog().table_exists(DISPOSITIONS) else {}
    gone = sorted(set(decided) - set(raw))
    if gone:
        print(f"dispositions for {len(gone)} upload day(s) with no raw events left untouched: {gone[0]}..{gone[-1]}")
    return sorted(d for d in raw if decided.get(d) != raw[d])


def following(days: list[date], landed: list[date]) -> list[date]:
    """A day's verdicts depend on the accepted events of the days before it, so re-deciding a day re-decides the
    dedup window's days after it too."""
    out = set(days)
    for d in days:
        out |= {x for x in landed if d < x <= d + timedelta(days=DEDUP_WINDOW_DAYS)}
    return sorted(out)


def accept(day: date | None) -> list[dict]:
    """`--day D`: decide D, after any earlier upload day whose dispositions are missing or stale, and re-decide the
    window's days after them (oldest first). `--all`: every upload day, in order."""
    cat = catalog()
    con = connect()
    if day:
        landed = sorted(fingerprints("raw.pos_uploads"))
        todo = following([d for d in stale_days() if d < day] + [day], landed)
    else:
        todo = upload_days()
    started, results = time.monotonic(), []
    for d in todo:
        results.append(accept_one(cat, d))
        if day:
            print(json.dumps(results[-1]))
    if not day:
        con.register("all_days", pa.Table.from_pylist(results))
        print(json.dumps({"upload_days": len(results), "seconds": round(time.monotonic() - started, 1)}))
        summary(con)
    return results


def ensure_dispositions() -> list[dict]:
    """Decide every upload day whose dispositions are missing or stale, and the window's days after each, oldest
    first (dbt's POS staging reads only accepted events, so landing raw.pos_uploads calls this)."""
    cat = catalog()
    todo = following(stale_days(), sorted(fingerprints("raw.pos_uploads")))
    out = [accept_one(cat, d) for d in todo]
    print(f"dispositions: {len(out)} upload day(s) decided")
    return out


def summary(con) -> None:
    """Every POS event the tills sent, by verdict; UK re-sends in Q3, against Part 6 and the manifest's step 1."""
    con.register(
        "d",
        catalog()
        .load_table(DISPOSITIONS)
        .scan(selected_fields=("basket_id", "till_id", "country", "event_ts", "amount_minor", "disposition"))
        .to_arrow(),
    )
    con.sql("""
        SELECT disposition, count(*) AS events, count(*) FILTER (WHERE country = 'GB') AS uk_events,
               count(DISTINCT till_id) AS tills
        FROM d GROUP BY 1 ORDER BY events DESC""").show()
    gb = con.sql("""SELECT count(*), sum(amount_minor) FROM d WHERE disposition = 'resend' AND country = 'GB'
                    AND event_ts::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'""").fetchall()[0]
    print(f"UK re-sends in Q3 (UTC dates, as the board deck cut them): {gb[0]:,} events, GBP {money(gb[1])}")
    con.register("ledger", scan("raw.payments", ("reference", "source", "status"), [("country", "=", "GB")]))
    tally = con.sql("""
        WITH a AS (SELECT DISTINCT basket_id FROM d WHERE disposition = 'accepted' AND country = 'GB'),
             l AS (SELECT reference FROM ledger WHERE source = 'pos' AND status = 'captured')
        SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM l),
               (SELECT count(*) FROM a WHERE basket_id NOT IN (SELECT reference FROM l)),
               (SELECT count(*) FROM l WHERE reference NOT IN (SELECT basket_id FROM a))""").fetchall()[0]
    print(
        f"UK accepted baskets {tally[0]:,}; UK ledger captured till payments {tally[1]:,}; "
        f"accepted without a payment {tally[2]}, payment without an accepted basket {tally[3]}"
    )


# ---------- 3. the waterfall: board deck -> correct, five steps, from records ----------


def manifest() -> dict:
    return json.loads((RAW / "_manifest.json").read_text())


def waterfall() -> dict:
    started = time.monotonic()
    con = connect()
    cat = catalog()
    con.register(
        "pos",
        cat.load_table(DISPOSITIONS)
        .scan(
            row_filter=EqualTo("country", "GB"),
            selected_fields=("basket_id", "country", "event_ts", "amount_minor", "disposition"),
        )
        .to_arrow(),
    )
    con.register("orders", cat.load_table("raw.orders").scan(row_filter=EqualTo("country", "GB")).to_arrow())
    con.register("refunds", cat.load_table("raw.refunds").scan(row_filter=EqualTo("country", "GB")).to_arrow())
    con.register("fx", cat.load_table("raw.fx_rates").scan().to_arrow())
    con.execute(f"""CREATE TEMP VIEW polled AS          -- the legacy poller's last version of each order it saw
        SELECT * FROM read_parquet('{RAW}/oltp/orders_poll/*/*.parquet', hive_partitioning = true)
        QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1""")
    rows = con.sql((SQL / "waterfall.sql").read_text().strip().rstrip(";")).fetchall()
    m = manifest()["uk_net_revenue_q3"]
    want = [s["amount_minor"] for s in m["steps"]]
    got = [r[2] for r in rows[1:]]
    print(f"{'UK net revenue, Q3 2026':<66} {'GBP':>14}  manifest")
    print(
        f"{'board deck (no fix)':<66} {money(rows[0][1]):>14}  "
        f"{'exact' if rows[0][1] == m['legacy_board_deck_minor'] else 'OFF'}"
    )
    for step, g, w in zip(m["steps"], got, want, strict=True):
        print(f"{'- ' + step['label']:<66} {money(g):>14}  {'exact' if g == w else f'OFF by {money(g - w)}'}")
    print(
        f"{'= correct (all five fixes)':<66} {money(rows[-1][1]):>14}  "
        f"{'exact' if rows[-1][1] == m['correct_minor'] else 'OFF'}"
    )
    gap = rows[0][1] - rows[-1][1]
    print(
        f"{'gap = sum of steps':<66} {money(gap):>14}  {'exact' if gap == sum(want) else 'OFF'};"
        f" unexplained {money(gap - sum(got))}; {time.monotonic() - started:.1f} s"
    )
    return {"legacy": rows[0][1], "correct": rows[-1][1], "steps": got, "unexplained": gap - sum(got)}


# ---------- 4. the bridge: what the PSP settled -> gold net revenue, record by record ----------


def bridge_inputs(con) -> None:
    cat = catalog()
    con.register("psp", cat.load_table(PSP_TABLE).scan(row_filter=EqualTo("country", "GB")).to_arrow())
    con.register(
        "ledger",
        cat.load_table("raw.payments")
        .scan(
            row_filter=EqualTo("country", "GB"),
            selected_fields=("payment_id", "source", "reference", "country", "status", "captured_at"),
        )
        .to_arrow(),
    )
    con.register("sales", cat.load_table("gold.fct_basket").scan().to_arrow())
    con.register("refunds", cat.load_table("gold.fct_refund").scan().to_arrow())
    con.register("stores", cat.load_table("gold.dim_store").scan().to_arrow())
    con.register("fx", cat.load_table("gold.dim_fx_rate").scan().to_arrow())


def bridge() -> dict:
    started = time.monotonic()
    con = connect()
    bridge_inputs(con)
    stmts = statements(SQL / "bridge.sql")
    for s in stmts[:-1]:
        con.execute(s)
    lines = con.sql(stmts[-1]).fetchall()
    report = (
        catalog()
        .load_table(REPORT)
        .scan(
            row_filter=And(
                EqualTo("country", "GB"),
                GreaterThanOrEqual("business_date", Q3[0].isoformat()),
                LessThanOrEqual("business_date", Q3[1].isoformat()),
            )
        )
        .to_arrow()
    )
    gold = sum(report.column("net_minor").to_pylist())
    total, bugs = 0, 0
    print(f"{'UK, Q3 2026':<74} {'records':>8} {'GBP':>14}")
    for line, minor, n in lines:
        total += minor
        bugs += minor if line.startswith("9") else 0
        print(f"{line[3:]:<74} {n:>8,} {money(minor):>14}")
    print(f"{'= net revenue the settlement files support':<74} {'':>8} {money(total):>14}")
    print(
        f"{'gold.rpt_daily_net_revenue_v2, GB, Q3':<74} {'':>8} {money(gold):>14}  "
        f"{'equal' if gold == total else 'DIFFERENT'}"
    )
    print(
        f"unexplained (BUG lines): {money(bugs) or '0.00'} in "
        f"{sum(n for line, _, n in lines if line.startswith('9'))} records; "
        f"{time.monotonic() - started:.1f} s"
    )
    unsettled = con.sql("""SELECT count(*), count(*) FILTER (WHERE ledger_status = 'captured'
                                                       AND captured_date > DATE '2026-09-30')
                           FROM pairs WHERE in_revenue AND settlement_date IS NULL""").fetchall()[0]
    print(
        f"Q3 revenue records with no PSP row by 5 October: {unsettled[0]}, "
        f"of which the ledger shows captured after 30 September: {unsettled[1]}"
    )
    return {"lines": {line: (minor, n) for line, minor, n in lines}, "supported": total, "gold": gold, "bugs": bugs}


def board_deck_records() -> dict:
    """The board deck's extra records, looked up in the PSP's files and the ledger: is there money behind them?"""
    con = connect()
    bridge_inputs(con)
    cat = catalog()
    con.register(
        "disp",
        cat.load_table(DISPOSITIONS)
        .scan(
            row_filter=And(EqualTo("country", "GB"), EqualTo("disposition", "resend")),
            selected_fields=("basket_id", "event_ts"),
        )
        .to_arrow(),
    )
    con.register(
        "orders",
        cat.load_table("raw.orders")
        .scan(row_filter=EqualTo("country", "GB"), selected_fields=("order_id",))
        .to_arrow(),
    )
    rows = con.sql(f"""
        WITH resent AS (SELECT DISTINCT basket_id FROM disp
                        WHERE event_ts::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'),
        deleted AS (SELECT DISTINCT order_id FROM read_parquet('{RAW}/oltp/orders_poll/*/*.parquet')
                    WHERE country = 'GB' AND created_at::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
                      AND order_id NOT IN (SELECT order_id FROM orders)),
        sale_rows AS (SELECT merchant_reference, count(*) AS n FROM psp WHERE record_type = 'SALE' GROUP BY 1)
        SELECT 'step 1: re-sent baskets' AS records, count(*) AS n,
               count(*) FILTER (WHERE s.n = 1) AS one_psp_sale, count(*) FILTER (WHERE s.n IS NULL) AS no_psp_sale,
               NULL AS ledger_voided
        FROM resent r LEFT JOIN sale_rows s ON s.merchant_reference = r.basket_id
        UNION ALL
        SELECT 'step 2: polled, then deleted orders', count(*), count(*) FILTER (WHERE s.n = 1),
               count(*) FILTER (WHERE s.n IS NULL), count(*) FILTER (WHERE l.status = 'voided')
        FROM deleted o LEFT JOIN sale_rows s ON s.merchant_reference = o.order_id
        LEFT JOIN ledger l ON l.reference = o.order_id""").fetchall()
    for r in rows:
        print(
            f"{r[0]:<38} {r[1]:>6,} records: one PSP sale {r[2]:,}, no PSP sale {r[3]:,}"
            + (f", ledger payment voided {r[4]:,}" if r[4] is not None else "")
        )
    return {r[0]: r[1:] for r in rows}


# ---------- 5. the daily control table, and the gate before publish ----------


def ts(d: date) -> str:
    return f"{d.isoformat()}T00:00:00+00:00"


def control(day: date) -> list[dict]:
    """Write upload day `day`'s control rows to gold.daily_controls (replacing a previous run's) and print a summary;
    return the failures. Closed UTC days D-7..D-1 are re-checked every run, so a record lost late still fails."""
    started = time.monotonic()
    cat = catalog()
    con = connect()
    first, last, until = (
        day - timedelta(days=CONTROL_WINDOW_DAYS),
        day - timedelta(days=CLOSED_AFTER_DAYS),
        day + timedelta(days=1),
    )
    for k, v in {"run_day": day, "first_day": first, "last_day": last, "files_until": until}.items():
        con.execute(f"SET VARIABLE {k} = DATE '{v}'")
    since, before = datetime.combine(first, datetime.min.time(), UTC), datetime.combine(day, datetime.min.time(), UTC)
    in_window = lambda col: [(col, ">=", since), (col, "<", before)]  # noqa: E731
    dates = (("upload_date", ">=", day - timedelta(days=DEDUP_WINDOW_DAYS)), ("upload_date", "<=", day))
    inputs = {
        "ledger": (
            "raw.payments",
            ("reference", "country", "amount_minor", "status", "authorised_at", "captured_at"),
            in_window("authorised_at"),
            in_window("captured_at"),
        ),
        "gold_sales": (
            "gold.fct_basket",
            ("transaction_id", "store_key", "profile_key", "event_ts", "amount_minor"),
            in_window("event_ts"),
        ),
        "gold_refunds": (
            "gold.fct_refund",
            ("transaction_id", "store_key", "refund_ts", "refund_minor"),
            in_window("refund_ts"),
        ),
        "cdc_refunds": (
            "raw.refunds",
            ("original_reference", "country", "tender_type", "refund_ts", "amount_minor"),
            in_window("refund_ts"),
        ),
        "psp": (
            PSP_TABLE,
            ("country", "settlement_date", "record_type", "merchant_reference", "transaction_ts", "presentment_minor"),
            [("settlement_date", ">=", first), ("settlement_date", "<=", until)],
        ),
        "stores": ("gold.dim_store", ("store_key", "store_id", "country")),
        "disp_closed": (
            DISPOSITIONS,
            ("basket_id", "country", "event_ts", "amount_minor"),
            [
                ("upload_date", ">=", first),
                ("upload_date", "<=", day),
                ("disposition", "=", "accepted"),
                ("event_ts", ">=", since),
                ("event_ts", "<", before),
            ],
        ),
        "profiles": ("gold.dim_sale_profile", ("profile_key", "tender_type")),
        "raw_window": (
            "raw.pos_uploads",
            ("upload_date", "event_id", "store_id", "tenders"),
            [("upload_date", ">=", first), ("upload_date", "<=", day)],
        ),
        "disp_all": (
            DISPOSITIONS,
            ("upload_date", "event_id", "country", "amount_minor"),
            [("upload_date", ">=", first), ("upload_date", "<=", day)],
        ),
        "disp_day": (DISPOSITIONS, ("event_id", "country", "amount_minor", "disposition"), [("upload_date", "=", day)]),
        "disp_window": (DISPOSITIONS, ("basket_id",), [*dates, ("disposition", "=", "accepted")]),
        "stock_raw": ("raw.stock_snapshots", ("store_id", "on_hand_qty"), [("business_date", "=", last)]),
        "stock_gold": (
            "gold.fct_stock_daily",
            ("store_key", "on_hand_qty"),
            [("date_key", "=", int(last.strftime("%Y%m%d")))],
        ),
        "clicks_raw": ("raw.clickstream_events", ("event_id",), [("event_date", "=", last)]),
    }
    for name, (table, columns, *groups) in inputs.items():
        con.register(name, scan(table, columns, *groups))
    click_files = sorted((RAW / f"clickstream/events/event_date={last}").glob("*.jsonl.gz"))
    con.execute("CREATE TEMP TABLE click_file (event_id VARCHAR)")
    if click_files:
        con.execute(
            f"INSERT INTO click_file SELECT event_id FROM read_json({[str(f) for f in click_files]}, "
            "columns = {'event_id': 'VARCHAR'})"
        )
    stmts = statements(SQL / "controls.sql")
    for stmt in stmts[:-1]:
        con.execute(stmt)
    rows = con.sql(stmts[-1])  # noqa: F841 (DuckDB reads it by name)
    out = con.sql(f"""SELECT DATE '{day}' AS run_day, * EXCLUDE (source_minor, source_hash, target_minor, target_hash),
                             source_minor::BIGINT AS source_minor, source_hash::BIGINT AS source_hash,
                             target_minor::BIGINT AS target_minor, target_hash::BIGINT AS target_hash,
                             now()::TIMESTAMPTZ AS checked_at FROM rows""").to_arrow_table()
    if not cat.table_exists(CONTROLS):
        cat.create_table(CONTROLS, schema=out.schema)
    t = cat.load_table(CONTROLS)
    t.overwrite(out.cast(t.schema().as_arrow()), overwrite_filter=EqualTo("run_day", day.isoformat()))
    con.register("out", out)
    print(f"controls for upload day {day} (closed UTC days {first}..{last}, PSP files to {until}):")
    con.sql("""SELECT check_name, count(*) AS rows_, count(*) FILTER (WHERE status = 'pass') AS pass,
                      count(*) FILTER (WHERE status = 'FAIL') AS fail, sum(pending) AS pending,
                      sum(target_count) FILTER (WHERE rule = 'info') AS info
               FROM out GROUP BY 1 ORDER BY 1""").show()
    failed = con.sql("""SELECT check_name, scope_date, country, source_count, target_count,
                               (target_minor - source_minor) AS minor_diff
                        FROM out WHERE status = 'FAIL' ORDER BY ALL""").fetchall()
    for f in failed:
        print(f"FAIL {f[0]} {f[1]} {f[2] or ''}: source {f[3]}, target {f[4]}, money difference {money(f[5])}")
    print(f"{len(failed)} failed; {time.monotonic() - started:.1f} s")
    return [
        dict(zip(["check", "scope_date", "country", "source", "target", "minor_diff"], f, strict=True)) for f in failed
    ]


# ---------- 6. Hands-On: change one upload day in the raw zone, and put it back ----------

HELD = LAB / "held"


def day_file(day: date) -> Path:
    return RAW / f"pos/events/upload_date={day}/pos-events-{day}.jsonl.gz"


def inject(kind: str, day: date) -> dict:
    """resend: a UK till re-sends, after midnight, the last basket it uploaded the day before (a new file for upload
    day D). hold: the UK till with the most late events for UTC day D-1 in D's upload loses its upload (moved to
    data/lab/part21/held). restore: both undone; the day's file is put back byte for byte."""
    resend = day_file(day).with_name(f"pos-events-{day}-resend.jsonl.gz")
    backup = HELD / day_file(day).name
    if kind == "restore":
        resend.unlink(missing_ok=True)
        if backup.exists():
            shutil.copyfile(backup, day_file(day))
            backup.unlink()
        out = {"restored": str(day)}
    elif kind == "resend":
        with gzip.open(day_file(day - timedelta(days=1)), "rt") as f:
            last = max(
                (json.loads(line) for line in f if line.startswith('{"event_id"') and '"GB-' in line),
                key=lambda e: e["uploaded_at"],
            )
        copy = {
            **last,
            "event_id": hashlib.md5(f"{last['event_id']}:resend".encode()).hexdigest()[:16],
            "uploaded_at": f"{day}T00:07:00.000Z",
        }
        with gzip.open(resend, "wt") as f:
            f.write(json.dumps(copy, separators=(",", ":")) + "\n")
        out = {
            "resent_basket": last["basket_id"],
            "first_event_id": last["event_id"],
            "new_event_id": copy["event_id"],
            "first_uploaded_at": last["uploaded_at"],
            "resent_at": copy["uploaded_at"],
            "amount_minor": last["tenders"][0]["amount_minor"],
        }
    elif kind == "hold":
        HELD.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(day_file(day), backup)
        with gzip.open(day_file(day), "rt") as f:
            lines = f.readlines()
        late = [json.loads(x) for x in lines if '"GB-' in x]
        late = [e for e in late if e["event_ts"][:10] == str(day - timedelta(days=1))]
        tills: dict[str, int] = {}
        for e in late:
            tills[e["till_id"]] = tills.get(e["till_id"], 0) + 1
        till = max(sorted(tills), key=lambda t: tills[t])
        held = [x for x in lines if f'"till_id":"{till}"' in x]
        with gzip.open(day_file(day), "wt") as f:
            f.writelines(x for x in lines if f'"till_id":"{till}"' not in x)
        with gzip.open(HELD / f"{till}-{day}.jsonl.gz", "wt") as f:
            f.writelines(held)
        amounts = [json.loads(x)["tenders"][0]["amount_minor"] for x in held]
        out = {"till": till, "events_held": len(held), "amount_minor": sum(amounts)}
    else:
        raise SystemExit(f"unknown kind {kind}")
    print(json.dumps(out))
    return out


# ---------- 7. restatement: the reconciled quarter gets a tag; as published vs as of now ----------

QUARTER_TAG = "q3_2026_reconciled"
RESTATEMENTS = "gold.restatements"


def q3_by_ref(ref: str | None) -> dict[str, int]:
    t = catalog().load_table(REPORT)
    snap = t.metadata.refs[ref].snapshot_id if ref else t.current_snapshot().snapshot_id
    rows = (
        t.scan(
            row_filter=And(
                GreaterThanOrEqual("business_date", Q3[0].isoformat()),
                LessThanOrEqual("business_date", Q3[1].isoformat()),
            ),
            selected_fields=("country", "net_minor"),
            snapshot_id=snap,
        )
        .to_arrow()
        .to_pylist()
    )
    out: dict[str, int] = {}
    for r in rows:
        out[r["country"]] = out.get(r["country"], 0) + r["net_minor"]
    return out


def restate() -> dict:
    """Tag the report snapshot the bridge reconciled (once; a tag that exists is never moved), then record the
    restatement: what the board saw against what the settlement files support, with the reasons."""
    m = manifest()["uk_net_revenue_q3"]
    t = catalog().load_table(REPORT)
    if QUARTER_TAG not in t.metadata.refs:
        if q3_by_ref(None)["GB"] != m["correct_minor"]:
            raise SystemExit("main doesn't hold the reconciled quarter; run the bridge first")
        with t.manage_snapshots() as ms:
            ms.create_tag(t.current_snapshot().snapshot_id, QUARTER_TAG)
        t = catalog().load_table(REPORT)
    tagged = t.metadata.refs[QUARTER_TAG].snapshot_id
    record = pa.Table.from_pylist(
        [
            {
                "period": "2026-Q3",
                "country": "GB",
                "measure": "net revenue (GBP minor units)",
                "as_published_minor": m["legacy_board_deck_minor"],
                "as_published_in": "board deck, 2026-10-05",
                "restated_minor": q3_by_ref(QUARTER_TAG)["GB"],
                "restated_from": f"{REPORT} tag {QUARTER_TAG} ({tagged})",
                "reasons": json.dumps({s["key"]: s["amount_minor"] for s in m["steps"]}),
                "status": "proposed",  # approved, or not, when Farah signs the quarter off
                "recorded_at": datetime.now(UTC).replace(microsecond=0),
            }
        ]
    )
    cat = catalog()
    if not cat.table_exists(RESTATEMENTS):
        cat.create_table(RESTATEMENTS, schema=record.schema)
    r = cat.load_table(RESTATEMENTS)
    r.overwrite(
        record.cast(r.schema().as_arrow()), overwrite_filter=And(EqualTo("period", "2026-Q3"), EqualTo("country", "GB"))
    )
    views = {"board deck (as published)": m["legacy_board_deck_minor"]}
    for label, ref in (
        ("tag published (Part 19, moves daily)", "published"),
        (f"tag {QUARTER_TAG} (never moves)", QUARTER_TAG),
        ("main (as of now)", None),
    ):
        views[label] = q3_by_ref(ref)["GB"]
    for label, minor in views.items():
        print(f"{label:<44} {money(minor):>14}")
    print(
        f"restatement proposed in {RESTATEMENTS}: "
        f"{money(record['restated_minor'][0].as_py() - m['legacy_board_deck_minor'])}"
        f" (snapshot {tagged})"
    )
    return views


def logs(day: date, task_id: str, dag_id: str = "larkspur_daily_revenue") -> list[str]:
    """What a task printed in its latest try of the newest run for `day`, read through Airflow's REST API (host)."""
    from scripts import part19_pipeline as p19

    runs = [
        r
        for r in p19.api(f"/dags/{dag_id}/dagRuns?limit=100")["dag_runs"]
        if (r["logical_date"] or "")[:10] == str(day)
    ]
    run = max(runs, key=lambda r: r["queued_at"] or "")
    ti = p19.api(f"/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances/{task_id}")
    content = p19.api(
        f"/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances/{task_id}/logs/{ti['try_number']}?full_content=true"
    )["content"]
    lines = [
        c["event"]
        for c in content
        if isinstance(c, dict)
        and str(c.get("logger", "")).endswith("SubprocessHook")
        and not c["event"].startswith(("Running command", "Output:", "Command exited", "Tmp dir"))
    ]
    print(f"{dag_id} {run['dag_run_id']} {task_id}: {ti['state']} (try {ti['try_number']})")
    for line in lines:
        print(line)
    return lines


def reset() -> None:
    """Drop Part 21's tables and the quarter's tag: the next run starts from Parts 1-20's state."""
    tok = cat9.token()
    for name in (PSP_TABLE, DISPOSITIONS, CONTROLS, RESTATEMENTS):
        cat9.drop_table(*name.split("."), tok)
    t = catalog().load_table(REPORT)
    if QUARTER_TAG in t.metadata.refs:
        with t.manage_snapshots() as ms:
            ms.remove_tag(QUARTER_TAG)
    print(f"dropped {PSP_TABLE}, {DISPOSITIONS}, {CONTROLS}, {RESTATEMENTS}; removed tag {QUARTER_TAG}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command")
    p.add_argument("kind", nargs="?")
    p.add_argument("--day", type=date.fromisoformat)
    p.add_argument("--until", type=date.fromisoformat)
    p.add_argument("--all", action="store_true")
    a = p.parse_args()
    if a.command == "land-psp":
        land_psp(a.until)
    elif a.command == "accept":
        accept(None if a.all else a.day)
    elif a.command == "waterfall":
        waterfall()
    elif a.command == "bridge":
        bridge()
        board_deck_records()
    elif a.command == "control":
        sys.exit(1 if control(a.day) else 0)
    elif a.command == "inject":
        inject(a.kind, a.day)
    elif a.command == "explain":  # the batch filter on gold's input view reaches raw and dispositions scans
        from scripts import part18_dbt as p18

        p18.explain("lakehouse_pipeline.silver.int_sales_lines")
    elif a.command == "restate":
        restate()
    elif a.command == "logs":
        logs(a.day, a.kind)
    elif a.command == "reset":
        reset()
    else:
        raise SystemExit(f"unknown command {a.command}")


if __name__ == "__main__":
    main()
