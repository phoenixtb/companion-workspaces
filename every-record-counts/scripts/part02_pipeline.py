"""Part 2: Larkspur's daily POS sales job, written twice.

v1 (`naive`) loads one day's upload, keeps that day's sales and appends them.
v2 (`run`) finds the business dates an upload touched and rebuilds exactly those dates, silver then
gold, from every landed upload that can hold them, in one transaction.

The generator's output in data/raw is "everything the tills will ever upload". `land` copies upload
partitions into the lab's own raw zone (data/lab/part02/raw) one day at a time, so the pipeline only
sees what has arrived so far, like a real landing zone.

uv run python scripts/part02_pipeline.py land 2026-05-31 2026-10-05   # every upload, at once
uv run python scripts/part02_pipeline.py naive 2026-07-01 2026-09-30  # v1, one run per day
uv run python scripts/part02_pipeline.py run 2026-08-16               # v2 for one upload
uv run python scripts/part02_pipeline.py daily 2026-08-14 2026-08-16  # land, then run, day by day
uv run python scripts/part02_pipeline.py check 2026-08-15             # v1 vs v2 vs a full recompute
uv run python scripts/part02_pipeline.py explain 2026-08-15           # where v1's difference comes from
uv run python scripts/part02_pipeline.py fingerprint                  # a hash of the gold table
uv run python scripts/part02_pipeline.py reset                        # delete the lab's raw zone and tables
"""

import argparse
import shutil
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/raw"
LAB = ROOT / "data/lab/part02"
LATE_DAYS = 2  # policy: a sale may land up to 2 days after its business date (measured worst case: 1)


def sql(name: str) -> str:
    return (ROOT / f"sql/part02/{name}.sql").read_text()


def days(start: date, end: date) -> list[date]:
    step = 1 if end >= start else -1  # a reversed range runs newest first
    return [start + timedelta(days=i * step) for i in range(abs((end - start).days) + 1)]


def land(day: date, source: Path = SOURCE, lab: Path = LAB) -> None:
    """Copy one upload partition (and the store list) into the lab's raw zone, as if the tills had sent it."""
    stores = lab / "raw/reference/stores/stores.csv"
    if not stores.exists():
        stores.parent.mkdir(parents=True)
        shutil.copy(source / "reference/stores/stores.csv", stores)
    partition = f"pos/events/upload_date={day}"
    if (source / partition).exists():
        shutil.copytree(source / partition, lab / "raw" / partition, dirs_exist_ok=True)


def connect(lab: Path = LAB) -> duckdb.DuckDBPyConnection:
    lab.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(lab / "larkspur.duckdb"))
    con.execute(f"SET VARIABLE landed_root = '{lab / 'raw'}'")
    con.execute(f"SET VARIABLE late_days = {LATE_DAYS}")
    con.execute(sql("schema"))
    refresh_views(con)
    return con


def refresh_views(con: duckdb.DuckDBPyConnection) -> None:
    """Point the views at whatever has landed (DuckDB checks the files exist when it creates a view)."""
    root = Path(con.execute("SELECT getvariable('landed_root')").fetchone()[0])
    if any((root / "pos/events").glob("upload_date=*")):
        con.execute(sql("views"))


def landed(con: duckdb.DuckDBPyConnection, day: date) -> bool:
    root = Path(con.execute("SELECT getvariable('landed_root')").fetchone()[0])
    return (root / f"pos/events/upload_date={day}").exists()


def transaction(con: duckdb.DuckDBPyConnection, statements: list[str]) -> None:
    """All or nothing: a crash halfway leaves silver and gold as they were."""
    con.execute("BEGIN TRANSACTION")
    try:
        for statement in statements:
            con.execute(statement)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def naive(con: duckdb.DuckDBPyConnection, day: date) -> int:
    con.execute(f"SET VARIABLE run_date = DATE '{day}'")
    return con.execute(sql("naive_load")).fetchone()[0]  # rows appended


def run(con: duckdb.DuckDBPyConnection, day: date, *, extra: list[str] = ()) -> list[date]:
    """Rebuild every business date that the upload for `day` touched. Returns the dates rebuilt.

    `extra` statements run inside the same transaction; the tests use one to simulate a crash.
    """
    if not landed(con, day):
        return []
    con.execute(f"SET VARIABLE run_date = DATE '{day}'")
    touched = [r[0] for r in con.execute(sql("affected_dates")).fetchall()]
    too_late = [d for d in touched if d + timedelta(days=LATE_DAYS) < day]
    if too_late:  # outside the policy: report it, never drop it silently
        print(f"  ! upload {day} holds sales for {too_late}, beyond the {LATE_DAYS}-day window")
    dates = [d for d in touched if d not in too_late]
    if not dates:
        return []
    con.execute("CREATE OR REPLACE TEMP TABLE affected AS SELECT unnest(?::DATE[]) AS business_date", [dates])
    con.execute(f"SET VARIABLE first_upload = DATE '{min(dates) - timedelta(days=1)}'")
    con.execute(f"SET VARIABLE last_upload = DATE '{max(dates) + timedelta(days=LATE_DAYS)}'")
    transaction(con, [s.query for s in con.extract_statements(sql("rebuild"))] + list(extra))
    return dates


def fingerprint(con: duckdb.DuckDBPyConnection, table: str = "gold.daily_store_sales", key: str = "") -> tuple:
    """Row count and an MD5 over every row in key order: equal fingerprints mean identical tables."""
    key = key or ("basket_id" if table.startswith("silver") else "business_date, store_id")
    return con.execute(f"SELECT count(*), md5(string_agg(t::VARCHAR, ',' ORDER BY {key})) FROM {table} t").fetchone()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("command", choices=["land", "naive", "run", "daily", "check", "explain", "fingerprint", "reset"])
    p.add_argument("start", nargs="?", type=date.fromisoformat)
    p.add_argument("end", nargs="?", type=date.fromisoformat)
    a = p.parse_args()
    span = days(a.start, a.end or a.start) if a.start else []

    if a.command == "reset":
        shutil.rmtree(LAB, ignore_errors=True)
        print(f"removed {LAB.relative_to(ROOT)}")
        return
    if a.command == "land":
        for d in span:
            land(d)
        print(f"landed {len(span)} upload partitions, {span[0]} to {span[-1]}")
        return

    con = connect()
    if a.command == "naive":
        rows = sum(naive(con, d) for d in span)
        print(f"v1 naive: {len(span)} runs, {rows:,} rows appended")
    elif a.command in ("run", "daily"):
        for d in span:
            if a.command == "daily":
                land(d)
                refresh_views(con)
            rebuilt = run(con, d)
            print(f"v2 run for upload {d}: rebuilt {', '.join(str(x) for x in rebuilt) or 'nothing'}")
    elif a.command == "check":
        con.execute(f"SET VARIABLE check_date = DATE '{a.start}'")
        print(f"business date {a.start}, revenue in minor units")
        con.sql(sql("check")).show()
    elif a.command == "explain":
        con.execute(f"SET VARIABLE check_date = DATE '{a.start}'")
        print(f"business date {a.start}: what v1 missed and what it counted twice")
        con.sql(sql("v1_explained")).show()
    elif a.command == "fingerprint":
        rows, digest = fingerprint(con)
        print(f"gold.daily_store_sales: {rows:,} rows, md5 {digest}")


if __name__ == "__main__":
    main()
