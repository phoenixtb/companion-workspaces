"""Part 5: other shapes for the same data, and the correctness fixes under all of them.

uv run python -m scripts.part05_shapes build --local     # needs Part 4's star (data/lab/part04); no containers
uv run python -m scripts.part05_shapes query --local sql/part05/refunds_two_shapes.sql [more.sql ...]

The build copies Part 4's star to data/lab/part05/larkspur.duckdb and adds: FX rates for every calendar day,
a close-of-day column on the stock snapshot fact, a Data Vault for refunds and the sales they refund (loaded
twice, with what had landed by 06:00 UTC on 1 October and then with everything, plus a re-run of the second
load to show it inserts nothing) and a wide refunds table built from the vault.
"""

import argparse
import shutil
import time
from pathlib import Path

import duckdb

from scripts import part04_star as star

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data/lab/part05/larkspur.duckdb"
VAULT = [
    "hub_sale",
    "hub_refund",
    "hub_store",
    "lnk_refund_sale",
    "lnk_sale_store",
    "sat_sale_pos",
    "sat_sale_order",
    "sat_refund",
    "sat_store",
]
# The daily report for 30 September ran at 06:00 UTC on 1 October: the first load holds what it could see.
LOADS = [("as filed", "2026-10-01 06:00:00+00"), ("everything", "infinity")]


def sql(name: str) -> str:
    return (ROOT / f"sql/part05/{name}.sql").read_text()


def counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in VAULT}


def load_vault(con: duckdb.DuckDBPyConnection, until: str) -> dict[str, int]:
    """One insert-only load of everything landed by `until`; returns the rows each table gained."""
    before = counts(con)
    con.execute(f"SET VARIABLE load_until = TIMESTAMPTZ '{until}'")
    con.execute(sql("vault_load"))
    return {t: n - before[t] for t, n in counts(con).items()}


def build(con: duckdb.DuckDBPyConnection, loads: list[tuple[str, str]] = LOADS) -> None:
    started = time.monotonic()
    con.execute(sql("fx_daily"))
    con.execute(sql("stock_close"))
    print(f"FX by day and closing stock            {time.monotonic() - started:5.1f}s")
    con.execute(sql("vault_tables"))
    rows = [(label, load_vault(con, until)) for label, until in loads]
    rows.append(("re-run", load_vault(con, loads[-1][1])))
    print(f"vault loads                            {time.monotonic() - started:5.1f}s")
    print("\n== rows inserted per load (as filed: landed by 2026-10-01 06:00 UTC; re-run: everything again)")
    print(f"{'table':<16}" + "".join(f"{label:>12}" for label, _ in rows))
    for t in VAULT:
        print(f"{t:<16}" + "".join(f"{inserted[t]:>12,}" for _, inserted in rows))
    con.execute(sql("wide_refund"))
    con.execute("CHECKPOINT")
    print(
        f"\nwide_refund: {con.execute('SELECT count(*) FROM wide_refund').fetchone()[0]:,} rows "
        f"({time.monotonic() - started:.1f}s)"
    )


def prepare(raw_root: str, db: Path = DB, star_db: Path = star.DB) -> duckdb.DuckDBPyConnection:
    """A fresh copy of Part 4's star, opened with the raw views."""
    if not star_db.exists():
        raise SystemExit(f"{star_db} is missing: run `uv run python scripts/part04_star.py build --local` first")
    db.parent.mkdir(parents=True, exist_ok=True)
    db.unlink(missing_ok=True)
    shutil.copyfile(star_db, db)
    return star.connect(db, raw_root=raw_root)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["build", "query"])
    p.add_argument("files", nargs="*", type=Path)
    p.add_argument("--local", action="store_true", help="read data/raw instead of the bucket")
    a = p.parse_args()
    raw_root = str(ROOT / "data/raw") if a.local else star.BUCKET_RAW
    if a.command == "build":
        build(prepare(raw_root))
    else:
        star.query(star.connect(DB, raw_root=raw_root), a.files)
