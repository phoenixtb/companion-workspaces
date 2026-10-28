"""Part 6: a SQL workbook over Larkspur's data in DuckDB.

uv run python -m scripts.part06_sql build --local      # needs Part 5's database (data/lab/part05); no containers
uv run python -m scripts.part06_sql query --local sql/part06/pos_duplicates.sql [more.sql ...]

The workbook is one DuckDB file, data/lab/part06/workbook.duckdb. The build makes two tables from the raw zone,
`clicks` (the clickstream with retries and bots flagged) and `sessions` (both session definitions). Part 5's star
is attached read-only as `star`, for the stock and sales queries.
"""

import argparse
import time
from pathlib import Path

import duckdb

from scripts import part04_star as star
from scripts import part05_shapes as shapes

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data/lab/part06/workbook.duckdb"


def sql(name: str) -> str:
    return (ROOT / f"sql/part06/{name}.sql").read_text()


def connect(raw_root: str, db: Path = DB, star_db: Path = shapes.DB) -> duckdb.DuckDBPyConnection:
    if not star_db.exists():
        raise SystemExit(f"{star_db} is missing: run `uv run python -m scripts.part05_shapes build --local` first")
    con = star.connect(db, raw_root=raw_root)
    con.execute(f"ATTACH '{star_db}' AS star (READ_ONLY)")
    con.execute("CREATE OR REPLACE TEMP MACRO major(minor) AS CAST(minor AS DECIMAL(18, 0)) * 0.01")  # as in Part 4
    return con


def query(con: duckdb.DuckDBPyConnection, files: list[Path]) -> None:
    """Run each statement and print its result; errors are printed (some statements fail on purpose)."""
    for f in files:
        print(f"-- {f}")
        for stmt in con.extract_statements(f.read_text()):
            text = "\n".join(line for line in stmt.query.splitlines() if not line.lstrip().startswith("--"))
            started = time.monotonic()
            try:
                rel = con.sql(text.strip().rstrip(";"))
            except duckdb.Error as e:
                print(f"{type(e).__name__}: {e}")
                continue
            if rel is None:
                continue
            if stmt.type == duckdb.StatementType.EXPLAIN:
                print(rel.fetchall()[0][1])
            elif stmt.type in (duckdb.StatementType.SELECT, duckdb.StatementType.MULTI):  # MULTI: a PIVOT
                rel.show(max_width=140)
                print(f"({time.monotonic() - started:.2f}s)")
            elif stmt.type == duckdb.StatementType.MERGE_INTO:  # DML returns a one-row count; print it compactly
                print(f"MERGE INTO: {rel.fetchall()[0][0]:,} rows changed")


def build(con: duckdb.DuckDBPyConnection) -> None:
    for name in ["clicks", "sessions"]:
        started = time.monotonic()
        con.execute(sql(name))
        rows = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        print(f"{name:<10} {rows:>10,} rows  {time.monotonic() - started:5.1f}s")
    con.execute("CHECKPOINT")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["build", "query"])
    p.add_argument("files", nargs="*", type=Path)
    p.add_argument("--local", action="store_true", help="read data/raw instead of the bucket")
    a = p.parse_args()
    raw_root = str(ROOT / "data/raw") if a.local else star.BUCKET_RAW
    if a.command == "build":
        DB.unlink(missing_ok=True)
        build(connect(raw_root))
    else:
        query(connect(raw_root), a.files)
