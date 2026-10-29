"""Part 6: the same questions in Trino, over an Iceberg copy of the workbook's clickstream.

uv run python -m scripts.part06_trino load                    # needs `make up-trino` and the Part 6 workbook
uv run python -m scripts.part06_trino query sql/part06/trino/approx.sql [more.sql ...]

`load` writes the workbook's `clicks` table to the Iceberg table larkspur.part06.clicks through Polaris (DuckDB),
which Trino reads as lakehouse.part06.clicks. `query` runs each statement in Trino and prints its rows; errors
are printed, because some statements show what Trino doesn't accept.
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb
import pyarrow as pa

from scripts import part04_star as star
from scripts import part06_sql as workbook

ROOT = Path(__file__).resolve().parents[1]
TRINO = "http://localhost:8080/v1/statement"


def load() -> None:
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    con.execute(f"ATTACH '{workbook.DB}' AS wb (READ_ONLY)")
    con.execute("CREATE SCHEMA IF NOT EXISTS lake.part06")
    con.execute("DROP TABLE IF EXISTS lake.part06.clicks")
    # DROP TABLE leaves the files behind (no purge); remove them so re-runs don't accumulate.
    fs = star.fs()
    if fs.exists("larkspur-lake/warehouse/part06/clicks"):
        fs.rm("larkspur-lake/warehouse/part06/clicks", recursive=True)
    started = time.monotonic()
    from scripts import part23_privacy as privacy  # Part 23: people who asked to be erased stay out of this copy too

    ids = con.sql("SELECT DISTINCT anonymous_id FROM wb.clicks").to_arrow_table().column(0)
    con.register("erased", pa.table({"anonymous_id": privacy.matching(ids, "anonymous_id")}))
    con.execute("CREATE TABLE lake.part06.clicks AS FROM wb.clicks WHERE anonymous_id NOT IN (FROM erased)")
    rows = con.execute("SELECT count(*) FROM lake.part06.clicks").fetchone()[0]
    print(f"lake.part06.clicks: {rows:,} rows ({time.monotonic() - started:.1f}s)")


def run(sql: str) -> tuple[list[str], list[list], dict]:
    req = urllib.request.Request(TRINO, data=sql.encode(), headers={"X-Trino-User": "rohan"})
    page = json.load(urllib.request.urlopen(req))
    columns, rows = [], []
    while True:
        if "error" in page:
            raise RuntimeError(page["error"]["message"])
        columns = columns or [c["name"] for c in page.get("columns", [])]
        rows += page.get("data", [])
        if "nextUri" not in page:
            return columns, rows, page["stats"]
        page = json.load(urllib.request.urlopen(page["nextUri"]))


def show(columns: list[str], rows: list[list]) -> None:
    if len(columns) == 1 and len(rows) == 1 and "\n" in str(rows[0][0]):  # EXPLAIN output: one text cell
        print(rows[0][0])
        return
    cells = [[str(c) for c in columns]] + [["NULL" if v is None else str(v) for v in r] for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(columns))]
    for n, r in enumerate(cells):
        print("  ".join(v.rjust(w) for v, w in zip(r, widths, strict=True)))
        if n == 0:
            print("  ".join("-" * w for w in widths))


def query(files: list[Path]) -> None:
    for f in files:
        print(f"-- {f}")
        code = "\n".join(line for line in f.read_text().splitlines() if not line.lstrip().startswith("--"))
        for statement in code.split(";"):  # comment lines are gone first: they may contain ';'
            text = statement.strip()
            if not text:
                continue
            started = time.monotonic()
            try:
                columns, rows, _ = run(text)
            except (RuntimeError, urllib.error.HTTPError) as e:
                print(f"ERROR: {e}\n")
                continue
            show(columns, rows)
            print(f"({time.monotonic() - started:.2f}s)\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["load", "query"])
    p.add_argument("files", nargs="*", type=Path)
    a = p.parse_args()
    load() if a.command == "load" else query(a.files)
