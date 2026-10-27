"""Part 4: Larkspur's star schema in a DuckDB file, built from the raw zone.

uv run python scripts/part04_star.py build --local              # from data/raw (no containers needed)
uv run python scripts/part04_star.py build                      # from s3://larkspur-raw/raw (make up-core)
uv run python scripts/part04_star.py query --local sql/part04/discount_grain_bug.sql [more.sql ...]

The star lands in data/lab/part04/larkspur_star.duckdb. The build runs sql/part04/ in order: staging,
dimensions, early-arriving facts (placeholders), facts, the catalogue catching up (Type 1 update of the
placeholders) and the delivery milestone (an update of the accumulating snapshot).
"""

import argparse
import tempfile
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data/lab/part04/larkspur_star.duckdb"
BUCKET_RAW = "s3://larkspur-raw/raw"


def fs():
    import s3fs

    return s3fs.S3FileSystem(
        key="larkspur",
        secret="larkspur-lab-secret",
        client_kwargs={"endpoint_url": "http://localhost:8333", "region_name": "us-east-1"},
    )


def sql(name: str) -> str:
    return (ROOT / f"sql/part04/{name}.sql").read_text()


def connect(db: Path = DB, raw_root: str | None = None) -> duckdb.DuckDBPyConnection:
    """Open the star; with raw_root, also create the raw views it's built from."""
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db))
    con.execute("SET TimeZone = 'UTC'")
    if raw_root is not None:
        if raw_root.startswith("s3://"):
            con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
        con.execute(f"SET VARIABLE raw_root = '{raw_root}'")
        con.execute((ROOT / "sql/raw/views.sql").read_text())
    return con


def land_supplier_files(con: duckdb.DuckDBPyConnection) -> None:
    """Supplier delivery files as text: every CSV in one read, each XLSX on its own (read_xlsx takes one file)."""
    con.execute("""
        CREATE OR REPLACE TABLE landed_supplier_deliveries AS
        SELECT regexp_extract(filename, '/(S\\d{3})_', 1)                               AS supplier_id,
               strptime(regexp_extract(filename, '_(\\d{8})\\.', 1), '%Y%m%d')::DATE   AS file_date,
               "PO Number" AS po_number, "PO Line" AS po_line, "SKU" AS sku,
               coalesce("Delivered Qty", "Qty Delivered") AS delivered_qty,           -- S017 renamed it
               "Delivery Date" AS delivery_date, "DC" AS dc_id
        FROM raw_supplier_deliveries_csv
    """)
    root = con.execute("SELECT getvariable('raw_root')").fetchone()[0]
    files = [
        f
        for (f,) in con.execute(f"SELECT file FROM glob('{root}/suppliers/deliveries/*/*.xlsx') ORDER BY 1").fetchall()
    ]
    with tempfile.TemporaryDirectory() as tmp:
        if root.startswith("s3://"):  # read_xlsx over S3 took about a second per file; fetch them first
            local = [str(Path(tmp) / Path(f).name) for f in files]
            fs().get([f.removeprefix("s3://") for f in files], local)
        else:
            local = files
        for f in local:
            con.execute(f"""
                INSERT INTO landed_supplier_deliveries
                SELECT regexp_extract('{f}', '(S\\d{{3}})_', 1),
                       strptime(regexp_extract('{f}', '_(\\d{{8}})\\.', 1), '%Y%m%d')::DATE,
                       "PO Number", "PO Line", "SKU", "Delivered Qty", "Delivery Date", "DC"
                FROM read_xlsx('{f}', all_varchar = true)
            """)


def show(con: duckdb.DuckDBPyConnection, query: str) -> None:
    con.sql(query).show(max_width=140)


def build(con: duckdb.DuckDBPyConnection, db: Path = DB) -> None:
    started = time.monotonic()
    land_supplier_files(con)
    con.execute(sql("staging"))
    con.execute(sql("dimensions"))
    print(f"staging and dimensions                 {time.monotonic() - started:5.1f}s")

    con.execute(sql("early_arriving"))
    print("\n== early-arriving facts: rows naming a product the catalogue doesn't list yet")
    show(
        con,
        """SELECT source, count(*) AS fact_rows, count(DISTINCT sku) AS skus, min(business_date) AS first_day,
                  max(business_date) AS last_day
           FROM stg_unresolved_product GROUP BY source ORDER BY source""",
    )

    con.execute(sql("facts"))
    print(f"facts                                  {time.monotonic() - started:5.1f}s")
    product = "SKU-00273"  # launched 16 July, sold that day; first in the extract of 17 July
    print(f"\n== {product} before the catalogue catches up")
    show(con, product_rows(product))
    con.execute(sql("late_arriving"))
    print(f"== {product} after (placeholder updated in place, Type 1)")
    show(con, product_rows(product))

    print("== purchase-order lines (accumulating snapshot), before and after the delivery milestone")
    before = con.execute("SELECT count(*) FROM fct_po_line WHERE status = 'ordered'").fetchone()[0]
    con.execute(sql("deliveries"))
    show(
        con,
        f"""SELECT status, count(*) AS po_lines, {before} AS ordered_before_update
            FROM fct_po_line GROUP BY status ORDER BY status""",
    )

    con.execute("""
        UPDATE dim_audit
        SET rows_rejected  = (SELECT count(*) FILTER (WHERE NOT is_valid) FROM stg_delivery_parsed),
            rows_duplicate = (SELECT count(*) FILTER (WHERE is_valid) FROM stg_delivery_parsed)
                             - (SELECT count(*) FROM stg_delivery)
        WHERE table_name = 'fct_po_line'
    """)
    for key, table in con.execute("SELECT audit_key, table_name FROM dim_audit").fetchall():
        con.execute(f"UPDATE dim_audit SET rows_loaded = (SELECT count(*) FROM {table}) WHERE audit_key = {key}")
    con.execute("DROP TABLE landed_supplier_deliveries")
    con.execute("CHECKPOINT")
    print(f"== the star ({time.monotonic() - started:.1f}s, {db.stat().st_size / 2**20:.0f} MiB)")
    show(
        con,
        " UNION ALL ".join(
            f"SELECT '{t}' AS table_name, count(*) AS rows FROM {t}"
            for (t,) in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE table_name SIMILAR TO '(dim|fct)_.*' ORDER BY 1"
            ).fetchall()
        ),
    )


def product_rows(sku: str) -> str:
    return f"""SELECT product_key, sku, version, category, subcategory, valid_from, valid_to, is_inferred
               FROM dim_product WHERE sku = '{sku}' ORDER BY valid_from"""


def query(con: duckdb.DuckDBPyConnection, files: list[Path]) -> None:
    for f in files:
        print(f"-- {f}")
        for stmt in con.extract_statements(f.read_text()):
            text = "\n".join(line for line in stmt.query.splitlines() if not line.lstrip().startswith("--"))
            started = time.monotonic()
            rel = con.sql(text.strip().rstrip(";"))
            if rel is not None:
                rel.show(max_width=140)
                print(f"({time.monotonic() - started:.2f}s)")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["build", "query"])
    p.add_argument("files", nargs="*", type=Path)
    p.add_argument("--local", action="store_true", help="read data/raw instead of the bucket")
    a = p.parse_args()
    raw_root = str(ROOT / "data/raw") if a.local else BUCKET_RAW
    if a.command == "build":
        DB.unlink(missing_ok=True)
        build(connect(raw_root=raw_root))
    else:  # queries read the star; a few also read raw reference data (FX rates, price changes)
        query(connect(raw_root=raw_root), a.files)
