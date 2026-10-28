"""Part 8: one gold table in Iceberg, Delta Lake and DuckLake; where each keeps its metadata; what small updates write.

uv run python scripts/duck.py --local sql/part08/stage.sql   # daily store sales, recomputed from Part 7's staging
uv run python -m scripts.part08_formats reset                # drop the part08 tables, their files, the DuckLake catalog
uv run python -m scripts.part08_formats load                 # Iceberg and DuckLake (DuckDB), two Delta tables (Spark)
uv run python -m scripts.part08_formats layout               # what each format keeps where
uv run python -m scripts.part08_formats late                 # one late basket, then 19 more: what each commit writes
uv run python -m scripts.part08_formats flush                # DuckLake: move inlined rows into a Parquet file
uv run python -m scripts.part08_formats interop              # who can read and write what

Needs `make up-spark` (core + Spark): Spark writes the Delta tables, DuckDB everything else, from the host.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

import duckdb
import psycopg

from scripts import part04_star as star
from scripts import part07_iceberg as p7

ROOT = Path(__file__).resolve().parents[1]
STAGED = ROOT / "data/lab/part08/daily_store_sales.parquet"
PREFIX = {  # where each table's files live in SeaweedFS
    "Iceberg": "larkspur-lake/warehouse/part08/daily_store_sales",
    "Delta (DVs)": "larkspur-lake/delta/part08/delta_dv",
    "Delta (UniForm)": "larkspur-lake/delta/part08/delta_uniform",
    "DuckLake": "larkspur-lake/ducklake",
}
TABLE = {  # the same tables as DuckDB names them (Delta: by path) and as the late-basket UPDATE targets them
    "Iceberg": "lake.part08.daily_store_sales",
    "Delta (DVs)": "part08.delta_dv",
    "Delta (UniForm)": "part08.delta_uniform",
    "DuckLake": "dl.daily_store_sales",
}
FILER = "http://localhost:8888"  # SeaweedFS filer HTTP API
PG = "host=localhost port=5432 user=larkspur password=larkspur"


def duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    con.execute((ROOT / "sql/part08/ducklake_attach.sql").read_text())
    return con


def spark(*args: str) -> str:
    """spark-submit inside the Spark container with the Delta Lake session config (spark/delta.conf.template)."""
    sys.stdout.flush()
    cmd = 'exec /opt/spark/bin/spark-submit --properties-file "$SPARK_CONF_DIR/delta.conf" ' + " ".join(args)
    run = subprocess.run(
        ["docker", "compose", "exec", "-T", "spark", "bash", "-c", cmd], cwd=ROOT, capture_output=True, text=True
    )
    if run.returncode:
        raise SystemExit(run.stderr[-3000:])
    return run.stdout


def late_basket_sql(table: str, store_id: str) -> str:
    text = (ROOT / "sql/part08/late_basket.sql").read_text()
    sql = "\n".join(line for line in text.splitlines() if not line.startswith("--"))
    return sql.format(table=table, store_id=store_id)


# ---------- reset and load ----------


def reset() -> None:
    cat = p7.catalog()
    if ("part08",) not in cat.list_namespaces():
        cat.create_namespace("part08")
    for ident in cat.list_tables("part08"):
        cat.drop_table(ident)
    # Delete through SeaweedFS's filer, not S3: S3A writes "directory" marker objects, and SeaweedFS keeps them as
    # real directories after the files under them are deleted, so Delta would find a non-empty location.
    for prefix in ("larkspur-lake/warehouse/part08", "larkspur-lake/delta", "larkspur-lake/ducklake"):
        req = urllib.request.Request(
            f"{FILER}/buckets/{prefix}?recursive=true&ignoreRecursiveError=true", method="DELETE"
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
    with psycopg.connect(f"{PG} dbname=larkspur", autocommit=True) as pg:
        pg.execute("DROP DATABASE IF EXISTS ducklake WITH (FORCE)")
        pg.execute("CREATE DATABASE ducklake")
    # Spark's Hive metastore (embedded Derby) forgets the Delta tables too.
    subprocess.run(["docker", "compose", "exec", "-T", "spark", "rm", "-rf", "/tmp/part08-metastore"], cwd=ROOT)
    print("part08: no tables, no files, an empty DuckLake catalog")


def load() -> None:
    con = duck()
    for fmt in ("Iceberg", "DuckLake"):
        started = time.monotonic()
        con.execute(f"CREATE TABLE {TABLE[fmt]} AS FROM read_parquet('{STAGED}')")
        print(f"> CREATE TABLE {TABLE[fmt]} AS FROM read_parquet(...)  ({time.monotonic() - started:.1f}s)")
    con.close()
    print(spark("/opt/lakehouse/jobs/run_sql.py", "/opt/lakehouse/sql/part08/spark/01_delta.sql"), end="")


# ---------- layout: what each format keeps, and where ----------


def kind(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    if "/_delta_log/" in path:
        if name.endswith(".checkpoint.parquet"):
            return "Delta checkpoint (Parquet)"
        if name.endswith(".json"):
            return "Delta commit (JSON)"
        if name.endswith(".crc"):
            return "Delta checksum (.crc)"
        return f"Delta log: {name}"
    if "/metadata/" in path:
        if name.endswith(".metadata.json"):
            return "Iceberg metadata (JSON)"
        if name.startswith("snap-"):
            return "Iceberg manifest list (Avro)"
        return "Iceberg manifest (Avro)"
    if name.startswith("deletion_vector_"):
        return "deletion vector (.bin)"
    if "delete" in name:
        return "delete file (Parquet)"
    return "data (Parquet)" if name.endswith(".parquet") else f"other: {name}"


def objects() -> dict[str, dict[str, int]]:
    """Every object under each table's prefix: path -> size."""
    fs = star.fs()
    out = {}
    for fmt, prefix in PREFIX.items():
        fs.invalidate_cache()
        found = fs.find(prefix, detail=True) if fs.exists(prefix) else {}
        out[fmt] = {p: i["size"] for p, i in found.items() if not p.endswith("/")}  # S3A leaves "directory" markers
    return out


def catalog_rows() -> dict[str, int]:
    """Rows in each table of DuckLake's catalog database (PostgreSQL)."""
    with psycopg.connect(f"{PG} dbname=ducklake") as pg:
        tables = [
            r[0]
            for r in pg.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1"
            )
        ]
        return {t: pg.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables}


def summarise(objs: dict[str, dict[str, int]]) -> list[tuple]:
    rows = []
    for fmt, files in objs.items():
        kinds, sizes = Counter(), Counter()
        for path, size in files.items():
            kinds[kind(path)] += 1
            sizes[kind(path)] += size
        rows += [(fmt, k, kinds[k], sizes[k]) for k in sorted(kinds)]
    return rows


def table(header: tuple, rows: list[tuple]) -> None:
    cells = [[str(h) for h in header]] + [[f"{v:,}" if isinstance(v, int) else str(v) for v in r] for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(header))]
    for n, r in enumerate(cells):
        print("  ".join(v.rjust(w) for v, w in zip(r, widths, strict=True)))
        if n == 0:
            print("  ".join("-" * w for w in widths))


def layout() -> None:
    table(("format", "object kind", "objects", "bytes"), summarise(objects()))
    print()
    rows = catalog_rows()
    used = [(t, n) for t, n in rows.items() if n]
    print(f"DuckLake catalog (PostgreSQL database `ducklake`): {len(rows)} tables, {len(used)} with rows")
    table(("table", "rows"), used)


# ---------- late: small updates, one commit each ----------


def diff(before: dict, after: dict) -> list[tuple]:
    added = {fmt: {p: s for p, s in after[fmt].items() if p not in before[fmt]} for fmt in after}
    return summarise(added)


def late(first: int = 1, last: int = 20) -> dict:
    con = duck()

    def update(stores: range) -> None:
        for n in stores:
            for fmt in ("Iceberg", "DuckLake"):
                con.execute(late_basket_sql(TABLE[fmt], f"GB-{n:04d}"))
        delta = (TABLE["Delta (DVs)"], TABLE["Delta (UniForm)"])
        print(spark("/opt/lakehouse/jobs/part08_late_baskets.py", str(stores[0]), str(stores[-1]), *delta), end="")

    o0, c0 = objects(), catalog_rows()
    update(range(first, first + 1))
    o1, c1 = objects(), catalog_rows()
    print(f"\nOne late basket (GB-{first:04d}, 30 September), one UPDATE per table; objects written:")
    table(("format", "object kind", "objects", "bytes"), diff(o0, o1))
    added = {t: c1[t] - c0.get(t, 0) for t in c1 if c1[t] != c0.get(t)}
    print("DuckLake catalog rows added: " + ", ".join(f"{t} +{n}" for t, n in added.items()))

    update(range(first + 1, last + 1))
    o2, c2 = objects(), catalog_rows()
    print(f"\nAll {last - first + 1} late baskets (one UPDATE commit each), per table:")
    rows = []
    for fmt in PREFIX:
        new = {p: s for p, s in o2[fmt].items() if p not in o0[fmt]}
        data = [s for p, s in new.items() if kind(p) == "data (Parquet)"]
        rows.append((fmt, len(new), len(data), sum(data), len(new) - len(data)))
    table(("format", "objects", "data files", "data bytes", "other objects"), rows)
    print(f"DuckLake catalog rows: {sum(c0.values()):,} -> {sum(c2.values()):,}")
    ice = sorted((p, n) for p, n in o2["Iceberg"].items() if p.endswith(".metadata.json"))
    print(f"Iceberg metadata file: {ice[0][1]:,} bytes at load, {ice[-1][1]:,} bytes after the 20th commit")
    checkpoints = sorted(p.rsplit("/", 1)[-1] for p in o2["Delta (DVs)"] if p.endswith(".checkpoint.parquet"))
    print(f"Delta checkpoints: {', '.join(checkpoints)}")
    return {"objects": (o0, o1, o2), "catalog": (c0, c1, c2)}


def flush() -> int:
    con = duck()
    before = len(objects()["DuckLake"])
    flushed = con.execute("CALL ducklake_flush_inlined_data('dl')").fetchall()
    print(
        f"rows flushed: {sum(int(r[-1]) for r in flushed)}; DuckLake objects: {before} -> {len(objects()['DuckLake'])}"
    )
    inlined = {t: n for t, n in catalog_rows().items() if t.startswith("ducklake_inlined_") and t[-1].isdigit()}
    print("inlined rows left: " + ", ".join(f"{t} {n}" for t, n in inlined.items()))
    return sum(inlined.values())


# ---------- interop: one table, several readers ----------


def uniform_metadata() -> list[tuple[int, str]]:
    """UniForm's Iceberg metadata files as (Delta version converted, path), oldest first."""
    fs = star.fs()
    fs.invalidate_cache()
    found = [p for p in fs.find(PREFIX["Delta (UniForm)"] + "/metadata") if p.endswith(".metadata.json")]
    return sorted((int(json.loads(fs.cat(p))["properties"]["delta-version"]), p) for p in found)


def interop() -> list[tuple]:
    con = duck()
    meta = uniform_metadata()
    newest = meta[-1][1]
    reads = {
        "Iceberg via Polaris": TABLE["Iceberg"],
        "DuckLake": TABLE["DuckLake"],
        "Delta (DVs), delta_scan": f"delta_scan('s3://{PREFIX['Delta (DVs)']}')",
        "Delta (UniForm), delta_scan": f"delta_scan('s3://{PREFIX['Delta (UniForm)']}')",
        "Delta (UniForm), iceberg_scan": f"iceberg_scan('s3://{newest}')",
    }
    rows = []
    for label, source in reads.items():
        n, total = con.execute(f"SELECT count(*), sum(sales_minor) FROM {source}").fetchall()[0]
        rows.append((label, n, int(total)))
    table(("reader (DuckDB 2.0)", "rows", "sales_minor"), rows)

    print(f"\nUniForm Iceberg metadata files: {len(meta)} (Delta versions {meta[0][0]}..{meta[-1][0]}); the newest:")
    print(f"  {newest.rsplit('/', 1)[-1]}")
    con.sql(
        f"SELECT snapshot_id, operation, summary['total-records'] AS records FROM iceberg_snapshots('s3://{newest}')"
    ).show()
    con.execute(f"ATTACH 's3://{PREFIX['Delta (DVs)']}' AS delta_dv (TYPE delta)")
    try:
        con.execute(late_basket_sql("delta_dv.main.delta_dv", "GB-0021"))
        print("DuckDB UPDATE on the Delta table: ran")
    except duckdb.Error as e:
        print(f"DuckDB UPDATE on the Delta table: {type(e).__name__}: {str(e).splitlines()[0]}")
    return rows


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["reset", "load", "layout", "late", "flush", "interop"])
    a = p.parse_args()
    {"reset": reset, "load": load, "layout": layout, "late": late, "flush": flush, "interop": interop}[a.command]()
