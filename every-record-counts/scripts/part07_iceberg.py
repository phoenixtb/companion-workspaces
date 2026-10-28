"""Part 7: Apache Iceberg in depth, on the lab's Polaris catalog.

uv run python scripts/duck.py --local sql/part07/stage.sql           # stage POS lines and clicks for Spark
uv run python -m scripts.part07_iceberg reset                        # drop the part07 tables and their files
uv run python -m scripts.part07_iceberg spark 01_load.sql [...]      # run sql/part07/spark/*.sql in the Spark container
uv run python -m scripts.part07_iceberg tree                         # walk sales_line's metadata tree by hand
uv run python -m scripts.part07_iceberg conflict                     # PyIceberg and Spark write the same table
uv run python -m scripts.part07_iceberg engines                      # who can read the v2 and v3 tables (host engines)

`spark` needs `make up-spark`; everything else needs `make up-core`. Trino reads are in sql/part07/trino/.
"""

import argparse
import json
import struct
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import fastavro
import pyarrow.parquet as pq

from scripts import part04_star as star

ROOT = Path(__file__).resolve().parents[1]
POLARIS = "http://localhost:8181/api/catalog"
TABLES = ["sales_line", "daily_store_sales_cow", "daily_store_sales_mor", "clicks_v2", "clicks_v3", "store_closure"]


def catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "lab",
        type="rest",
        uri=POLARIS,
        warehouse="larkspur",
        credential="root:s3cr3t",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "oauth2-server-uri": f"{POLARIS}/v1/oauth/tokens",
            "header.X-Iceberg-Access-Delegation": "",  # no vended credentials: use the keys below
            "s3.endpoint": "http://localhost:8333",
            "s3.access-key-id": "larkspur",
            "s3.secret-access-key": "larkspur-lab-secret",
            "s3.region": "us-east-1",
        },
    )


def reset() -> None:
    cat = catalog()
    if ("part07",) not in cat.list_namespaces():
        cat.create_namespace("part07")
    for ident in cat.list_tables("part07"):
        cat.drop_table(ident)
    # Dropping a table forgets it; the files stay (Part 1's Gotcha). Remove them too.
    fs = star.fs()
    if fs.exists("larkspur-lake/warehouse/part07"):
        fs.rm("larkspur-lake/warehouse/part07", recursive=True)
    print("part07: no tables, no files")


def spark(files: list[str], strict: bool = False, capture: bool = False) -> tuple[int, str]:
    """Run sql/part07/spark/<file> in the Spark container; return the exit code and, if captured, the output."""
    sys.stdout.flush()  # keep our own output in order with the container's
    cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-submit", "/opt/lakehouse/jobs/run_sql.py"]
    cmd += ["--strict"] if strict else []
    cmd += [f"/opt/lakehouse/sql/part07/spark/{f}" for f in files]
    out = subprocess.PIPE if capture else None
    run = subprocess.run(cmd, cwd=ROOT, stdout=out, stderr=subprocess.PIPE, text=True)
    if run.returncode:
        print(run.stderr[-3000:], file=sys.stderr)
    return run.returncode, run.stdout or ""


# ---------- tree: the metadata tree, read as plain files ----------


def token() -> str:
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": "root",
            "client_secret": "s3cr3t",
            "scope": "PRINCIPAL_ROLE:ALL",
        }
    ).encode()
    return json.load(urllib.request.urlopen(f"{POLARIS}/v1/oauth/tokens", data=body))["access_token"]


def load_table_response(table: str) -> dict:
    req = urllib.request.Request(
        f"{POLARIS}/v1/larkspur/namespaces/part07/tables/{table}", headers={"Authorization": f"Bearer {token()}"}
    )
    return json.load(urllib.request.urlopen(req))


def name(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def day(b: bytes) -> date:  # a day-transform partition value: int32 days since 1970-01-01, little-endian
    return date(1970, 1, 1) + timedelta(days=struct.unpack("<i", b)[0])


def micros(b: bytes) -> str:  # a timestamp bound: int64 microseconds since the epoch, little-endian
    return datetime.fromtimestamp(struct.unpack("<q", b)[0] / 1e6, UTC).strftime("%Y-%m-%d %H:%M:%S")


def tree(table: str = "sales_line", pick_day: str = "2026-09-30") -> None:
    fs = star.fs()
    location = load_table_response(table)["metadata-location"]
    print(f"1. catalog: GET {POLARIS}/v1/larkspur/namespaces/part07/tables/{table}")
    print(f'   "metadata-location": "{location}"')

    meta = json.loads(fs.cat(location))
    schema = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    print(f"2. metadata file: {name(location)} ({fs.size(location):,} bytes, JSON)")
    print(f"   format-version {meta['format-version']}, last-sequence-number {meta['last-sequence-number']}")
    print("   schema: " + ", ".join(f"{f['id']}={f['name']}" for f in schema["fields"]))
    for spec in meta["partition-specs"]:
        fields = ", ".join(f"{f['name']} = {f['transform']}(field {f['source-id']})" for f in spec["fields"])
        print(f"   partition spec {spec['spec-id']}: {fields}")
    print("   refs: " + ", ".join(f"{n} ({r['type']}) -> {r['snapshot-id']}" for n, r in meta["refs"].items()))
    snap = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
    print(
        f"   current snapshot {snap['snapshot-id']}: {snap['summary']['operation']}, sequence {snap['sequence-number']}"
    )

    print(f"3. manifest list: {name(snap['manifest-list'])} ({fs.size(snap['manifest-list']):,} bytes, Avro)")
    with fs.open(snap["manifest-list"], "rb") as f:
        manifests = list(fastavro.reader(f))
    for m in manifests:
        bounds = m["partitions"][0]
        print(
            f"   {name(m['manifest_path'])}: content {m['content']}, {m['added_files_count']} added files, "
            f"{m['added_rows_count']:,} rows, days {day(bounds['lower_bound'])}..{day(bounds['upper_bound'])}"
        )

    manifest = manifests[0]["manifest_path"]
    with fs.open(manifest, "rb") as f:
        entries = list(fastavro.reader(f))
    print(
        f"4. manifest: {name(manifest)} ({fs.size(manifest):,} bytes, Avro), {len(entries)} entries; "
        f"the one for {pick_day}:"
    )
    e = next(e for e in entries if str(e["data_file"]["partition"]["event_ts_day"]) == pick_day)
    d = e["data_file"]
    lower, upper = (
        dict((b["key"], b["value"]) for b in d["lower_bounds"]),
        dict((b["key"], b["value"]) for b in d["upper_bounds"]),
    )
    print(f"   status {e['status']} (added), snapshot_id {e['snapshot_id']}")
    print(f"   file_path .../data/{'/'.join(d['file_path'].split('/')[-2:])}")
    print(f"   record_count {d['record_count']:,}, file_size_in_bytes {d['file_size_in_bytes']:,}")
    print(f"   bounds of field 6 (event_ts): {micros(lower[6])} .. {micros(upper[6])}")
    print(f"   bounds of field 1 (basket_id): {lower[1].decode()} .. {upper[1].decode()}")

    with fs.open(d["file_path"], "rb") as f:
        footer = pq.ParquetFile(f).schema_arrow
    ids = ", ".join(f"{fld.name}={fld.metadata[b'PARQUET:field_id'].decode()}" for fld in list(footer)[:4])
    print(f"5. data file: Parquet, {d['record_count']:,} rows; field IDs in its footer: {ids}, ...")


# ---------- conflict: two engines, one table ----------


def conflict() -> None:
    getattr(sys.stdout, "reconfigure", lambda **_: None)(
        line_buffering=True
    )  # PyIceberg's retry warning goes to stderr; keep the order
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.expressions import And, EqualTo

    cat = catalog()
    stale = cat.load_table("part07.daily_store_sales_mor")  # the data-science job reads the table: snapshot S
    base = stale.current_snapshot().snapshot_id
    print(f"PyIceberg loaded daily_store_sales_mor at snapshot {base}")

    print("Spark commits an UPDATE to GB-0042 on 30 September in the meantime:")
    if spark(["conflict_update.sql"], strict=True)[0]:
        raise SystemExit("the Spark UPDATE failed")
    print(f"main is now {cat.load_table('part07.daily_store_sales_mor').current_snapshot().snapshot_id}")

    row = And(EqualTo("store_id", "GB-0042"), EqualTo("business_date", "2026-09-30"))
    print("PyIceberg, still on its old snapshot, deletes the same store-day:")
    try:
        stale.delete(row)
        print(f"  committed {stale.current_snapshot().snapshot_id}")
    except (CommitFailedException, Exception) as e:  # noqa: BLE001 - print what the catalog or validation said
        print(f"  {type(e).__name__}: {str(e).splitlines()[0][:220]}")


# ---------- engines: who reads what ----------


def engines() -> None:
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    for label, sql in [
        ("DuckDB 2.0, v3 rows after the DV", "SELECT count(*) FROM lake.part07.clicks_v3"),
        (
            "DuckDB 2.0, v3 variant path",
            "SELECT payload.sku::VARCHAR FROM lake.part07.clicks_v3 WHERE event_type = 'product_view' "
            "GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1",
        ),
        ("DuckDB 2.0, v2 rows after the delete file", "SELECT count(*) FROM lake.part07.clicks_v2"),
    ]:
        print(f"{label:<44} {con.execute(sql).fetchone()[0]}")
    con.close()
    cat = catalog()
    for table in ("clicks_v2", "clicks_v3"):
        try:
            rows = cat.load_table(f"part07.{table}").scan().to_arrow().num_rows
            print(f"{'PyIceberg 0.12, ' + table:<44} {rows}")
        except Exception as e:  # noqa: BLE001 - the point is which engine fails, and how
            reason = next((line.strip() for line in str(e).splitlines() if "Unsupported" in line), str(e))
            print(f"{'PyIceberg 0.12, ' + table:<44} {type(e).__name__}: {reason.split(' [')[0]}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["reset", "spark", "tree", "conflict", "engines"])
    p.add_argument("files", nargs="*")
    a = p.parse_args()
    if a.command == "spark":
        sys.exit(spark(a.files)[0])
    {"reset": reset, "tree": tree, "conflict": conflict, "engines": engines}[a.command]()
