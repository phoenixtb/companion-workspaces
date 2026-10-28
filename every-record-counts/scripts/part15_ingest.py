"""Part 15: batch ingestion with dlt into Iceberg raw tables, registered in Polaris.

uv run python -m scripts.part15_ingest reset                         # drop Part 15's tables, dlt state and pipelines
uv run python -m scripts.part15_ingest suppliers --through 2026-08-19 # land the supplier files that had arrived by then
uv run python -m scripts.part15_ingest suppliers --through 2026-08-19 # run it again: nothing new
uv run python -m scripts.part15_ingest suppliers --through 2026-08-20 --ignore-aliases  # S017's renamed column
uv run python -m scripts.part15_ingest suppliers                      # with the alias: every file since
uv run python -m scripts.part15_ingest checks                         # account for every landed row
uv run python -m scripts.part15_ingest sellers --pull first           # needs the mock API (part15_sellers_api)
uv run python -m scripts.part15_ingest sellers --pull second          # add --lag 0 to see the high-water-mark trap
uv run python -m scripts.part15_ingest reconcile                      # full extract vs the incremental replica
uv run python -m scripts.part15_ingest loads                          # load ids, snapshots, what each load wrote

Needs `make up-core` and `make upload`. dlt's filesystem destination writes Iceberg tables with PyIceberg through
Polaris as Part 9's `pipeline` principal, which holds no storage key: Polaris vends one per table. dlt's own
bookkeeping (load records, pipeline state) is plain files next to the tables, written with the lab's S3 key.
"""

import argparse
import csv
import io
import json
import re
import shutil
import urllib.request
import warnings
from datetime import UTC, date, datetime
from pathlib import Path

import dlt
import duckdb
import openpyxl
import pyarrow as pa
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.typedef import EMPTY_DICT

from scripts import part09_catalog as cat9
from scripts.part15_sellers_api import PORT, iso, parse

ROOT = Path(__file__).resolve().parents[1]
FEED = json.loads((ROOT / "ingest/supplier_feed.json").read_text())
PIPELINES = ROOT / "data/lab/part15/pipelines"  # dlt's local working state; the destination keeps a copy
SQL = ROOT / "sql/part15"
API = f"http://localhost:{PORT}"
LAKE = "s3://larkspur-lake/warehouse"  # the catalog's allowed location; dlt puts each dataset in a folder below it
FIRST_SEEN = datetime(2024, 1, 1, tzinfo=UTC)  # initial value of the sellers cursor
LAG_S = 900  # re-read the last 15 minutes below the high-water mark on every pull

# PyIceberg warns when an overwrite's delete finds nothing, as every first overwrite of an empty table does.
warnings.filterwarnings("ignore", message="Delete operation did not match any records")

RAW_TABLES = ["supplier_deliveries", "supplier_files", "marketplace_sellers", "marketplace_sellers_full"]
SILVER_TABLES = ["supplier_deliveries", "supplier_delivery_quarantine"]


# ---------- destination: dlt filesystem + Iceberg via Polaris ----------


def configure(principal: str = "pipeline") -> None:
    """Point dlt's Iceberg support at Polaris. Keys follow PyIceberg's REST catalog properties."""
    secret = cat9.PRINCIPALS.get(principal, "s3cr3t")
    dlt.secrets["iceberg_catalog.iceberg_catalog_name"] = "polaris"
    dlt.secrets["iceberg_catalog.iceberg_catalog_type"] = "rest"
    dlt.secrets["iceberg_catalog.iceberg_catalog_config"] = {
        # dlt requires `type`; PyIceberg refuses it next to `py-catalog-impl`, so it stays empty.
        "type": "",
        "py-catalog-impl": "scripts.part15_ingest.PolarisRestCatalog",
        "uri": f"{cat9.POLARIS}/catalog",
        "warehouse": cat9.CATALOG,
        "credential": f"{principal}:{secret}",
        "scope": "PRINCIPAL_ROLE:ALL",
        "oauth2-server-uri": f"{cat9.POLARIS}/catalog/v1/oauth/tokens",
        "s3.endpoint": cat9.S3,  # keys arrive with each table (credential vending, Part 9)
        "s3.region": "us-east-1",
    }


class PolarisRestCatalog(RestCatalog):
    """PyIceberg's REST catalog, but `create_namespace` checks first.

    dlt 1.31 calls create_namespace for its dataset on every load and accepts only "already exists" (409). Polaris
    checks the grant before existence and answers 403, and Part 9's rule is that pipelines never create
    namespaces. So: if the namespace exists, say so the way dlt expects; otherwise ask (and be refused).
    """

    def create_namespace(self, namespace, properties=EMPTY_DICT) -> None:
        if self.namespace_exists(namespace):
            raise NamespaceAlreadyExistsError(f"Namespace already exists: {namespace}")
        super().create_namespace(namespace, properties)


def destination():
    return dlt.destinations.filesystem(
        bucket_url=LAKE,
        credentials={  # for dlt's bookkeeping files only; table data goes through PyIceberg with vended keys
            "aws_access_key_id": "larkspur",
            "aws_secret_access_key": "larkspur-lab-secret",
            "endpoint_url": cat9.S3,
            "region_name": "us-east-1",
        },
    )


def pipeline(name: str, dataset: str = "raw") -> dlt.Pipeline:
    return dlt.pipeline(name, destination=destination(), dataset_name=dataset, pipelines_dir=str(PIPELINES))


def catalog(principal: str = "pipeline"):
    return cat9.pyiceberg(principal)


def scan(ident: str, principal: str = "pipeline") -> pa.Table:
    return catalog(principal).load_table(ident).scan().to_arrow()


# ---------- reset ----------


def reset() -> None:
    tok = cat9.token()
    status, _ = cat9.call("GET", f"{cat9.CAT}/namespaces/raw", tok=tok)
    if status != 200:  # Part 9's namespaces, principals and grants are a prerequisite
        cat9.setup()
    for ns, names in [("raw", RAW_TABLES), ("silver", SILVER_TABLES)]:
        for name in names:
            cat9.drop_table(ns, name, tok)
    status, body = cat9.call("GET", f"{cat9.CAT}/namespaces/part15/tables", tok=tok)
    for ident in body["identifiers"] if status == 200 else []:
        cat9.drop_table("part15", ident["name"], tok)
    cat9.call("DELETE", f"{cat9.CAT}/namespaces/part15", tok=tok)
    fs = cat9.lake_fs()
    dlt_files = fs.glob("larkspur-lake/warehouse/raw/_dlt_*") + fs.glob("larkspur-lake/warehouse/raw/init")
    for d in dlt_files + fs.glob("larkspur-lake/warehouse/part15"):
        fs.rm(d, recursive=True)
    shutil.rmtree(PIPELINES, ignore_errors=True)
    print("part15: no Part 15 tables, no dlt state (local or in the bucket)")


# ---------- suppliers: files from the landing zone ----------


def landing_files() -> list[dict]:
    """Every file in the landing zone with what object storage says about it, and what its name says."""
    fs = cat9.lake_fs()
    pattern = re.compile(FEED["file_name"])
    out = []
    prefix = FEED["landing_zone"].removeprefix("s3://")
    for path, info in sorted(fs.find(prefix, detail=True).items()):
        m = pattern.match(Path(path).name)
        out.append(
            {
                "path": f"s3://{path}",
                "etag": info.get("ETag", "").strip('"'),
                "size": info["size"],
                "supplier_id": m["supplier_id"] if m else None,
                "file_date": datetime.strptime(m["file_date"], "%Y%m%d").date() if m else None,
                "format": m["format"] if m else None,
            }
        )
    return out


def read_rows(fmt: str, body: bytes) -> list[list[str | None]]:
    """A file as sent: header row first, every value text (or None for an empty XLSX cell)."""
    if fmt == "csv":
        return list(csv.reader(io.StringIO(body.decode("utf-8"), newline="")))
    ws = openpyxl.load_workbook(io.BytesIO(body), read_only=True).active
    return [[None if v is None else str(v) for v in row] for row in ws.iter_rows(values_only=True)]


def supplier_source(through: date | None, ignore_aliases: bool, ingested_at: datetime):
    aliases = {} if ignore_aliases else FEED["header_aliases"]
    fs = cat9.lake_fs()

    @dlt.resource(
        name="supplier_deliveries",
        table_format="iceberg",
        write_disposition="append",  # raw is append-only: a re-sent file is a new version, never an overwrite
        schema_contract={"columns": "freeze"},  # an unknown header stops the load instead of adding a column
    )
    def deliveries():
        loaded = dlt.current.resource_state().setdefault("files", {})  # path -> ETag of the version loaded
        for f in landing_files():
            if f["supplier_id"] is None:  # not ours to guess: report it and leave it in the landing zone
                print(f"  skipped, name breaks the convention: {f['path']}")
                continue
            if (through and f["file_date"] > through) or loaded.get(f["path"]) == f["etag"]:
                continue
            header, *rows = read_rows(f["format"], fs.cat_file(f["path"].removeprefix("s3://")))
            names = [aliases.get(f["supplier_id"], {}).get(h, h) for h in header]
            meta = {"supplier_id": f["supplier_id"], "file_date": f["file_date"], "_source_file": f["path"]}
            yield dlt.mark.with_table_name(
                {
                    **meta,
                    "format": f["format"],
                    "etag": f["etag"],
                    "size_bytes": f["size"],
                    "header": " | ".join(h or "" for h in header),  # as sent, before any alias
                    "data_rows": len(rows),
                    "_ingested_at": ingested_at,
                },
                "supplier_files",
            )
            yield [
                {**dict(zip(names, row, strict=True)), **meta, "_row_number": n, "_ingested_at": ingested_at}
                for n, row in enumerate(rows, start=2)  # line 1 is the header
            ]
            loaded[f["path"]] = f["etag"]

    return deliveries


def suppliers(through: date | None = None, ignore_aliases: bool = False) -> dict:
    configure()
    p = pipeline("larkspur_suppliers")
    ingested_at = datetime.now(UTC).replace(microsecond=0)
    try:
        info = p.run(supplier_source(through, ignore_aliases, ingested_at))
    except Exception as e:  # dlt wraps step failures in PipelineStepFailed
        p.abort_packages()  # otherwise the next run retries the same failed package first
        cause = e.__cause__ or e
        message = str(cause).splitlines()[0]
        print(f"FAILED: {type(cause).__name__}: {message[message.find('In schema') :]}")
        return {"failed": type(cause).__name__}
    counts = p.last_trace.last_normalize_info.row_counts if info.loads_ids else {}
    out = {
        "load_ids": info.loads_ids,
        "files": counts.get("supplier_files", 0),
        "rows": counts.get("supplier_deliveries", 0),
    }
    out.update(table_totals("raw", ["supplier_files", "supplier_deliveries"]))
    loads = ", ".join(info.loads_ids) or "(none)"
    print(f"through {through or 'the latest file'}: load {loads}: {out['files']:,} new files, {out['rows']:,} new rows")
    for name in ["supplier_files", "supplier_deliveries"]:
        print(f"  raw.{name:<22} {out[name]['rows']:>7,} rows  {out[name]['snapshots']} snapshots")
    return out


def table_totals(ns: str, names: list[str]) -> dict:
    c = catalog()
    out = {}
    for name in names:
        try:
            t = c.load_table(f"{ns}.{name}")
        except Exception:
            out[name] = {"rows": 0, "snapshots": 0}
            continue
        out[name] = {"rows": t.scan().count(), "snapshots": len(t.snapshots())}
    return out


# ---------- checks: flag, quarantine, account for every row ----------


def checks() -> dict:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("raw_supplier_deliveries", scan("raw.supplier_deliveries"))
    con.register("raw_supplier_files", scan("raw.supplier_files"))
    formats = [(k, v) for k, v in FEED["date_format"].items() if k != "default"]
    con.execute("CREATE TABLE declared_date_format (supplier_id VARCHAR, date_format VARCHAR)")
    con.executemany("INSERT INTO declared_date_format VALUES (?, ?)", formats)
    con.execute("SET VARIABLE default_date_format = ?", [FEED["date_format"]["default"]])
    con.execute((SQL / "supplier_checks.sql").read_text())
    out = {}
    for name in ["accounting", "quarantine_reasons", "headers"]:
        rel = con.sql((SQL / f"report_{name}.sql").read_text())
        rel.show(max_width=140)
        out[name] = rel.fetchall()
    c = catalog()
    for name, query in [
        ("supplier_deliveries", "SELECT * FROM delivery_valid"),
        ("supplier_delivery_quarantine", "SELECT * FROM delivery_quarantine"),
    ]:
        data = con.sql(query).to_arrow_table()
        ident = f"silver.{name}"
        t = c.load_table(ident) if c.table_exists(ident) else c.create_table(ident, schema=data.schema)
        t.overwrite(data)  # silver is rebuilt from raw on every run: same raw, same silver
        out[name] = t.scan().count()
        print(f"silver.{name:<29} {out[name]:>7,} rows")
    return out


# ---------- sellers: the marketplace API ----------


def admin(path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{API}{path}", data=json.dumps(body).encode() if body is not None else None, method="POST" if body else "GET"
    )
    req.add_header("Content-Type", "application/json")
    return json.load(urllib.request.urlopen(req))


def seller_source(lag: float):
    client = RESTClient(
        base_url=API,
        paginator=JSONResponseCursorPaginator(cursor_path="next_cursor", cursor_param="cursor"),
        data_selector="data",
    )

    def typed(page: list[dict]) -> list[dict]:
        return [{**r, "created_at": parse(r["created_at"]), "updated_at": parse(r["updated_at"])} for r in page]

    @dlt.resource(
        name="marketplace_sellers",
        table_format="iceberg",
        primary_key="seller_id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},  # a re-read row replaces, never doubles
    )
    def changed(
        updated_at=dlt.sources.incremental("updated_at", initial_value=FIRST_SEEN, lag=lag or None),  # noqa: B008
    ):
        since = updated_at.start_value  # the stored high-water mark minus the lag
        print(f"  incremental: updated_since={iso(since)}")
        for page in client.paginate("/v1/sellers", params={"updated_since": iso(since), "limit": 100}):
            yield typed(page)

    @dlt.resource(name="marketplace_sellers_full", table_format="iceberg", write_disposition="replace")
    def everything():
        for page in client.paginate("/v1/sellers", params={"updated_since": iso(FIRST_SEEN), "limit": 100}):
            yield typed(page)

    return changed, everything


def sellers(pull: str, lag: float = LAG_S) -> dict:
    import time

    from datagen import sellers as world

    clock = {"first": world.FIRST_PULL, "second": world.SECOND_PULL}[pull]
    admin("/_admin/clock", {"at": iso(clock)})
    principal, dataset, name = ("pipeline", "raw", "larkspur_sellers") if lag else ("root", "part15", "sellers_nolag")
    configure(principal)
    p = pipeline(name, dataset)
    changed, everything = seller_source(lag)
    print(f"pull {pull} at {iso(clock)} (lag {lag:g} s) into {dataset}.marketplace_sellers")
    started = time.monotonic()
    info = p.run(changed)
    elapsed = time.monotonic() - started
    stats = admin("/_admin/stats")
    rows = p.last_trace.last_normalize_info.row_counts.get("marketplace_sellers", 0) if info.loads_ids else 0
    t = catalog(principal).load_table(f"{dataset}.marketplace_sellers")
    out = {
        "rows_fetched": rows,
        "pages": stats["pages"],
        "throttled": stats["throttled"],
        "table_rows": t.scan().count(),
        "snapshots": [snapshot_line(s) for s in t.snapshots()],
        "seconds": round(elapsed, 1),
    }
    print(f"  {rows:,} rows in {stats['pages']} pages, {stats['throttled']} answered 429 and retried, {elapsed:.1f} s")
    print(f"  {dataset}.marketplace_sellers: {out['table_rows']:,} rows, {len(out['snapshots'])} snapshots:")
    print(f"    {' | '.join(out['snapshots'])}")
    if pull == "second" and lag:
        admin("/_admin/clock", {"at": iso(clock)})  # fresh rate-limit window
        p.run(everything)
        out["full_rows"] = catalog().load_table("raw.marketplace_sellers_full").scan().count()
        print(f"  full extract: raw.marketplace_sellers_full {out['full_rows']:,} rows (replace)")
    return out


def snapshot_line(s) -> str:
    """One snapshot as its operation and the records it added and deleted, e.g. 'overwrite -1,200 +1,152'."""
    props = s.summary.additional_properties
    deleted = int(props.get("deleted-records", 0))
    return f"{s.summary.operation.value}{f' -{deleted:,}' if deleted else ''} +{int(props.get('added-records', 0)):,}"


def reconcile() -> dict:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("replica", scan("raw.marketplace_sellers"))
    con.register("full_extract", scan("raw.marketplace_sellers_full"))
    out = {}
    replica = [("raw (lag 900 s)", "replica")]
    try:
        con.register("replica_nolag", scan("part15.marketplace_sellers", "root"))
        replica.append(("part15 (no lag)", "replica_nolag"))
    except Exception:
        pass
    template = (SQL / "sellers_reconcile.sql").read_text()
    for label, table in replica:
        print(f"== {label} vs the full extract")
        rel = con.sql(template.replace("{replica}", table))
        rel.show(max_width=140)
        out[label] = rel.fetchall()
    return out


# ---------- load metadata ----------


def loads() -> list:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    t = catalog().load_table("raw.supplier_deliveries")
    con.register("raw_supplier_deliveries", t.scan().to_arrow())
    snaps = pa.table(
        {
            "snapshot_id": [s.snapshot_id for s in t.snapshots()],
            "committed_at": [datetime.fromtimestamp(s.timestamp_ms / 1000, UTC) for s in t.snapshots()],
            "added_rows": [int(s.summary["added-records"]) for s in t.snapshots()],
        }
    )
    con.register("snapshots", snaps)
    rel = con.sql((SQL / "report_loads.sql").read_text())
    rel.show(max_width=140)
    return rel.fetchall()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["reset", "suppliers", "checks", "sellers", "reconcile", "loads"])
    p.add_argument("--through", type=date.fromisoformat, help="only files dated up to this day (default: all)")
    p.add_argument("--ignore-aliases", action="store_true", help="land as if the S017 alias weren't declared yet")
    p.add_argument("--pull", choices=["first", "second"], default="first")
    p.add_argument("--lag", type=float, default=LAG_S, help="seconds re-read below the high-water mark (0: none)")
    a = p.parse_args()
    if a.command == "reset":
        reset()
    elif a.command == "suppliers":
        suppliers(a.through, a.ignore_aliases)
    elif a.command == "checks":
        checks()
    elif a.command == "sellers":
        sellers(a.pull, a.lag)
    elif a.command == "reconcile":
        reconcile()
    else:
        loads()


if __name__ == "__main__":
    main()
