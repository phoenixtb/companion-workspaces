"""Part 9: the catalog as control plane. Principals, roles and grants in Polaris; four engines on one table.

uv run python -m scripts.part09_catalog reset      # drop the Part 9 tables and roles (and namespaces, if empty)
uv run python -m scripts.part09_catalog setup      # vending on; namespaces, principals, roles, grants (plain HTTP)
uv run python -m scripts.part09_catalog load       # the pipeline principal writes a raw and a gold table
uv run python -m scripts.part09_catalog engines    # PyIceberg, DuckDB, Trino and Spark read the same gold table
uv run python -m scripts.part09_catalog access     # what the analyst may and may not do, engine by engine
uv run python -m scripts.part09_catalog pointer    # what the catalog stores vs what object storage holds
uv run python -m scripts.part09_catalog load-table # the loadTable answer: where vended keys travel
uv run python -m scripts.part09_catalog duckdb-vended  # DuckDB in vended mode: which S3 endpoint it used

Needs `make up-core`; `engines` and `access` use Trino (`make up-trino`) and `engines` Spark (`make up-spark`) when
they run. `load` needs Part 8's staged gold file (`uv run python scripts/duck.py --local sql/part08/stage.sql`).
"""

import argparse
import base64
import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
POLARIS = "http://localhost:8181/api"
CATALOG = "larkspur"
NAMESPACES = ["raw", "silver", "gold"]
RAW_DAY = "2026-09-30"  # one upload day of raw POS events becomes raw.pos_events
GOLD_SOURCE = ROOT / "data/lab/part08/daily_store_sales.parquet"  # Part 8's gold table, staged by DuckDB
PART9_TABLES = ["raw.pos_events", "gold.daily_store_sales"]
S3 = "http://localhost:8333"

# Lab-only principals. Polaris generates random credentials; `setup` resets them to these fixed values so that
# Trino's catalog file (trino/catalog/lakehouse_analyst.properties) can name them.
PRINCIPALS = {"analyst": "analyst-lab-secret", "pipeline": "pipeline-lab-secret"}

# Privileges go to catalog roles, catalog roles to principal roles, principal roles to principals.
READ = ["TABLE_LIST", "TABLE_READ_DATA"]
WRITE = [*READ, "TABLE_CREATE", "TABLE_DROP", "TABLE_WRITE_DATA"]
CATALOG_ROLES = {
    "gold_reader": [("catalog", None, "NAMESPACE_LIST")] + [("namespace", "gold", p) for p in READ],
    "lake_writer": [("catalog", None, "NAMESPACE_LIST")] + [("namespace", ns, p) for ns in NAMESPACES for p in WRITE],
}
PRINCIPAL_ROLES = {"analysts": ("analyst", "gold_reader"), "pipelines": ("pipeline", "lake_writer")}

# Credential vending: Polaris assumes this role on SeaweedFS's STS (seaweedfs/iam.json) and narrows every session
# to one table's prefix, read-only unless the principal may write the table.
VENDING = {"roleArn": "arn:aws:iam::000000000000:role/polaris-vending", "stsEndpoint": "http://seaweedfs:8333"}

MGMT = "/management/v1"
CAT = f"/catalog/v1/{CATALOG}"


# ---------- HTTP ----------


def token(client_id: str = "root", secret: str = "s3cr3t") -> str:
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        }
    ).encode()
    return json.load(urllib.request.urlopen(f"{POLARIS}/catalog/v1/oauth/tokens", data=body))["access_token"]


def call(method: str, path: str, body: dict | None = None, tok: str | None = None, headers: dict | None = None):
    """One REST call; returns (status, parsed body). HTTP errors come back as values, not exceptions."""
    req = urllib.request.Request(
        f"{POLARIS}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {tok or token()}", "Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, json.loads(raw) if raw else None


def ok(status: int, what: str, accept: tuple = (200, 201, 204)) -> None:
    if status not in accept:
        raise SystemExit(f"{what}: HTTP {status}")


# ---------- reset / setup ----------


def lake_fs():
    from scripts import part04_star as star

    return star.fs()


def drop_table(ns: str, name: str, tok: str) -> None:
    call("DELETE", f"{CAT}/namespaces/{ns}/tables/{name}", tok=tok)
    fs = lake_fs()
    if fs.exists(f"larkspur-lake/warehouse/{ns}/{name}"):  # a drop forgets the table; the files stay (Part 1)
        fs.rm(f"larkspur-lake/warehouse/{ns}/{name}", recursive=True)


def reset() -> None:
    tok = token()
    # Only Part 9's own tables: later parts keep theirs in the same governed namespaces (Part 16's CDC replicas,
    # Part 17's dbt models), and they take minutes to rebuild. A namespace that still holds them isn't dropped.
    for ident in PART9_TABLES:
        ns, name = ident.split(".")
        drop_table(ns, name, tok)
    for ns in NAMESPACES:
        call("DELETE", f"{CAT}/namespaces/{ns}", tok=tok)  # refused unless empty
    for role in PRINCIPAL_ROLES:
        call("DELETE", f"{MGMT}/principal-roles/{role}", tok=tok)
    for role in CATALOG_ROLES:
        call("DELETE", f"{MGMT}/catalogs/{CATALOG}/catalog-roles/{role}", tok=tok)
    # Principals stay: Trino holds an OAuth token for `analyst`, and a re-created principal (new id) would make
    # every Trino request fail with 401 until Trino restarts. `setup` resets their credentials anyway.
    print("part09: no raw.pos_events or gold.daily_store_sales, no Part 9 roles (principals kept, namespaces in use)")


def setup() -> None:
    tok = token()
    # 1. Credential vending: give the catalog an STS endpoint and a role to assume (an update of Part 0's catalog).
    status, cat = call("GET", f"{MGMT}/catalogs/{CATALOG}", tok=tok)
    ok(status, "get catalog")
    storage = cat["storageConfigInfo"]
    if storage.get("stsUnavailable") or storage.get("roleArn") != VENDING["roleArn"]:
        new = {**storage, **VENDING, "stsUnavailable": False}
        status, body = call(
            "PUT",
            f"{MGMT}/catalogs/{CATALOG}",
            {"currentEntityVersion": cat["entityVersion"], "storageConfigInfo": new},
            tok=tok,
        )
        if status != 200:
            raise SystemExit(f"enable credential vending: HTTP {status}: {body['error']['message'][:120]}")
    print(f"catalog {CATALOG}: vends credentials by assuming {VENDING['roleArn']}")

    # 2. Namespaces belong to the platform team (root here): no role below may create or drop one.
    for ns in NAMESPACES:
        status, _ = call("POST", f"{CAT}/namespaces", {"namespace": [ns]}, tok=tok)
        ok(status, f"namespace {ns}", (200, 409))
    print("namespaces:", ", ".join(NAMESPACES))

    # 3. Principals (OAuth clients), reset to fixed lab credentials.
    for name, secret in PRINCIPALS.items():
        status, _ = call("POST", f"{MGMT}/principals", {"principal": {"name": name}}, tok=tok)
        ok(status, f"principal {name}", (201, 409))
        status, _ = call("POST", f"{MGMT}/principals/{name}/reset", {"clientId": name, "clientSecret": secret}, tok=tok)
        ok(status, f"reset {name}")

    # 4. Catalog roles and their privileges.
    for role, grants in CATALOG_ROLES.items():
        status, _ = call("POST", f"{MGMT}/catalogs/{CATALOG}/catalog-roles", {"catalogRole": {"name": role}}, tok=tok)
        ok(status, f"catalog role {role}", (201, 409))
        for kind, ns, privilege in grants:
            grant = {"type": kind, "privilege": privilege} | ({"namespace": [ns]} if ns else {})
            status, _ = call("PUT", f"{MGMT}/catalogs/{CATALOG}/catalog-roles/{role}/grants", {"grant": grant}, tok=tok)
            ok(status, f"grant {privilege} to {role}")

    # 5. Principal roles carry catalog roles; principals carry principal roles.
    for prole, (principal, crole) in PRINCIPAL_ROLES.items():
        status, _ = call("POST", f"{MGMT}/principal-roles", {"principalRole": {"name": prole}}, tok=tok)
        ok(status, f"principal role {prole}", (201, 409))
        body = {"catalogRole": {"name": crole}}
        ok(
            call("PUT", f"{MGMT}/principal-roles/{prole}/catalog-roles/{CATALOG}", body, tok=tok)[0],
            f"{crole} → {prole}",
        )
        body = {"principalRole": {"name": prole}}
        ok(call("PUT", f"{MGMT}/principals/{principal}/principal-roles", body, tok=tok)[0], f"{prole} → {principal}")
    print()
    print_grants(tok)


def grants_by_securable(crole: str, tok: str) -> dict[str, list[str]]:
    _, body = call("GET", f"{MGMT}/catalogs/{CATALOG}/catalog-roles/{crole}/grants", tok=tok)
    out: dict[str, list[str]] = {}
    for g in body["grants"]:
        where = f"namespace {'.'.join(g['namespace'])}" if g["type"] == "namespace" else g["type"]
        out.setdefault(where, []).append(g["privilege"])
    return {k: sorted(v) for k, v in sorted(out.items())}


def print_grants(tok: str) -> None:
    print(f"{'principal':<10} {'principal role':<15} {'catalog role':<12} {'on':<17} privileges")
    for prole, (principal, crole) in PRINCIPAL_ROLES.items():
        for i, (where, privs) in enumerate(grants_by_securable(crole, tok).items()):
            left = f"{principal:<10} {prole:<15} {crole:<12}" if i == 0 else " " * 39
            print(f"{left} {where:<17} {', '.join(p.removeprefix('TABLE_') for p in privs)}")


# ---------- clients, one per engine ----------


def pyiceberg(principal: str, vended: bool = True):
    """PyIceberg as `principal`. With `vended`, it holds no storage keys: they come with each loaded table."""
    from pyiceberg.catalog import load_catalog

    props = {
        "uri": f"{POLARIS}/catalog",
        "warehouse": CATALOG,
        "credential": f"{principal}:{PRINCIPALS.get(principal, 's3cr3t')}",
        "scope": "PRINCIPAL_ROLE:ALL",
        "oauth2-server-uri": f"{POLARIS}/catalog/v1/oauth/tokens",
        "s3.endpoint": S3,  # where to send requests; the keys come from the catalog
        "s3.region": "us-east-1",
    }
    if not vended:  # opt out of vending and bring our own keys (Parts 1-8)
        props |= {
            "header.X-Iceberg-Access-Delegation": "",
            "s3.access-key-id": "larkspur",
            "s3.secret-access-key": "larkspur-lab-secret",
        }
    return load_catalog(principal, type="rest", **props)


def duck(principal: str, key: str, secret: str) -> duckdb.DuckDBPyConnection:
    """DuckDB attached to Polaris as `principal`, reading files with a static S3 key (see the Part 9 post on why)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    sql = (ROOT / "sql/part09/attach.sql").read_text()
    for k, v in {"principal": principal, "client_secret": PRINCIPALS[principal], "key": key, "secret": secret}.items():
        sql = sql.replace("{" + k + "}", v)
    for stmt in con.extract_statements(sql):
        con.execute(stmt)
    return con


def trino(sql: str) -> tuple[list, str | None]:
    """Run one Trino statement; return (rows, None) or ([], the error message with its causes)."""
    req = urllib.request.Request(
        "http://localhost:8080/v1/statement", data=sql.encode(), headers={"X-Trino-User": "lab"}
    )
    page, rows = json.load(urllib.request.urlopen(req)), []
    while True:
        rows += page.get("data", [])
        if "error" in page:
            info, msgs = page["error"].get("failureInfo"), []
            while info:
                msg = short(info.get("message") or "")
                if msg and msg not in msgs:
                    msgs.append(msg)
                info = info.get("cause")
            return [], " ← ".join(msgs)
        if "nextUri" not in page:
            return rows, None
        page = json.load(urllib.request.urlopen(page["nextUri"]))


def up(port: int) -> bool:
    import socket

    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("localhost", port)) == 0


def spark_up() -> bool:
    out = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "--services"], cwd=ROOT, capture_output=True, text=True
    ).stdout
    return "spark" in out.split()


# ---------- load: the pipeline principal writes ----------


def raw_events() -> pa.Table:
    """One upload day of raw POS events from the raw bucket, flattened: nested lines and tenders kept as JSON text."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # Arrow timestamps in UTC; PyIceberg rejects other zones
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    return con.sql(f"""
        SELECT event_id, event_type, store_id, till_id, basket_id, event_ts, uploaded_at, currency, loyalty_id,
               to_json(lines)::VARCHAR AS lines_json, to_json(tenders)::VARCHAR AS tenders_json
        FROM read_json('s3://larkspur-raw/raw/pos/events/upload_date={RAW_DAY}/*.jsonl.gz')
        ORDER BY event_ts, event_id
    """).to_arrow_table()


def load() -> dict:
    if not GOLD_SOURCE.exists():
        raise SystemExit(f"missing {GOLD_SOURCE}: run `uv run python scripts/duck.py --local sql/part08/stage.sql`")
    cat, tok = pyiceberg("pipeline"), token()
    out = {}
    for ident, data in [("raw.pos_events", raw_events()), ("gold.daily_store_sales", pq.read_table(GOLD_SOURCE))]:
        ns, name = ident.split(".")
        drop_table(ns, name, tok)  # re-runs start from an empty table
        table = cat.create_table(ident, schema=data.schema)
        table.append(data)
        key = table.io.properties.get("s3.access-key-id", "")
        out[ident] = table.scan().count()
        print(f"{ident:<23} {out[ident]:>7,} rows  written with vended key {key[:8]}… (pipeline holds no lake key)")
    return out


# ---------- engines: one gold table, four readers ----------

GOLD_TOTALS = "SELECT count(*) AS store_days, sum(sales_minor) AS sales_minor FROM {t}"


def engines() -> list[tuple]:
    rows = []
    t = pyiceberg("analyst").load_table("gold.daily_store_sales").scan().to_arrow()
    rows.append(("PyIceberg 0.12", "analyst", "vended", t.num_rows, pc.sum(t["sales_minor"]).as_py()))
    con = duck("analyst", "lake-reader", "lake-reader-lab-secret")
    n, s = con.execute(GOLD_TOTALS.format(t="lake.gold.daily_store_sales")).fetchone()
    rows.append(("DuckDB 2.0", "analyst", "static read key", n, int(s)))
    if up(8080):
        data, err = trino(GOLD_TOTALS.format(t="lakehouse_analyst.gold.daily_store_sales"))
        rows.append(("Trino 484", "analyst", "vended", *(data[0] if data else (err, None))))
    else:
        print("Trino is not running (make up-trino): skipped")
    if spark_up():
        rows.append(("Spark 4.1.3", "analyst", "vended", *spark_totals()))
    else:
        print("Spark is not running (make up-spark): skipped")
    print(f"{'engine':<15} {'principal':<9} {'storage credentials':<19} {'store_days':>10} {'sales_minor':>17}")
    for engine, who, creds, n, s in rows:
        print(f"{engine:<15} {who:<9} {creds:<19} {n:>10,} {s:>17,}")
    return rows


# A second Spark catalog, `analyst`, next to spark-defaults' `lakehouse` (root, static keys): no storage keys, and
# the header that asks Polaris to vend them. Spark's own S3 endpoint setting still says where to send requests.
SPARK_ANALYST = {
    "spark.sql.catalog.analyst": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.analyst.type": "rest",
    "spark.sql.catalog.analyst.uri": "http://polaris:8181/api/catalog",
    "spark.sql.catalog.analyst.warehouse": CATALOG,
    "spark.sql.catalog.analyst.credential": f"analyst:{PRINCIPALS['analyst']}",
    "spark.sql.catalog.analyst.scope": "PRINCIPAL_ROLE:ALL",
    "spark.sql.catalog.analyst.header.X-Iceberg-Access-Delegation": "vended-credentials",
    "spark.sql.catalog.analyst.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.analyst.s3.endpoint": "http://seaweedfs:8333",
    "spark.sql.catalog.analyst.s3.path-style-access": "true",
    "spark.sql.catalog.analyst.client.region": "us-east-1",
}


def spark_sql(sql: str) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-sql", "-S"]
    for k, v in SPARK_ANALYST.items():
        cmd += ["--conf", f"{k}={v}"]
    return subprocess.run([*cmd, "-e", sql], cwd=ROOT, capture_output=True, text=True)


def spark_totals() -> tuple[int, int]:
    run = spark_sql(GOLD_TOTALS.format(t="analyst.gold.daily_store_sales"))
    if run.returncode:
        raise SystemExit(run.stderr[-2000:])
    n, s = run.stdout.split()[-2:]
    return int(n), int(s)


# ---------- access: what the analyst may not do ----------


def short(msg: str) -> str:
    """One line, with file names and request IDs shortened so the interesting part fits."""
    msg = msg.strip().splitlines()[0]
    msg = re.sub(r"\d{8}_\d{6}_\d{5}_\w+-[0-9a-f-]{36}\.parquet", "….parquet", msg)  # Trino's data file names
    msg = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}\.parquet", "….parquet", msg)  # other writers' data file names
    return re.sub(r" ?\(Request ID: \w+\)| ?\(Service: S3.*?\)| ?\(SDK Attempt Count: \d+\)", "", msg)


def first_line(e: Exception) -> str:
    return short(f"{type(e).__name__}: {e}")


def access() -> dict[str, str]:
    """The analyst tries three things in each engine: read gold, read raw, write gold."""
    out: dict[str, str] = {}
    cat = pyiceberg("analyst")
    gold = cat.load_table("gold.daily_store_sales")
    out["PyIceberg read gold"] = f"ok, {gold.scan().count():,} rows"
    try:
        cat.load_table("raw.pos_events")
        out["PyIceberg read raw"] = "ok (unexpected)"
    except Exception as e:
        out["PyIceberg read raw"] = first_line(e)
    try:
        gold.append(pq.read_table(GOLD_SOURCE).slice(0, 1))
        out["PyIceberg write gold"] = "ok (unexpected)"
    except Exception as e:
        out["PyIceberg write gold"] = first_line(e)

    con = duck("analyst", "lake-reader", "lake-reader-lab-secret")
    for what, sql in [
        ("DuckDB read raw", "SELECT count(*) FROM lake.raw.pos_events"),
        ("DuckDB write gold", "INSERT INTO lake.gold.daily_store_sales FROM lake.gold.daily_store_sales LIMIT 1"),
    ]:
        try:
            con.sql(sql).fetchall() if sql.startswith("SELECT") else con.execute(sql)
            out[what] = "ok (unexpected)"
        except duckdb.Error as e:
            out[what] = first_line(e)

    if up(8080):
        for what, sql in [
            ("Trino list schemas", "SHOW SCHEMAS FROM lakehouse_analyst"),
            ("Trino read raw", "SELECT count(*) FROM lakehouse_analyst.raw.pos_events"),
            (
                "Trino write gold",
                "INSERT INTO lakehouse_analyst.gold.daily_store_sales "
                "SELECT * FROM lakehouse_analyst.gold.daily_store_sales LIMIT 1",
            ),
        ]:
            rows, err = trino(sql)
            out[what] = err or ("ok, " + ", ".join(r[0] for r in rows) if "SCHEMAS" in sql else f"ok {rows}")
    else:
        print("Trino is not running (make up-trino): skipped")
    if spark_up():
        run = spark_sql("SELECT count(*) FROM analyst.raw.pos_events")
        errors = [line for line in run.stderr.splitlines() if line.startswith("org.apache.iceberg.exceptions")]
        out["Spark read raw"] = short(errors[0].removeprefix("org.apache.iceberg.exceptions.")) if errors else "ok?"
    else:
        print("Spark is not running (make up-spark): skipped")

    out.update(storage_bypass(gold))
    for what, result in out.items():
        print(f"{what:<28} {result}")
    return out


def storage_bypass(gold) -> dict[str, str]:
    """Go around the catalog: read raw.pos_events' files straight from object storage, with two kinds of key."""
    from pyarrow import fs as pafs

    raw_glob = "s3://larkspur-lake/warehouse/raw/pos_events/data/*.parquet"
    out = {}
    # 1. A static read key for the lake bucket, the kind DuckDB holds here: the catalog's refusal doesn't apply.
    con = duckdb.connect()
    con.execute(
        "CREATE SECRET (TYPE s3, KEY_ID 'lake-reader', SECRET 'lake-reader-lab-secret', REGION 'us-east-1', "
        "ENDPOINT 'localhost:8333', URL_STYLE 'path', USE_SSL false)"
    )
    n = con.execute(f"SELECT count(*) FROM read_parquet('{raw_glob}')").fetchone()[0]
    out["raw files, static read key"] = f"ok, {n:,} rows (loyalty_id included)"
    # 2. The credentials Polaris vended to the analyst for the gold table: scoped to that table's prefix.
    p = gold.io.properties
    s3 = pafs.S3FileSystem(
        access_key=p["s3.access-key-id"],
        secret_key=p["s3.secret-access-key"],
        session_token=p["s3.session-token"],
        endpoint_override=S3,
        region="us-east-1",
    )
    first_raw = lake_fs().glob(raw_glob.removeprefix("s3://"))[0]
    for what, path in [("gold metadata, vended key", gold.metadata_location), ("raw file, vended key", first_raw)]:
        try:
            with s3.open_input_file(path.removeprefix("s3://")) as f:
                out[what] = f"ok, {len(f.read()):,} bytes"
        except OSError as e:
            out[what] = "denied: " + re.search(r"AWS Error \w+ during \w+ operation", str(e)).group(0)
    # SeaweedFS's session token is a JWT that carries the session policy Polaris asked for (AWS's is opaque).
    claims = p["s3.session-token"].split(".")[1]
    policy = json.loads(json.loads(base64.urlsafe_b64decode(claims + "=" * (-len(claims) % 4)))["spol"])
    s3 = [st for st in policy["Statement"] if any(a.startswith("s3:") for a in st["Action"])]
    actions = sorted(a for st in s3 for a in st["Action"])
    objects = sorted({r.removeprefix("arn:aws:s3:::") for st in s3 for r in st["Resource"] if "/" in r})
    prefixes = sorted({st["Condition"]["StringLike"]["s3:prefix"] for st in s3 if "Condition" in st})
    out["vended session policy"] = (
        f"{', '.join(actions)}; objects {', '.join(objects)}; list prefix {', '.join(prefixes)}"
    )
    return out


def load_table_answer(principal: str = "analyst", table: str = "gold.daily_store_sales") -> dict:
    """The raw loadTable JSON with vended credentials requested: where the keys travel."""
    ns, name = table.split(".")
    path = f"{CAT}/namespaces/{ns}/tables/{name}"
    _, body = call("GET", path, tok=token(principal, PRINCIPALS[principal]), headers=DELEGATION)
    creds = body.get("storage-credentials", [])
    print(f"GET {path} (as {principal}, X-Iceberg-Access-Delegation: vended-credentials)")
    print(f"  top-level keys:      {', '.join(body)}")
    print(f"  config keys:         {', '.join(sorted(body['config']))}")
    for c in creds:
        print(f"  storage-credentials: prefix {c['prefix']}: {', '.join(sorted(c['config']))}")
    return body


DELEGATION = {"X-Iceberg-Access-Delegation": "vended-credentials"}


def duckdb_vended() -> list[str]:
    """DuckDB in vended mode, with HTTP logging: where did it send the keys? (Not in the tests: it calls AWS.)"""
    con = duckdb.connect()
    sql = (
        (ROOT / "sql/part09/attach.sql")
        .read_text()
        .replace("ACCESS_DELEGATION_MODE 'none'", "ACCESS_DELEGATION_MODE 'vended_credentials'")
    )
    for k, v in {"principal": "analyst", "client_secret": PRINCIPALS["analyst"], "key": "-", "secret": "-"}.items():
        sql = sql.replace("{" + k + "}", v)
    for stmt in con.extract_statements(sql):
        if "TYPE s3" not in stmt.query:  # no static S3 key in this session
            con.execute(stmt)
    con.execute("CALL enable_logging('HTTP')")
    try:
        con.execute("SELECT count(*) FROM lake.gold.daily_store_sales").fetchall()
        print("read: ok")
    except duckdb.Error as e:
        print("read:", short(str(e)))
    hosts = con.execute(
        "SELECT DISTINCT regexp_extract(request.url, '^https?://[^/]+') FROM duckdb_logs_parsed('HTTP') ORDER BY 1"
    ).fetchall()
    for (h,) in hosts:
        print("requests went to:", h)
    return [h for (h,) in hosts]


# ---------- pointer: catalog row vs object storage ----------


def pointer(table: str = "gold.daily_store_sales") -> dict:
    ns, name = table.split(".")
    _, body = call("GET", f"{CAT}/namespaces/{ns}/tables/{name}")
    sql = (
        "SELECT name, entity_version, internal_properties->>'metadata-location' AS metadata_location, "
        "pg_column_size(e.*) AS row_bytes FROM polaris_schema.entities e "
        f"WHERE name = '{name}' AND internal_properties->>'parent-namespace' = '{ns}'"
    )
    psql = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "larkspur", "-d", "polaris", "-At", "-F", "|"]
    row = subprocess.run([*psql, "-c", sql], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    _, version, location, row_bytes = row.split("|")
    print(f"catalog (PostgreSQL row in polaris_schema.entities, {int(row_bytes):,} bytes, entity_version {version}):")
    print(f"  metadata-location = {location}")
    print(f"  same as the REST loadTable answer: {location == body['metadata-location']}")
    fs = lake_fs()
    files = fs.find(f"larkspur-lake/warehouse/{ns}/{name}", detail=True)
    kinds: dict[str, list[int]] = {}
    for path, info in files.items():
        kind = (
            "metadata (JSON)"
            if path.endswith(".metadata.json")
            else "manifest list (Avro)"
            if "/snap-" in path
            else "manifest (Avro)"
            if path.endswith(".avro")
            else "data (Parquet)"
        )
        kinds.setdefault(kind, []).append(info["size"])
    print(f"object storage (s3://larkspur-lake/warehouse/{ns}/{name}/):")
    for kind, sizes in sorted(kinds.items()):
        print(f"  {kind:<21} {len(sizes):>2} objects {sum(sizes):>9,} bytes")
    return {"location": location, "rest": body["metadata-location"], "row_bytes": int(row_bytes), "kinds": kinds}


def main() -> None:
    p = argparse.ArgumentParser()
    steps = {
        "reset": reset,
        "setup": setup,
        "load": load,
        "engines": engines,
        "access": access,
        "pointer": pointer,
        "load-table": load_table_answer,
        "duckdb-vended": duckdb_vended,
    }
    p.add_argument("step", choices=list(steps))
    steps[p.parse_args().step]()


if __name__ == "__main__":
    sys.exit(main())
