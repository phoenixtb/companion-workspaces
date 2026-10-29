"""Part 23: locks on the lake. Classify personal data, mask and filter it per user through Trino + OPA, keep dev out
of production, and carry one customer's erasure request through every copy until the bytes are gone.

uv run python -m scripts.part23_privacy setup          # namespace customer, Trino's `gateway` principal, grants
uv run python -m scripts.part23_privacy customers      # CRM: crm.customers -> raw.customers -> customer.dim_customer
uv run python -m scripts.part23_privacy discover       # every table: columns whose names or values look personal
uv run python -m scripts.part23_privacy tag            # governance/classification.json -> Iceberg table properties
uv run python -m scripts.part23_privacy policy-data    # table properties -> opa/larkspur/classification/data.json
uv run python -m scripts.part23_privacy as USER "SQL"  # one statement through Trino as USER
uv run python -m scripts.part23_privacy access         # who may do what: analysts, dev vs prod, drops
uv run python -m scripts.part23_privacy audit          # OPA's decision log, summarised
uv run python -m scripts.part23_privacy find CUSTOMER_ID      # where one person is: tables, refs, snapshots
uv run python -m scripts.part23_privacy prove CUSTOMER_ID     # byte level: decode every object that could hold them
uv run python -m scripts.part23_privacy erase CUSTOMER_ID     # the request, end to end (source, lake, files, register)

Needs `make up-trino` (Trino with OPA) and Parts 16-21's tables. Trino users are plain names in the lab: nothing
authenticates them (the post says what production adds).
"""

import argparse
import gzip
import hashlib
import hmac
import io
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import fastavro
import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from scripts import part09_catalog as cat9

ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATION = ROOT / "governance/classification.json"
OPA_DATA = ROOT / "opa/larkspur/classification/data.json"
PG = "host=localhost port=5432 dbname=larkspur user=larkspur password=larkspur"
GOVERNED = ["raw", "silver", "gold", "customer"]  # Polaris namespaces with owners; partNN namespaces are lab copies
PIPELINE = "lakehouse_pipeline"  # Trino catalog of the pipeline principal (Part 17)
TOMBSTONE = "erased"  # what replaces an identifier in records the law makes us keep
REGISTER = "customer.erasure_register"
REGISTER_KEY = b"larkspur-lab-register-key"  # lab only: production keeps it in a secret store, not in the repo
PROP = "larkspur.pii."  # one table property per classified column: larkspur.pii.<column> = direct|identifier|quasi

# ---------- Polaris: the customer namespace and Trino's own principal ----------

# Lab-only secrets (Trino reads the gateway's from GATEWAY_SECRET in .env; ClickHouse's is in its database DDL)
PRINCIPALS = {"gateway": "gateway-lab-secret", "olap_reader": "olap-reader-lab-secret"}
READ = ["TABLE_LIST", "TABLE_READ_DATA", "TABLE_READ_PROPERTIES", "NAMESPACE_READ_PROPERTIES"]
WRITE = [*READ, "TABLE_CREATE", "TABLE_DROP", "TABLE_WRITE_DATA", "TABLE_WRITE_PROPERTIES"]
CATALOG_ROLES = {
    # Trino's principal for people: reads gold and customer; OPA decides per user what comes back
    "gateway_reader": [("catalog", None, "NAMESPACE_LIST")]
    + [("namespace", ns, p) for ns in ("gold", "customer") for p in READ],
    # the pipeline writes the customer namespace as it writes raw/silver/gold (Part 9's lake_writer)
    "customer_writer": [("namespace", "customer", p) for p in WRITE],
    # ClickHouse (Part 13) read Polaris as root until Part 23; it needs one lab namespace, read only
    "olap_lab_reader": [("catalog", None, "NAMESPACE_LIST")] + [("namespace", "part10", p) for p in READ],
}  # fmt: skip
PRINCIPAL_ROLES = {
    "gateways": ("gateway", "gateway_reader"),
    "pipelines": ("pipeline", "customer_writer"),
    "olap_readers": ("olap_reader", "olap_lab_reader"),
}


def setup() -> None:
    tok = cat9.token()
    cat9.ok(
        cat9.call("POST", f"{cat9.CAT}/namespaces", {"namespace": ["customer"]}, tok=tok)[0], "customer", (200, 409)
    )
    for name, secret in PRINCIPALS.items():
        cat9.ok(
            cat9.call("POST", f"{cat9.MGMT}/principals", {"principal": {"name": name}}, tok=tok)[0], name, (201, 409)
        )
        reset = {"clientId": name, "clientSecret": secret}
        cat9.ok(cat9.call("POST", f"{cat9.MGMT}/principals/{name}/reset", reset, tok=tok)[0], f"reset {name}")
    for role, grants in CATALOG_ROLES.items():
        body = {"catalogRole": {"name": role}}
        cat9.ok(
            cat9.call("POST", f"{cat9.MGMT}/catalogs/{cat9.CATALOG}/catalog-roles", body, tok=tok)[0], role, (201, 409)
        )
        for kind, ns, privilege in grants:
            grant = {"type": kind, "privilege": privilege} | ({"namespace": [ns]} if ns else {})
            path = f"{cat9.MGMT}/catalogs/{cat9.CATALOG}/catalog-roles/{role}/grants"
            cat9.ok(cat9.call("PUT", path, {"grant": grant}, tok=tok)[0], f"{privilege} to {role}")
    for prole, (principal, crole) in PRINCIPAL_ROLES.items():
        cat9.ok(
            cat9.call("POST", f"{cat9.MGMT}/principal-roles", {"principalRole": {"name": prole}}, tok=tok)[0],
            prole,
            (201, 409),
        )
        path = f"{cat9.MGMT}/principal-roles/{prole}/catalog-roles/{cat9.CATALOG}"
        cat9.ok(cat9.call("PUT", path, {"catalogRole": {"name": crole}}, tok=tok)[0], f"{crole} -> {prole}")
        body = {"principalRole": {"name": prole}}
        cat9.ok(cat9.call("PUT", f"{cat9.MGMT}/principals/{principal}/principal-roles", body, tok=tok)[0], prole)
    key_table()
    print("namespace customer; principals gateway (gold + customer, read: Trino's `governed` catalog) and olap_reader "
          "(part10, read: ClickHouse)")  # fmt: skip
    drops()


KEY_TABLE = "customer.pseudonym_key"


def key_table() -> None:
    """The pseudonymisation key as a one-row table, written by the pipeline (PyIceberg: no SQL text that a query
    history could show) from PSEUDONYM_KEY. OPA lets only the `masker` identity read it; it is never classified, so
    analysts are refused it like any unclassified table."""
    key = os.environ.get("PSEUDONYM_KEY", "larkspur-lab-pseudonym-key").encode()  # lab default; production: a vault
    cat = cat9.pyiceberg("pipeline")
    data = pa.table({"key": pa.array([key], pa.binary())})
    if cat.table_exists(KEY_TABLE):
        cat.load_table(KEY_TABLE).overwrite(data)
    else:
        cat.create_table(KEY_TABLE, schema=data.schema).append(data)


def drops() -> None:
    """Who can drop (and, with drop-with-purge on, delete files at once): the TABLE_DROP holders, per namespace."""
    tok = cat9.token()
    _, cat = cat9.call("GET", f"{cat9.MGMT}/catalogs/{cat9.CATALOG}", tok=tok)
    print("polaris.config.drop-with-purge.enabled =", cat["properties"].get("polaris.config.drop-with-purge.enabled"))
    _, roles = cat9.call("GET", f"{cat9.MGMT}/principal-roles", tok=tok)
    for prole in sorted(r["name"] for r in roles["roles"]):
        _, crs = cat9.call("GET", f"{cat9.MGMT}/principal-roles/{prole}/catalog-roles/{cat9.CATALOG}", tok=tok)
        for crole in crs.get("roles", []):
            g = cat9.grants_by_securable(crole["name"], tok)
            where = sorted(k.removeprefix("namespace ") for k, v in g.items() if "TABLE_DROP" in v)
            if where:
                print(f"  TABLE_DROP  {prole:<10} via {crole['name']:<14} on {', '.join(where)}")


# ---------- Trino as a named user ----------


def trino(sql: str, user: str = "lab", session: dict[str, str] | None = None) -> tuple[list, list, str | None]:
    """One statement as `user`; returns (column names, rows, error). The user name is all OPA sees."""
    headers = {"X-Trino-User": user}
    if session:
        headers["X-Trino-Session"] = ",".join(f"{k}={v}" for k, v in session.items())
    req = urllib.request.Request(f"{cat9.TRINO}/v1/statement", data=sql.encode(), headers=headers)
    page, rows, cols = json.load(urllib.request.urlopen(req)), [], []
    while True:
        cols = cols or [c["name"] for c in page.get("columns", [])]
        rows += page.get("data", [])
        if "error" in page:
            return cols, [], cat9.short(page["error"]["message"])
        if "nextUri" not in page:
            return cols, rows, None
        page = json.load(urllib.request.urlopen(page["nextUri"]))


def must(sql: str, user: str = "lab", session: dict[str, str] | None = None) -> list:
    _, rows, err = trino(sql, user, session)
    if err:
        raise SystemExit(f"Trino as {user}: {err}\n  {sql[:200]}")
    return rows


def show(user: str, sql: str) -> None:
    cols, rows, err = trino(sql, user)
    print(f"-- as {user}")
    if err:
        print("   ", err)
        return
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(c) for i, c in enumerate(cols)]
    print("   ", "  ".join(f"{c:<{w}}" for c, w in zip(cols, widths, strict=True)))
    for r in rows:
        print("   ", "  ".join(f"{str(v):<{w}}" for v, w in zip(r, widths, strict=True)))


# ---------- the CRM: a small customer table with real personal fields (synthetic) ----------

FIRST = {
    "GB": ["Olivia", "Amelia", "Isla", "Ava", "Oliver", "George", "Arthur", "Noah", "Leo", "Freya", "Harry", "Grace"],
    "IN": ["Aarav", "Vivaan", "Aditya", "Ananya", "Diya", "Ishaan", "Saanvi", "Kavya", "Arjun", "Meera", "Rahul"],
    "SG": ["Wei Ling", "Jun Jie", "Hui Min", "Ethan", "Chloe", "Ryan", "Nur Aisyah", "Muhammad", "Priya", "Kai"],
}  # fmt: skip
LAST = {
    "GB": ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson", "Davies", "Patel", "Wright", "Evans"],
    "IN": ["Sharma", "Iyer", "Reddy", "Nair", "Gupta", "Menon", "Khan", "Das", "Rao", "Kulkarni", "Banerjee", "Singh"],
    "SG": ["Tan", "Lim", "Lee", "Ng", "Ong", "Wong", "Goh", "Chua", "Koh", "Teo", "Rahman", "Kumar"],
}  # fmt: skip
CITY = {"GB": ["London", "Manchester", "Leeds", "Bristol", "Glasgow"], "IN": ["Bengaluru", "Mumbai", "Delhi", "Pune",
        "Chennai"], "SG": ["Singapore"]}  # fmt: skip
STREET = ["Station Road", "High Street", "Park Avenue", "Church Lane", "Mill Road", "Lake View", "Hill Street"]


def customer_row(cid: str) -> dict:
    """Deterministic, synthetic personal fields for one customer ID (no real people; example.com addresses)."""
    h = int.from_bytes(hashlib.sha256(cid.encode()).digest()[:8], "big")
    cc = cid[1:3]
    first, last = FIRST[cc][h % len(FIRST[cc])], LAST[cc][(h >> 8) % len(LAST[cc])]
    n = h >> 16
    if cc == "GB":  # outward code + inward code; phone in Ofcom's drama range (07700 900000-900999)
        letters = "ABDEFGHJLNPQRSTUWXYZ"
        postcode = (
            f"{'NSEW'[n % 4]}{'EWC'[(n >> 2) % 3]}{n % 19 + 1} {n % 9 + 1}{letters[n % 20]}{letters[(n >> 5) % 20]}"
        )
        phone = f"+44 7700 900{n % 1000:03d}"
    elif cc == "IN":
        postcode, phone = f"{560001 + n % 900:06d}", f"+91 9{n % 10**9:09d}"
    else:
        postcode, phone = f"{(n % 82 + 1):02d}{n % 9999:04d}", f"+65 8{n % 10**7:07d}"
    return {
        "customer_id": cid,
        "loyalty_id": "L" + cid[1:],  # every online customer also holds the loyalty card with the same number
        "full_name": f"{first} {last}",
        "email": f"{first}.{last}.{int(cid[3:])}@example.com".lower().replace(" ", ""),
        "phone": phone,
        "address_line": f"{n % 180 + 1} {STREET[n % len(STREET)]}",
        "city": CITY[cc][(h >> 24) % len(CITY[cc])],
        "postcode": postcode,
        "country": cc,
        "marketing_consent": bool((h >> 32) % 3),
        "created_at": datetime(2024, 1, 1, tzinfo=UTC).replace(day=1 + (h >> 40) % 28, month=1 + (h >> 48) % 12),
    }


def customers() -> dict:
    """The CRM table in PostgreSQL (the source), its nightly copy in raw, and the customer mart people query."""
    con = duckdb.connect()
    ids = con.sql(
        f"SELECT DISTINCT customer_id FROM read_parquet(['{ROOT}/data/raw/oltp/orders/*/*.parquet', "
        f"'{ROOT}/data/raw/oltp/orders_poll/*/*.parquet']) ORDER BY 1"
    ).fetchall()
    # a reload must not bring back anyone who asked to be erased: the suppression list drops them
    data = suppress(pa.Table.from_pylist([customer_row(cid) for (cid,) in ids]), delete={"customer_id": "customer_id"})
    with psycopg.connect(PG, autocommit=True) as pg:
        pg.execute("CREATE SCHEMA IF NOT EXISTS crm")
        pg.execute("DROP TABLE IF EXISTS crm.customers")
        pg.execute("""CREATE TABLE crm.customers (customer_id text PRIMARY KEY, loyalty_id text, full_name text,
            email text, phone text, address_line text, city text, postcode text, country text,
            marketing_consent boolean, created_at timestamptz)""")
        with pg.cursor().copy("COPY crm.customers FROM STDIN") as cp:
            for r in data.to_pylist():
                cp.write_row(list(r.values()))
    cat = cat9.pyiceberg("pipeline")
    cat9.drop_table("raw", "customers", cat9.token())  # re-runs start from an empty table
    cat.create_table("raw.customers", schema=data.schema).append(data)
    must(f"""CREATE OR REPLACE TABLE {PIPELINE}.customer.dim_customer AS
        SELECT c.customer_id, c.loyalty_id, c.full_name, c.email, c.phone, c.address_line, c.city, c.postcode,
               c.country, c.marketing_consent, CAST(c.created_at AS date) AS customer_since,
               count(o.order_id) AS orders, CAST(min(o.created_at) AS date) AS first_order
        FROM {PIPELINE}.raw.customers c LEFT JOIN {PIPELINE}.raw.orders o ON o.customer_id = c.customer_id
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11""", "dbt")  # fmt: skip
    n = must(f"SELECT count(*) FROM {PIPELINE}.customer.dim_customer", "dbt")[0][0]
    print(f"crm.customers (PostgreSQL) -> raw.customers -> customer.dim_customer: {n:,} customers")
    return {"customers": n}


# ---------- discover: what looks personal, by name and by value ----------

NAME_RULES = [
    (r"e_?mail", "direct"), (r"phone|mobile", "direct"), (r"full_name|first_name|last_name|^name$", "direct"),
    (r"address", "direct"), (r"postcode|zip|pin_code", "quasi"), (r"(^|_)ip(_|$)|ip_address", "identifier"),
    (r"customer_id|loyalty_id|anonymous_id|user_id|device_id", "identifier"), (r"user_agent", "quasi"),
]  # fmt: skip
VALUE_RULES = [  # what a sampled value looks like or contains (an ID inside a longer string counts)
    (re.compile(r"^[\w.+-]+@[\w-]+\.[\w.]+$"), "email"),
    (re.compile(r"^\+\d{2} [\d ]{7,}$"), "phone"),
    (re.compile(r"\ba-C(GB|IN|SG)\d{7}\b"), "anonymous id"),
    (re.compile(r"\bC(GB|IN|SG)\d{7}\b"), "customer id"),
    (re.compile(r"\bL(GB|IN|SG)\d{7}\b"), "loyalty id"),
    (re.compile(r"^\d{1,3}(\.\d{1,3}){3}$"), "IPv4 address"),
]


def tables(namespaces: list[str] | None = None) -> list[str]:
    cat = cat9.pyiceberg("root", vended=False)
    out = []
    for ns in sorted(cat.list_namespaces()):
        if namespaces is None or ns[0] in namespaces:
            out += [".".join(t) for t in sorted(cat.list_tables(ns))]
    return out


def columns(table: str) -> list[tuple[str, str]]:
    ns, name = table.split(".")
    return must(f"SELECT column_name, data_type FROM lakehouse.information_schema.columns WHERE table_schema = '{ns}' "
                f"AND table_name = '{name}' ORDER BY ordinal_position")  # fmt: skip


def discover() -> list[tuple]:
    """Name rules on every column of every table; value rules on a 1,000-row sample of each string column."""
    found, all_tables = [], tables()
    for table in all_tables:
        cols = columns(table)
        by_name = {c: next((cls for rx, cls in NAME_RULES if re.search(rx, c)), None) for c, _ in cols}
        by_name = {c: cls for c, cls in by_name.items() if cls}
        text = [c for c, t in cols if t.startswith("varchar")]
        by_value = {}
        if text:
            sample = must(
                f"SELECT {', '.join(chr(34) + c + chr(34) for c in text)} FROM lakehouse.{table} "
                "ORDER BY rand() LIMIT 1000"
            )
            for i, c in enumerate(text):
                kinds = Counter(
                    next((k for rx, k in VALUE_RULES if rx.search(str(r[i]))), None) for r in sample if r[i]
                )
                kinds.pop(None, None)
                if kinds:
                    by_value[c] = kinds.most_common(1)[0][0]
        for c in sorted(set(by_name) | set(by_value)):
            found.append((table, c, by_name.get(c, "-"), by_value.get(c, "-")))
    print(f"{'table':<34} {'column':<18} {'by name':<11} by value (sample of 1,000 rows)")
    for t, c, n, v in found:
        print(f"{t:<34} {c:<18} {n:<11} {v}")
    print(f"{len(found)} columns in {len({f[0] for f in found})} of {len(all_tables)} tables")
    return found


# ---------- tag: the reviewed classification travels with each table ----------


def tag() -> dict:
    """Write governance/classification.json into each table's properties (as the catalog admin: stewards' job)."""
    reviewed = json.loads(CLASSIFICATION.read_text())
    cat = cat9.pyiceberg("root", vended=False)
    for table, spec in reviewed["tables"].items():
        t = cat.load_table(table)
        props = {f"{PROP}{c}": cls for c, cls in spec.get("columns", {}).items()}
        props["larkspur.classification"] = spec["level"]
        if "row-scope" in spec:
            props["larkspur.row-scope"] = spec["row-scope"]
        stale = [k for k in t.properties if k.startswith(PROP) and k not in props]
        with t.transaction() as tx:
            tx.set_properties(props)
            if stale:
                tx.remove_properties(*stale)
        print(f"{table:<30} {spec['level']:<9} {', '.join(f'{c}={k}' for c, k in spec.get('columns', {}).items())}")
    return reviewed


def policy_data() -> dict:
    """What OPA needs, read back from the tables themselves: classification per table, never a second copy."""
    cat = cat9.pyiceberg("root", vended=False)
    out = {}
    for table in tables(GOVERNED):
        p = cat.load_table(table).properties
        if "larkspur.classification" not in p:
            continue  # unclassified: OPA lets no analyst read it (fail closed)
        entry = {"level": p["larkspur.classification"], "columns": {}}
        entry["columns"] = {k.removeprefix(PROP): v for k, v in sorted(p.items()) if k.startswith(PROP)}
        if "larkspur.row-scope" in p:
            entry["row-scope"] = p["larkspur.row-scope"]
        out[table] = entry
    OPA_DATA.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    personal = {t: e for t, e in out.items() if e["columns"]}
    print(f"{OPA_DATA.relative_to(ROOT)}: {len(out)} classified tables, {len(personal)} with personal columns")
    unclassified = [t for t in tables(["gold", "customer"]) if t not in out]
    print("readable by analysts but unclassified (OPA refuses them):", ", ".join(unclassified) or "none")
    return out


# ---------- access: the decisions OPA now makes ----------

ACCESS = [
    ("analyst_gb", "SELECT count(*) FROM governed.gold.web_sessions"),
    ("analyst_gb", "SELECT count(*) FROM lakehouse_pipeline.raw.customers"),
    ("analyst_gb", "DROP TABLE governed.gold.web_sessions"),
    ("dbt_dev", "CREATE TABLE lakehouse_dev.dev.p23_probe AS SELECT 1 AS x"),
    ("dbt_dev", "DROP TABLE lakehouse_dev.dev.p23_probe"),
    ("dbt_dev", "SELECT count(*) FROM lakehouse_pipeline.gold.fct_basket"),
    ("dbt_dev", "CREATE TABLE lakehouse_pipeline.gold.p23_probe AS SELECT 1 AS x"),
    ("dbt_dev", "DROP TABLE lakehouse_pipeline.gold.fct_basket"),
    ("dbt_dev", "DELETE FROM lakehouse_pipeline.gold.fct_basket WHERE false"),
    ("dbt", "DELETE FROM lakehouse_pipeline.gold.fct_basket WHERE false"),
    ("intern", "SELECT 1"),
]


def access() -> list[tuple]:
    out = []
    for user, sql in ACCESS:
        _, rows, err = trino(sql, user)
        result = f"denied: {err.removeprefix('Access Denied: ')}" if err else f"ok {rows[0] if rows else ''}".strip()
        out.append((user, sql, result))
        print(f"{user:<11} {sql[:66]:<66} {result[:70]}")
    return out


def audit(since: str = "15m", user: str | None = None) -> list[dict]:
    """OPA's console decision log: one JSON line per decision, with the input Trino sent and the result. Prints a
    count per user, operation and answer, then every refusal in full (who, what, when, which decision)."""
    logs = subprocess.run(["docker", "compose", "logs", "--no-log-prefix", "--since", since, "opa"], cwd=ROOT,
                          capture_output=True, text=True).stdout  # fmt: skip
    decisions = [json.loads(line) for line in logs.splitlines() if '"decision_id"' in line]
    decisions = [d for d in decisions if not user or d["input"]["context"]["identity"]["user"] == user]
    by, denied = Counter(), []
    for d in decisions:
        op = d["input"]["action"].get("operation", "-")
        if d["path"].endswith("allow"):
            verdict = "allow" if d.get("result") else "DENY"
        else:  # batch filters, row filters and masks answer with lists, not yes/no
            verdict = f"{len(d.get('result') or [])} {d['path'].rsplit('/', 1)[-1]}"
            verdict = verdict.replace("batch", "allowed of batch")
        answer = verdict.split()[-1] if verdict[0].isdigit() else verdict
        by[(d["input"]["context"]["identity"]["user"], op, answer)] += 1
        if verdict == "DENY":
            denied.append(d)
    print(f"{'user':<11} {'operation':<22} {'answer':<12} decisions")
    for (u, op, v), n in sorted(by.items()):
        print(f"{u:<11} {op:<22} {v:<12} {n:>5}")
    for d in denied:
        res = d["input"]["action"].get("resource", {})
        what = next(iter(res.values()), {}) if res else {}
        name = ".".join(what[k] for k in ("name", "catalogName", "schemaName", "tableName") if what.get(k))
        print(f"DENY {d['timestamp'][:19]}Z {d['input']['context']['identity']['user']} "
              f"{d['input']['action']['operation']} {name or '-'} decision {d['decision_id'][:8]}")  # fmt: skip
    return decisions


def doors() -> dict[str, str]:
    """Masks live in Trino. What the same analyst reaches through other doors: Polaris, then the files."""
    out = {}
    sql = "SELECT anonymous_id, session_id FROM governed.gold.web_sessions WHERE converted ORDER BY started_at LIMIT 1"
    _, rows, _ = trino(sql, "analyst_gb")
    out["Trino as analyst_gb, gold.web_sessions"] = f"anonymous_id {rows[0][0]}, session_id {rows[0][1]}"
    cat = cat9.pyiceberg("analyst")
    scan = cat.load_table("gold.web_sessions").scan(
        row_filter="country == 'GB' and converted == true", selected_fields=("anonymous_id", "session_id", "started_at")
    )
    first = min(scan.to_arrow().to_pylist(), key=lambda r: r["started_at"])
    out["PyIceberg as analyst, gold.web_sessions"] = (
        f"anonymous_id {first['anonymous_id']}, session_id {first['session_id']}"
    )
    try:
        cat.load_table("customer.dim_customer")
        out["PyIceberg as analyst, customer.dim_customer"] = "ok"
    except Exception as e:  # noqa: BLE001 - the refusal is the result
        out["PyIceberg as analyst, customer.dim_customer"] = cat9.first_line(e)[:110]
    for k, v in out.items():
        print(f"{k:<44} {v}")
    return out


def topics(customer_id: str) -> int:
    """The CDC topic's log segments, decoded by Kafka's own dump tool (no broker needed): records naming the person."""
    script = ("for p in 0 1 2; do /opt/kafka/bin/kafka-dump-log.sh --print-data-log --files "
              "/k/larkspur.shop.orders-$p/00000000000000000000.log 2>/dev/null; done | grep -c "
              + customer_id)  # fmt: skip
    image = next(
        line.split("=", 1)[1] for line in (ROOT / ".env").read_text().splitlines() if line.startswith("KAFKA_IMAGE=")
    )
    run = subprocess.run(["docker", "run", "--rm", "--memory", "768m", "-v", "every-record-counts_kafka-data:/k:ro",
                          "--entrypoint", "/bin/bash", image, "-c", script],
                         capture_output=True, text=True)  # fmt: skip
    n = int(run.stdout.strip() or 0)
    print(f"larkspur.shop.orders (zstd-compressed segments, decoded): {n} records name {customer_id}")
    return n


# ---------- one person: where they are ----------


CASES = ROOT / "data/lab/part23/cases"  # the privacy team's case file (lab: a JSON file; deleted when the case closes)


def identifiers(customer_id: str) -> dict[str, str]:
    """Every identifier the platform holds for one customer, read from the CRM when the case opens and kept in the
    case file, so the proof after erasure still knows what to look for."""
    case = CASES / f"{customer_id}.json"
    if case.exists():
        return json.loads(case.read_text())
    sql = f"SELECT loyalty_id, email, phone FROM {PIPELINE}.raw.customers WHERE customer_id = '{customer_id}'"
    row = must(sql, "erasure")
    if not row:
        raise SystemExit(f"{customer_id} is not in the CRM: nothing links their identifiers")
    ids = {"customer_id": customer_id, "anonymous_id": f"a-{customer_id}"}
    ids |= dict(zip(("loyalty_id", "email", "phone"), row[0], strict=True))
    case.parent.mkdir(parents=True, exist_ok=True)
    case.write_text(json.dumps(ids, indent=1) + "\n")
    return ids


# Where each identifier can appear, and what erasure does there. `delete`: the record has no reason to stay.
# `tombstone`: the record is a financial transaction the law makes us keep, so only the identifier goes.
LOCATIONS = [
    ("raw.customers", "customer_id", "delete", PIPELINE),
    ("customer.dim_customer", "customer_id", "delete", PIPELINE),
    ("raw.orders", "customer_id", "tombstone", PIPELINE),
    ("raw.orders_changes", "customer_id", "tombstone", PIPELINE),
    ("raw.pos_uploads", "loyalty_id", "tombstone", PIPELINE),
    ("raw.pos_events", "loyalty_id", "tombstone", PIPELINE),
    ("raw.clickstream_events", "anonymous_id", "delete", PIPELINE),
    ("gold.web_sessions", "anonymous_id", "delete", PIPELINE),
    ("part06.clicks", "anonymous_id", "delete", "lakehouse"),  # a lab copy nobody owns: found by `discover`
]
PARTITION = {"raw.pos_uploads": "upload_date", "raw.clickstream_events": "event_date"}


def where(column: str, ids: dict) -> str:
    if column == "anonymous_id":  # signed-in events also carry user_id
        return f"(anonymous_id = '{ids['anonymous_id']}' OR user_id = '{ids['customer_id']}')"
    return f"{column} = '{ids[column]}'"


def has_column(catalog: str, table: str, column: str) -> bool:
    ns, name = table.split(".")
    return bool(must(f"SELECT 1 FROM {catalog}.information_schema.columns WHERE table_schema = '{ns}' "
                     f"AND table_name = '{name}' AND column_name = '{column}'", "erasure"))  # fmt: skip


def find(customer_id: str, quiet: bool = False) -> list[dict]:
    """Rows per location now, plus how many snapshots and refs each table keeps (old snapshots still hold rows)."""
    ids, out = identifiers(customer_id), []
    cat = cat9.pyiceberg("root", vended=False)
    for table, column, action, catalog in LOCATIONS:
        if not cat.table_exists(table):
            continue
        cond = where(column, ids) if has_column(catalog, table, "user_id") or column != "anonymous_id" else (
            f"anonymous_id = '{ids['anonymous_id']}'")  # fmt: skip
        n = must(f"SELECT count(*) FROM {catalog}.{table} WHERE {cond}", "erasure")[0][0]
        t = cat.load_table(table)
        refs = sorted(k for k in t.metadata.refs if k != "main")
        out.append({"table": table, "rows": n, "action": action, "snapshots": len(t.metadata.snapshots), "refs": refs})
    if not quiet:
        print(f"subject {customer_id}: " + ", ".join(f"{k}={v}" for k, v in ids.items() if k != "customer_id"))
        print(f"{'table':<26} {'rows':>5} {'snapshots':>9}  refs   action")
        for r in out:
            print(f"{r['table']:<26} {r['rows']:>5} {r['snapshots']:>9}  {','.join(r['refs']) or '-':<6} {r['action']}")
    return out


def pick_subject(before: str = "2026-08-01") -> str:
    """A UK customer with orders, loyalty baskets and clickstream events, none of them on or after `before` (the
    tests' subjects stay clear of the days other parts' controls compare), not erased yet."""
    sql = f"""
        SELECT c.customer_id FROM {PIPELINE}.raw.customers c
        WHERE c.country = 'GB'
          AND c.customer_id IN (SELECT customer_id FROM {PIPELINE}.raw.orders
                                GROUP BY 1 HAVING max(created_at) < TIMESTAMP '{before} 00:00 UTC')
          AND c.customer_id IN (SELECT user_id FROM {PIPELINE}.raw.clickstream_events WHERE user_id IS NOT NULL
                                GROUP BY 1 HAVING max(event_date) < DATE '{before}')
          AND c.loyalty_id IN (SELECT loyalty_id FROM {PIPELINE}.raw.pos_uploads WHERE loyalty_id IS NOT NULL
                               GROUP BY 1 HAVING max(upload_date) < DATE '{before}')
        ORDER BY c.customer_id LIMIT 1"""
    return must(sql, "erasure")[0][0]


# ---------- prove: decode every object that could hold the person, and look ----------


def needles(customer_id: str) -> list[bytes]:
    ids = identifiers(customer_id)
    # Not the phone: the lab's numbers come from a 1,000-number drama range, so many customers share one
    return [ids[k].encode() for k in ("customer_id", "loyalty_id", "email")]


def search(blob: bytes, key: str, terms: list[bytes]) -> bool:
    """True if any term is in the object's content, decoded by type: compressed bytes hide plain-text matches."""
    if key.endswith(".gz"):
        blob = gzip.decompress(blob)
    if key.endswith(".parquet"):
        t = pq.read_table(io.BytesIO(blob))
        for col in t.itercolumns():
            if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
                vals = pc.unique(col.drop_null()).to_pylist()
                if any(term.decode() in v for v in vals for term in terms):
                    return True
        return False
    if key.endswith(".avro"):  # manifests and manifest lists: column bounds are personal data too
        blob = "".join(repr(r) for r in fastavro.reader(io.BytesIO(blob))).encode()  # bytes print as b'CGB…'
    return any(term in blob for term in terms)


def prove(customer_id: str, label: str = "") -> dict:
    """Every object under the tables' locations (data, delete files, manifests, metadata) and the raw zone's files."""
    terms = needles(customer_id)
    fs = cat9.lake_fs()
    cat = cat9.pyiceberg("root", vended=False)
    prefixes = {}  # object prefix -> what to call it
    for table, *_ in LOCATIONS:
        if cat.table_exists(table):
            prefixes[cat.load_table(table).location().removeprefix("s3://") + "/"] = table
    for raw in ("pos/events", "clickstream/events", "oltp/orders/", "oltp/orders_poll"):
        prefixes[f"larkspur-raw/raw/{raw}"] = f"raw zone: {raw.rstrip('/')}"
    hits, scanned, nbytes = Counter(), 0, 0
    fs.invalidate_cache()  # s3fs caches listings; files deleted since the last call would still be listed
    for prefix, name in prefixes.items():
        for key in fs.find(prefix):
            blob = fs.cat_file(key)
            scanned, nbytes = scanned + 1, nbytes + len(blob)
            if search(blob, key, terms):
                kind = "metadata" if "/metadata/" in key else ("delete file" if "-deletes" in key else "data")
                hits[(name, kind)] += 1
    print(f"prove {label}: {scanned:,} objects ({nbytes / 2**20:,.0f} MiB) decoded and searched for "
          f"{len(terms)} identifiers")  # fmt: skip
    for (where_, kind), n in sorted(hits.items()):
        print(f"  {where_:<32} {kind:<11} {n:>3} objects")
    print(f"  objects still holding the subject: {sum(hits.values())}")
    return {"objects": scanned, "hits": sum(hits.values()), "by": {f"{w} {k}": n for (w, k), n in hits.items()}}


# ---------- erase: the request, end to end ----------


def subject_hash(customer_id: str) -> str:
    return hmac.new(REGISTER_KEY, customer_id.encode(), "sha256").hexdigest()


def erase(customer_id: str, request_id: str) -> dict:
    """Source first, then every lake location, then the files that still hold old versions, then the register."""
    ids, started = identifiers(customer_id), datetime.now(UTC)
    actions = []
    # 1. The sources: the CRM row goes; the order keeps its money and loses its customer (tax records)
    with psycopg.connect(PG, autocommit=True) as pg:
        n = pg.execute("DELETE FROM crm.customers WHERE customer_id = %s", (customer_id,)).rowcount
        actions.append(("postgres crm.customers", "delete", n))
        if pg.execute("SELECT to_regclass('shop.orders')").fetchone()[0]:
            n = pg.execute("UPDATE shop.orders SET customer_id = %s WHERE customer_id = %s", (TOMBSTONE, customer_id))
            actions.append(("postgres shop.orders", "tombstone", n.rowcount))
    # 2. Every lake location: a row-level DELETE or UPDATE (Trino writes delete files: the old rows still exist)
    sessions = {"rows": 0, "converted": 0}
    cat = cat9.pyiceberg("root", vended=False)
    days: dict[str, list] = {}
    for table, column, action, catalog in LOCATIONS:
        if not cat.table_exists(table):
            continue
        cond = where(column, ids) if column != "anonymous_id" or has_column(catalog, table, "user_id") else (
            f"anonymous_id = '{ids['anonymous_id']}'")  # fmt: skip
        if table in PARTITION:
            days[table] = [r[0] for r in must(f"SELECT DISTINCT CAST({PARTITION[table]} AS varchar) FROM "
                                              f"{catalog}.{table} WHERE {cond}", "erasure")]  # fmt: skip
        if table == "gold.web_sessions":
            sessions = dict(
                zip(
                    ("rows", "converted"),
                    must(f"SELECT count(*), count_if(converted) FROM {catalog}.{table} WHERE {cond}", "erasure")[0],
                    strict=True,
                )
            )
        sql = (f"DELETE FROM {catalog}.{table} WHERE {cond}" if action == "delete" else
               f"UPDATE {catalog}.{table} SET {column} = '{TOMBSTONE}' WHERE {cond}")  # fmt: skip
        n = must(sql, "erasure")[0][0]
        actions.append((table, action, n))
    # 3. The register and the suppression list, before anything can fail: what was done where, and a keyed hash of
    #    each identifier that every loader checks from now on
    register(request_id, customer_id, started, actions, sessions)
    write_suppression(request_id, ids)
    # 4. Rewrite the files those rows lived in, expire the snapshots that still point at the old files, then delete
    #    whatever no snapshot references. Retention 0 s: the lab's single writer is idle (production: hours, not 0).
    for table, _, _, catalog in LOCATIONS:
        part = days.get(table)
        if cat.table_exists(table) and (part is None or part):
            clean(table, catalog, part)
    # 5. The raw zone: the files as they landed. Rewrite the objects that hold the person; register those too.
    raw = rewrite_raw([ids[k].encode() for k in ("customer_id", "loyalty_id", "anonymous_id")])
    register(request_id, customer_id, started, raw, sessions)
    for loc, action, n in actions + raw:
        print(f"{loc:<34} {action:<10} {n:>5}")
    actions += raw
    return {"actions": actions, "sessions": sessions}


MAINTENANCE = {f"{c}.{p}": "0s" for c in (PIPELINE, "lakehouse") for p in (
    "expire_snapshots_min_retention", "remove_orphan_files_min_retention")}  # fmt: skip


def clean(table: str, catalog: str = PIPELINE, days: list[str] | None = None) -> None:
    """Rewrite the files (dropping deleted rows for good), expire every snapshot but the current one and those a tag
    or branch keeps, then delete files no snapshot references. Retention 0 s: the lab's only writer is idle."""
    cat = cat9.pyiceberg("root", vended=False)
    refs = sorted(k for k in cat.load_table(table).metadata.refs if k != "main")
    t = cat.load_table(table)
    identity = {t.schema().find_field(f.source_id).name for f in t.spec().fields if str(f.transform) == "identity"}
    if days and PARTITION[table] in identity:  # Trino's optimize takes a WHERE on identity partition columns only
        scope = f" WHERE {PARTITION[table]} IN ({', '.join(f'DATE {d!r}' for d in days)})"
    else:  # e.g. raw.pos_uploads as Part 17's `land` writes it (unpartitioned): rewrite every file
        scope = ""
    t0 = time.monotonic()
    alter = f"ALTER TABLE {catalog}.{table} EXECUTE "
    must(f"{alter}optimize(file_size_threshold => '1GB'){scope}", "erasure")
    must(f"{alter}expire_snapshots(retention_threshold => '0s')", "erasure", MAINTENANCE)
    # The current snapshot's manifests still list the files it replaced (status DELETED) with their column bounds,
    # and a file's lowest or highest value can be the person's ID. Rewriting the manifests drops those entries.
    must(f"{alter}optimize_manifests", "erasure")
    must(f"{alter}expire_snapshots(retention_threshold => '0s')", "erasure", MAINTENANCE)
    must(f"{alter}remove_orphan_files(retention_threshold => '0s')", "erasure", MAINTENANCE)
    left = len(cat.load_table(table).metadata.snapshots)
    print(f"  {table:<24} files and manifests rewritten, expired, orphans removed in {time.monotonic() - t0:4.1f} s; "
          f"{left} snapshot(s) left; refs: {', '.join(refs) or 'none'}")  # fmt: skip


def pin(table: str, tag: str) -> None:
    """A tag on the current snapshot, as finance might ask for at quarter close (PyIceberg: Trino 484 can't)."""
    t = cat9.pyiceberg("root", vended=False).load_table(table)
    t.manage_snapshots().create_tag(t.current_snapshot().snapshot_id, tag).commit()
    print(f"{table}: tag {tag} -> snapshot {t.current_snapshot().snapshot_id}")


def repoint(table: str, tag: str) -> None:
    """Move a tag to the current (erased) snapshot: the tag keeps its name, the old version stops being kept."""
    t = cat9.pyiceberg("root", vended=False).load_table(table)
    old = t.metadata.refs[tag].snapshot_id
    t.manage_snapshots().remove_tag(tag).create_tag(t.current_snapshot().snapshot_id, tag).commit()
    print(f"{table}: tag {tag} moved from snapshot {old} to {t.current_snapshot().snapshot_id}")


def weed(command: str) -> str:
    run = ["docker", "compose", "exec", "-T", "seaweedfs", "sh", "-c", f'printf "{command}\\n" | weed shell']
    return subprocess.run(run, cwd=ROOT, capture_output=True, text=True, check=True).stdout


def garbage() -> dict[str, tuple[int, int, float]]:
    """Per bucket: bytes on the volumes, bytes of deleted objects still on them, and the highest garbage share of any
    one volume (SeaweedFS compacts a volume by itself only once that share passes 30%, checked every 14 minutes)."""
    out: dict[str, list] = {}
    for m in re.finditer(r"Size:(\d+), .*?Collection:([\w-]*), .*?DeletedByteCount:(\d+)", weed("volume.list")):
        size, coll, deleted = int(m[1]), m[2] or "(seaweedfs)", int(m[3])
        acc = out.setdefault(coll, [0, 0, 0.0])
        acc[0], acc[1], acc[2] = acc[0] + size, acc[1] + deleted, max(acc[2], deleted / size if size else 0.0)
    return {k: (v[0], v[1], v[2]) for k, v in sorted(out.items())}


def vacuum(threshold: float = 0.001) -> None:
    """Compact the buckets' volumes now, so deleted objects' bytes leave the disk."""
    before = garbage()
    weed(f"lock\\nvolume.vacuum -collection larkspur-raw,larkspur-lake -garbageThreshold {threshold}\\nunlock")
    for _ in range(24):  # volume servers report the compacted sizes with their next heartbeats
        time.sleep(5)
        after = garbage()
        if all(after[c][1] < threshold * after[c][0] for c in ("larkspur-raw", "larkspur-lake")):
            break
    head = f"{'bucket':<14} {'volumes GiB':>11} {'deleted, on disk GiB':>21} {'worst volume':>13}"
    print(head + "   after vacuum: volumes, deleted")
    for coll in ("larkspur-raw", "larkspur-lake"):
        (s0, d0, r0), (s1, d1, _) = before[coll], after[coll]
        print(
            f"{coll:<14} {s0 / 2**30:>11.2f} {d0 / 2**30:>21.2f} {r0:>12.0%}   {s1 / 2**30:>13.2f} {d1 / 2**30:>8.2f}"
        )


def untag(table: str, tag: str) -> None:
    """Drop a tag (policy: no long-lived tags on tables classified personal)."""
    t = cat9.pyiceberg("root", vended=False).load_table(table)
    t.manage_snapshots().remove_tag(tag).commit()
    print(f"{table}: tag {tag} dropped")


RAW_TARGETS = [("larkspur-raw/raw/clickstream/events", "delete"), ("larkspur-raw/raw/pos/events", "tombstone"),
               ("larkspur-raw/raw/oltp/orders", "tombstone"),
               ("larkspur-raw/raw/oltp/orders_poll", "tombstone")]  # fmt: skip


def rewrite_raw(terms: list[bytes] | None = None) -> list[tuple]:
    """JSON Lines and Parquet in larkspur-raw: drop suppressed people's events, tombstone their identifiers in
    transactions; a file left empty is deleted. With `terms` (an erasure), only files containing them are read; without
    (after a re-upload of the raw zone), every file is checked against the suppression list."""
    fs = cat9.lake_fs()
    out = Counter()
    fs.invalidate_cache()
    for prefix, action in RAW_TARGETS:
        for key in fs.find(prefix):
            blob = fs.cat_file(key)
            if terms is not None and not search(blob, key, terms):
                continue
            where = prefix.removeprefix("larkspur-raw/")
            if key.endswith(".jsonl.gz"):
                lines = gzip.decompress(blob).decode().splitlines()
                kept, changed = [], 0
                for line in lines:
                    r = json.loads(line)
                    if is_suppressed("anonymous_id", r.get("anonymous_id")) or (
                        "anonymous_id" in r and is_suppressed("customer_id", r.get("user_id"))
                    ):
                        continue
                    if is_suppressed("loyalty_id", r.get("loyalty_id")):
                        r["loyalty_id"], changed = TOMBSTONE, changed + 1
                        line = json.dumps(r, separators=(",", ":"))
                    kept.append(line)
                if len(kept) == len(lines) and not changed:
                    continue
                if kept:
                    fs.pipe_file(key, gzip.compress(("\n".join(kept) + "\n").encode(), compresslevel=6, mtime=0))
                else:
                    fs.rm_file(key)
                out[(where, action)] += len(lines) - len(kept) if action == "delete" else 1
            else:
                t = pq.read_table(io.BytesIO(blob))
                fixed, n = tombstone_column(t.column("customer_id"), "customer_id")
                if not n:
                    continue
                sink = io.BytesIO()
                pq.write_table(t.set_column(t.schema.get_field_index("customer_id"), "customer_id", fixed), sink)
                fs.pipe_file(key, sink.getvalue())
                out[(where, action)] += 1
    return [(f"{p} (files)" if a == "tombstone" else f"{p} (events)", a, n) for (p, a), n in sorted(out.items())]


# ---------- suppression: what every loader checks before landing personal data ----------

SUPPRESSION = "customer.erasure_suppression"  # one keyed hash per identifier of every erased person
_suppressed: dict[str, set[str]] | None = None
_seen: dict[tuple[str, str], bool] = {}


def keyed(value: str) -> str:
    return hmac.new(REGISTER_KEY, value.encode(), "sha256").hexdigest()


def suppression_list(refresh: bool = False) -> dict[str, set[str]]:
    global _suppressed
    if _suppressed is None or refresh:
        _seen.clear()
        cat = cat9.pyiceberg("pipeline")
        _suppressed = {}
        if cat.table_exists(SUPPRESSION):
            for r in (
                cat.load_table(SUPPRESSION).scan(selected_fields=("kind", "identifier_hmac")).to_arrow().to_pylist()
            ):
                _suppressed.setdefault(r["kind"], set()).add(r["identifier_hmac"])
    return _suppressed


def is_suppressed(kind: str, value: str | None) -> bool:
    if not value:
        return False
    hit = _seen.get((kind, value))
    if hit is None:
        hit = _seen[(kind, value)] = keyed(value) in suppression_list().get(kind, set())
    return hit


def matching(col, kind: str) -> pa.Array:
    """The distinct values of an Arrow column that belong to suppressed people (computed per load, never stored)."""
    if not suppression_list().get(kind):
        return pa.array([], pa.string())
    values = pc.unique(col.drop_null() if hasattr(col, "drop_null") else col).to_pylist()
    return pa.array([v for v in values if is_suppressed(kind, v)], pa.string())


def tombstone_column(col, kind: str):
    hits = matching(col, kind)
    if not len(hits):
        return col, 0
    mask = pc.is_in(col, value_set=hits)
    return pc.if_else(mask, TOMBSTONE, col), pc.sum(mask).as_py() or 0


def suppress(data: pa.Table, delete: dict[str, str] | None = None, tombstone: dict[str, str] | None = None) -> pa.Table:
    """Apply the suppression list to rows about to be landed: drop rows whose `delete` columns name an erased person,
    replace `tombstone` columns' identifiers with 'erased'. Columns map to identifier kinds."""
    dropped = changed = 0
    for col, kind in (delete or {}).items():
        hits = matching(data.column(col), kind)
        if len(hits):
            keep = pc.invert(pc.fill_null(pc.is_in(data.column(col), value_set=hits), False))
            dropped += data.num_rows - (pc.sum(keep).as_py() or 0)
            data = data.filter(keep)
    for col, kind in (tombstone or {}).items():
        fixed, n = tombstone_column(data.column(col), kind)
        if n:
            data = data.set_column(data.schema.get_field_index(col), col, fixed)
            changed += n
    if dropped or changed:
        print(f"suppression list: {dropped} rows dropped, {changed} identifiers tombstoned")
    return data


def write_suppression(request_id: str, ids: dict) -> None:
    rows = [{"request_id": request_id, "kind": k, "identifier_hmac": keyed(ids[k])}
            for k in ("customer_id", "loyalty_id", "anonymous_id", "email")]  # fmt: skip
    data = pa.Table.from_pylist(rows, schema=pa.schema([("request_id", pa.string()), ("kind", pa.string()),
                                                        ("identifier_hmac", pa.string())]))  # fmt: skip
    cat = cat9.pyiceberg("pipeline")
    t = cat.load_table(SUPPRESSION) if cat.table_exists(SUPPRESSION) else cat.create_table(SUPPRESSION, data.schema)
    t.append(data)
    suppression_list(refresh=True)


REGISTER_SCHEMA = pa.schema([
    ("request_id", pa.string()), ("subject_hmac", pa.string()), ("received_at", pa.timestamp("us", "UTC")),
    ("completed_at", pa.timestamp("us", "UTC")), ("location", pa.string()), ("action", pa.string()),
    ("rows_affected", pa.int64()), ("converted_sessions", pa.int64())])  # fmt: skip


def register_table():
    cat = cat9.pyiceberg("pipeline")
    return (
        cat.load_table(REGISTER) if cat.table_exists(REGISTER) else cat.create_table(REGISTER, schema=REGISTER_SCHEMA)
    )


def register(request_id: str, customer_id: str, started: datetime, actions: list[tuple], sessions: dict) -> None:
    rows = [
        {"request_id": request_id, "subject_hmac": subject_hash(customer_id), "received_at": started,
         "completed_at": datetime.now(UTC), "location": loc, "action": action, "rows_affected": n,
         "converted_sessions": sessions["converted"] if loc == "gold.web_sessions" else None}
        for loc, action, n in actions
    ]  # fmt: skip
    data = pa.Table.from_pylist(rows, schema=REGISTER_SCHEMA)
    t = register_table()
    t.append(data)


def restore_lab(keep: str) -> dict:
    """Lab hygiene, not a production step: forget every erasure except request `keep` (earlier test runs used real
    generated customers), rebuild the suppression list from `keep`'s case file, and give tombstoned orders back their
    customer IDs from the generator's extracts, except the kept subjects'. Re-land the other tables afterwards with
    their own loaders (they apply the suppression list); the post's Part 23 section lists them."""
    reg = register_table()
    kept = reg.scan(row_filter=f"request_id == '{keep}'").to_arrow()
    subjects = set(kept["subject_hmac"].to_pylist())
    reg.delete(f"request_id != '{keep}'")
    cat = cat9.pyiceberg("pipeline")
    if cat.table_exists(SUPPRESSION):
        cat.load_table(SUPPRESSION).delete("request_id != ''")
    for case in sorted(CASES.glob("*.json")):
        ids = json.loads(case.read_text())
        if subject_hash(ids["customer_id"]) in subjects:
            write_suppression(keep, ids)
        else:
            case.unlink()
    con = duckdb.connect()
    files = [f"{ROOT}/data/raw/oltp/{d}/*/*.parquet" for d in ("orders", "orders_poll")]
    owners = con.sql(f"SELECT DISTINCT order_id, customer_id FROM read_parquet({files})").to_arrow_table()
    owner = dict(zip(owners["order_id"].to_pylist(), owners["customer_id"].to_pylist(), strict=True))
    out = {"register rows kept": kept.num_rows}
    for table in ("raw.orders", "raw.orders_changes"):
        ids = [r[0] for r in must(f"SELECT DISTINCT order_id FROM {PIPELINE}.{table} WHERE customer_id = '{TOMBSTONE}'",
                                  "erasure")]  # fmt: skip
        back = {o: owner[o] for o in ids if o in owner and not is_suppressed("customer_id", owner[o])}
        if back:
            cases = " ".join(f"WHEN '{o}' THEN '{c}'" for o, c in back.items())
            must(f"UPDATE {PIPELINE}.{table} SET customer_id = CASE order_id {cases} END "
                 f"WHERE order_id IN ({', '.join(repr(o) for o in back)})", "erasure")  # fmt: skip
        out[table] = f"{len(back)} orders restored, {len(ids) - len(back)} kept tombstoned"
    with psycopg.connect(PG, autocommit=True) as pg:
        if pg.execute("SELECT to_regclass('shop.orders')").fetchone()[0]:
            ids = [r[0] for r in pg.execute("SELECT order_id FROM shop.orders WHERE customer_id = %s", (TOMBSTONE,))]
            back = {o: owner[o] for o in ids if o in owner and not is_suppressed("customer_id", owner[o])}
            for o, c in back.items():
                pg.execute("UPDATE shop.orders SET customer_id = %s WHERE order_id = %s", (c, o))
            out["postgres shop.orders"] = f"{len(back)} orders restored, {len(ids) - len(back)} kept tombstoned"
    for k, v in out.items():
        print(f"{k:<24} {v}")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("setup", "drops", "customers", "discover", "tag", "policy-data", "access", "doors", "vacuum"):
        sub.add_parser(name)
    a = sub.add_parser("as")
    a.add_argument("user")
    a.add_argument("sql")
    a = sub.add_parser("audit")
    a.add_argument("--since", default="15m")
    a.add_argument("--user")
    for name in ("pin", "repoint", "untag"):
        a = sub.add_parser(name)
        a.add_argument("table")
        a.add_argument("tag")
    a = sub.add_parser("restore-lab")
    a.add_argument("--keep", required=True)
    a = sub.add_parser("clean")
    a.add_argument("table")
    for name in ("find", "prove", "erase", "topics"):
        a = sub.add_parser(name)
        a.add_argument("customer_id")
        a.add_argument("--label", default="")
        a.add_argument("--request", default=f"ER-{date.today():%Y%m%d}-001")
    args = p.parse_args()
    if args.cmd == "as":
        show(args.user, args.sql)
    elif args.cmd == "audit":
        audit(args.since, args.user)
    elif args.cmd in ("find", "prove"):
        (find if args.cmd == "find" else lambda c: prove(c, args.label))(args.customer_id)
    elif args.cmd in ("pin", "repoint", "untag"):
        {"pin": pin, "repoint": repoint, "untag": untag}[args.cmd](args.table, args.tag)
    elif args.cmd == "restore-lab":
        restore_lab(args.keep)
    elif args.cmd == "clean":
        clean(args.table, next((c for t, _, _, c in LOCATIONS if t == args.table), PIPELINE))
    elif args.cmd == "topics":
        topics(args.customer_id)
    elif args.cmd == "erase":
        erase(args.customer_id, args.request)
    else:
        {"setup": setup, "drops": drops, "customers": customers, "discover": discover, "tag": tag,
         "policy-data": policy_data, "access": access, "doors": doors, "vacuum": vacuum}[args.cmd]()  # fmt: skip


if __name__ == "__main__":
    sys.exit(main())
