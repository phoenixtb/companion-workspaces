"""Part 31: the last mile. Net revenue, stock availability and conversion defined once in Cube (cube/model), served
over Cube's REST API and its PostgreSQL-wire SQL API, and charted in Superset through that SQL API.

uv run python -m scripts.part31_serve opinions     # ad-hoc SQL, the incident's old mistakes: several answers each
uv run python -m scripts.part31_serve token --scope GB   # a signed token, e.g. for curl
uv run python -m scripts.part31_serve metrics      # the three metrics through Cube's REST API
uv run python -m scripts.part31_serve sql          # the generated SQL Cube sends to Trino for net revenue
uv run python -m scripts.part31_serve check        # Cube vs the board pack and vs dbt's report table, day by day
uv run python -m scripts.part31_serve scopes       # one token per scope: which Trino user, which rows (OPA)
uv run python -m scripts.part31_serve cache        # cold vs warm latency, Cube's in-memory result cache
uv run python -m scripts.part31_serve sql-api      # the same metrics through the SQL API (PostgreSQL protocol)
uv run python -m scripts.part31_serve superset     # a database, three datasets and three charts via Superset's API
uv run python -m scripts.part31_serve timezone     # a business date queried in a time zone west of UTC
uv run python -m scripts.part31_serve limit        # Cube's default row limit on a detailed query
uv run python -m scripts.part31_serve meta         # what a client learns about each metric: /v1/meta
uv run python -m scripts.part31_serve polls        # one query: through Cube's driver vs polled at once

Needs `make up-serve` (core, Trino + OPA, Cube, Superset). Tokens are signed with the lab's CUBE_API_SECRET (.env).
"""

import argparse
import base64
import hashlib
import hmac
import http.cookiejar
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg

from scripts import part23_privacy as lab23

ROOT = Path(__file__).resolve().parents[1]
CUBE = "http://localhost:4000/cubejs-api/v1"
SUPERSET = "http://localhost:8088"
OPINIONS_SQL = ROOT / "sql/part31/two_opinions.sql"
BOARD_PACK = ROOT / "governance/board-pack-2026-q3.json"
Q3 = ["2026-07-01", "2026-09-30"]


def env(key: str) -> str:
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise SystemExit(f"{key} missing from .env: make env")


# ---------- Cube's REST API ----------


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def token(scope: str = "all", ttl: int = 3600) -> str:
    """An HS256 JSON Web Token, as the app's backend would sign it: the payload becomes Cube's security context."""
    now = int(time.time())
    head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = b64(json.dumps({"scope": scope, "iat": now, "exp": now + ttl}).encode())
    sig = hmac.new(env("CUBE_API_SECRET").encode(), f"{head}.{body}".encode(), hashlib.sha256).digest()
    return f"{head}.{body}.{b64(sig)}"


def cube(path: str, body: dict | None = None, scope: str = "all") -> tuple[dict, int]:
    """One REST call; retries while Cube answers `Continue wait`. Returns (response, number of Continue waits)."""
    waits = 0
    while True:
        req = urllib.request.Request(
            f"{CUBE}/{path}",
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": token(scope), "Content-Type": "application/json"},
        )
        try:
            out = json.load(urllib.request.urlopen(req, timeout=120))
        except urllib.error.HTTPError as e:
            out = json.loads(e.read() or b"{}") | {"status": e.code}
        if out.get("error") == "Continue wait":
            waits += 1
            continue
        return out, waits


def load(query: dict, scope: str = "all", cache: str | None = None) -> tuple[list[dict], dict, int]:
    body = {"query": query} | ({"cache": cache} if cache else {})
    out, waits = cube("load", body, scope)
    if "error" in out:
        raise RuntimeError(f"Cube ({scope}): {out['error']}")
    return out["data"], out, waits


UK_Q3_REVENUE = {
    "measures": ["net_revenue.net_revenue", "net_revenue.sales", "net_revenue.refunds", "net_revenue.baskets"],
    "dimensions": ["net_revenue.country"],
    "filters": [{"member": "net_revenue.country", "operator": "equals", "values": ["GB"]}],
    "timeDimensions": [{"dimension": "net_revenue.business_date", "dateRange": Q3}],
}
Q3_STOCK = {
    "measures": ["stock_availability.availability", "stock_availability.in_stock_at_close",
                 "stock_availability.store_sku_days"],
    "timeDimensions": [{"dimension": "stock_availability.business_date", "dateRange": Q3}],
}  # fmt: skip
CONVERSION = {"measures": ["conversion.conversion_rate", "conversion.converted_sessions", "conversion.sessions"]}


def metrics(scope: str = "all", show: bool = True) -> dict:
    """The three numbers of the incident, as any client asks for them."""
    rev = load(UK_Q3_REVENUE, scope)[0][0]
    stock = load(Q3_STOCK, scope)[0][0]
    conv = load(CONVERSION, scope)[0][0]
    out = {
        "uk_net_revenue_q3": Decimal(rev["net_revenue.net_revenue"]),
        "uk_sales_q3": Decimal(rev["net_revenue.sales"]),
        "uk_refunds_q3": Decimal(rev["net_revenue.refunds"]),
        "uk_baskets_q3": int(rev["net_revenue.baskets"]),
        "stock_availability_q3": float(stock["stock_availability.availability"]),
        "store_sku_days_in_stock": int(stock["stock_availability.in_stock_at_close"]),
        "store_sku_days": int(stock["stock_availability.store_sku_days"]),
        "conversion_rate": float(conv["conversion.conversion_rate"]),
        "converted_sessions": int(conv["conversion.converted_sessions"]),
        "sessions": int(conv["conversion.sessions"]),
    }
    if show:
        print(f"scope {scope}: one definition each, through Cube's REST API")
        print(
            f"  UK net revenue, Q3 2026      GBP {out['uk_net_revenue_q3']:>16,}  "
            f"(sales {out['uk_sales_q3']:,}, refunds {out['uk_refunds_q3']:,}, {out['uk_baskets_q3']:,} baskets)"
        )
        print(
            f"  stock availability at close  {100 * out['stock_availability_q3']:.1f}%  "
            f"({out['store_sku_days_in_stock']:,} of {out['store_sku_days']:,} store-SKU-days)"
        )
        print(f"  conversion                   {100 * out['conversion_rate']:.2f}%  "
              f"({out['converted_sessions']:,} of {out['sessions']:,} sessions)")  # fmt: skip
    return out


def generated_sql(query: dict = UK_Q3_REVENUE) -> str:
    out, _ = cube("sql", {"query": query})
    sql, params = out["sql"]["sql"]
    print(sql)
    print("-- parameters:", params)
    return sql


# ---------- ad-hoc SQL: the incident's mistakes, still easy to make ----------


def statements(path: Path) -> list[tuple[str, str]]:
    """(label, SQL) per statement; a statement's label is the comment line just above it."""
    out = []
    for chunk in path.read_text().split(";"):
        lines = [x for x in chunk.strip().splitlines() if x.strip()]
        first = next((i for i, x in enumerate(lines) if not x.lstrip().startswith("--")), None)
        if first is None:
            continue
        out.append((lines[first - 1].lstrip("- ").strip(), "\n".join(lines[first:])))
    return out


def opinions() -> list[tuple[str, str, list]]:
    """Run every statement of sql/part31/two_opinions.sql as the user its label names."""
    found = []
    for label, sql in statements(OPINIONS_SQL):
        user = label.split("(as ", 1)[1].split(")")[0] if "(as " in label else "analyst_gb"
        cols, rows, err = lab23.trino(sql, user)
        if err:
            raise SystemExit(f"{label}: {err}")
        found.append((label, user, rows))
        print(f"-- {label}")
        for r in rows:
            print("   ", "  ".join(str(v) for v in r))
    return found


# ---------- checks ----------


def rpt_by_day() -> dict[tuple[str, str], Decimal]:
    rows = lab23.must(
        "SELECT country, CAST(business_date AS varchar), CAST(CAST(net_minor AS DECIMAL(18, 0)) * 0.01 AS varchar) "
        "FROM governed.gold.rpt_daily_net_revenue",
        "analyst_gb",
    )
    return {(c, d): Decimal(v) for c, d, v in rows}


def cube_by_day() -> dict[tuple[str, str], Decimal]:
    data, _, _ = load(
        {
            "measures": ["net_revenue.net_revenue"],
            "dimensions": ["net_revenue.country"],
            "timeDimensions": [{"dimension": "net_revenue.business_date", "granularity": "day"}],
            "limit": 50000,
        }
    )
    return {
        (r["net_revenue.country"], r["net_revenue.business_date.day"][:10]): Decimal(r["net_revenue.net_revenue"])
        for r in data
    }


def check() -> dict:
    pack = json.loads(BOARD_PACK.read_text())
    m = metrics(show=False)
    want_rev = Decimal(pack["uk_net_revenue_q3_minor"]) / 100
    rows = [
        ("UK net revenue Q3 = board pack", f"{m['uk_net_revenue_q3']:,}", m["uk_net_revenue_q3"] == want_rev),
        ("stock availability at close = board pack", f"{100 * m['stock_availability_q3']:.1f}%",
         round(100 * m["stock_availability_q3"], 1) == pack["stock_available_at_close_pct"]),
        ("conversion = board pack (sessions contract)", f"{m['converted_sessions']:,} / {m['sessions']:,} = "
         f"{100 * m['conversion_rate']:.2f}%", round(100 * m["conversion_rate"], 2) == pack["conversion_pct"]),
    ]  # fmt: skip
    rpt, cub = rpt_by_day(), cube_by_day()
    only_rpt = {k for k in rpt if k not in cub or cub[k] != rpt[k]}
    only_cube = {k for k in cub if k not in rpt or cub[k] != rpt[k]}
    rows.append(
        ("every country-day = dbt's gold.rpt_daily_net_revenue", f"{len(cub):,} country-days; differing: "
         f"{len(only_rpt)} / {len(only_cube)}", not only_rpt and not only_cube and len(cub) == len(rpt))
    )  # fmt: skip
    for name, found, ok in rows:
        print(f"{'pass' if ok else 'FAIL'}  {name:<52} {found}")
    return {"rows": rows, "country_days": len(cub), "differing": len(only_rpt) + len(only_cube)}


# ---------- scopes: one identity per token scope, OPA decides ----------


def trino_users_since(since: datetime) -> list[tuple[str, int]]:
    """Which Trino users ran Cube's queries (the source header Cube's client sends), from Trino's own query list."""
    rows = lab23.must(
        'SELECT "user", count(*) FROM system.runtime.queries '
        f"WHERE created >= from_iso8601_timestamp('{since.isoformat()}') AND source = 'nodejs-client' "
        "GROUP BY 1 ORDER BY 1"
    )
    return [(u, n) for u, n in rows]


def scopes() -> dict:
    since = datetime.now(UTC)
    out = {}
    query = {
        "measures": CONVERSION["measures"],
        "dimensions": ["conversion.country"],
        "order": {"conversion.country": "asc"},
    }
    for scope in ("all", "GB", "IN", "SG"):
        data, _, _ = load(query, scope)
        out[scope] = [(r["conversion.country"], int(r["conversion.sessions"]), int(r["conversion.converted_sessions"]))
                      for r in data]  # fmt: skip
        print(f"scope {scope:<3} -> " + "; ".join(f"{c} {s:,} sessions, {v:,} converted" for c, s, v in out[scope]))
    rev = {}
    for scope in ("all", "GB"):
        data, _, _ = load({"measures": ["net_revenue.net_revenue"], "dimensions": ["net_revenue.country"],
                           "timeDimensions": UK_Q3_REVENUE["timeDimensions"]}, scope)  # fmt: skip
        rev[scope] = sorted(r["net_revenue.country"] for r in data)
        print(f"scope {scope:<3} -> net revenue rows for {', '.join(rev[scope])} (gold revenue has no row scope)")
    try:
        load(CONVERSION, "FR")
        out["FR"] = "answered"
    except RuntimeError as e:
        out["FR"] = str(e)
    print(f"scope FR  -> {out['FR']}")
    users = trino_users_since(since)
    print("Trino users that ran Cube's queries:", ", ".join(f"{u} ({n})" for u, n in users))
    return {"sessions": out, "revenue_countries": rev, "users": users}


# ---------- cache ----------


def timed(query: dict, cache: str | None = None) -> tuple[float, int, dict]:
    t0 = time.perf_counter()
    _, raw, waits = load(query, cache=cache)
    return time.perf_counter() - t0, waits, raw


METRIC_QUERIES = (("UK net revenue Q3", UK_Q3_REVENUE), ("stock availability Q3", Q3_STOCK),
                  ("conversion", CONVERSION))  # fmt: skip


def cache_demo(key_seconds: int = 120) -> list[tuple]:
    """Cold, then warm; then after the refresh key's renewal threshold: Trino's default key is
    FLOOR(to_unixtime(NOW()) / 120), re-checked at most every 120 s. Run right after Cube starts (cold cache)."""
    since, t0 = datetime.now(UTC), time.time()
    cold = [timed(q) for _, q in METRIC_QUERIES]
    warm = [timed(q) for _, q in METRIC_QUERIES]
    time.sleep(max(0.0, t0 + key_seconds + 5 - time.time()))
    after = [timed(q) for _, q in METRIC_QUERIES]
    time.sleep(10)
    later = [timed(q) for _, q in METRIC_QUERIES]
    fresh = [timed(q, cache="no-cache") for _, q in METRIC_QUERIES]
    runs = lab23.must(
        "SELECT query LIKE '%refresh_key%', count(*), CAST(avg(date_diff('millisecond', created, \"end\")) AS bigint) "
        "FROM system.runtime.queries "
        f"WHERE created >= from_iso8601_timestamp('{since.isoformat()}') AND source = 'nodejs-client' GROUP BY 1"
    )
    out = []
    print(
        f"{'query':<22} {'cold':>7} {'warm':>7} {'key expired':>12} {'10 s later':>11} {'no-cache':>9}  "
        "last refresh (UTC)"
    )
    for i, (name, _) in enumerate(METRIC_QUERIES):
        refreshed = [x[i][2]["lastRefreshTime"] for x in (cold, after, later)]
        row = (name, cold[i][0], warm[i][0], after[i][0], later[i][0], fresh[i][0], cold[i][1], *refreshed)
        out.append(row)
        print(
            f"{name:<22} {row[1]:>6.2f}s {row[2]:>6.3f}s {row[3]:>11.3f}s {row[4]:>10.3f}s {row[5]:>8.2f}s  "
            f"{row[7][11:19]}, {row[8][11:19]}, {row[9][11:19]}"
        )
    for is_key, n, ms in sorted(runs):
        print(
            f"Trino statements from Cube, {'refresh-key checks' if is_key else 'metric queries':<18}: {n}, "
            f"{ms / 1000:.2f} s each on average (created to end)"
        )
    print(f"Continue waits: {sum(r[6] for r in out)}")
    return out


# ---------- the SQL API: Cube as a PostgreSQL server ----------

SQL_API = "host=localhost port=15432 dbname=cube user=superset password={}"
SQL_API_QUERIES = [
    ("net revenue by country, Q3",
     "SELECT country, MEASURE(net_revenue) AS net_revenue FROM net_revenue "
     "WHERE business_date >= '2026-07-01' AND business_date < '2026-10-01' GROUP BY 1 ORDER BY 1"),
    ("stock availability, Q3",
     "SELECT MEASURE(availability) AS availability FROM stock_availability "
     "WHERE business_date >= '2026-07-01' AND business_date < '2026-10-01'"),
    ("conversion", "SELECT MEASURE(conversion_rate) AS conversion_rate, MEASURE(sessions) AS sessions FROM conversion"),
]  # fmt: skip


def sql_api() -> dict:
    out = {}
    with psycopg.connect(SQL_API.format(env("CUBE_SQL_PASSWORD")), autocommit=True) as con:
        print("server_version:", con.execute("SELECT version()").fetchone()[0])
        tables = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1"
        print("tables:", ", ".join(r[0] for r in con.execute(tables).fetchall()))
        for name, sql in SQL_API_QUERIES:
            cur = con.execute(sql)
            rows = cur.fetchall()
            types = ", ".join(f"{d.name} {con.adapters.types.get(d.type_code).name}" for d in cur.description)
            out[name] = rows
            out[name + " types"] = types
            print(f"-- {name} ({types})\n   " + "\n   ".join("  ".join(str(v) for v in r) for r in rows))
    return out


# ---------- Superset: a database, datasets and charts, all through its REST API ----------


class Superset:
    """Session-based client: Superset's POSTs need a bearer token and a CSRF token tied to a session cookie."""

    def __init__(self) -> None:
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        login = {"username": "admin", "password": env("SUPERSET_ADMIN_PASSWORD"), "provider": "db", "refresh": True}
        self.token = self.call("POST", "/api/v1/security/login", login, auth=False)["access_token"]
        self.csrf = self.call("GET", "/api/v1/security/csrf_token/")["result"]

    def call(self, method: str, path: str, body: dict | None = None, auth: bool = True) -> dict:
        headers = {"Content-Type": "application/json", "Referer": SUPERSET}
        if auth:
            headers["Authorization"] = f"Bearer {self.token}"
            if method != "GET":
                headers["X-CSRFToken"] = self.csrf
        req = urllib.request.Request(
            SUPERSET + path, method=method, headers=headers, data=json.dumps(body).encode() if body else None
        )
        try:
            return json.load(self.opener.open(req, timeout=120))
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"Superset {method} {path}: {e.code} {e.read().decode()[:400]}") from None

    def find(self, kind: str, column: str, value: str) -> int | None:
        q = urllib.parse.quote(json.dumps({"filters": [{"col": column, "opr": "eq", "value": value}]}))
        found = self.call("GET", f"/api/v1/{kind}/?q={q}")["result"]
        return found[0]["id"] if found else None


DATABASE = "Larkspur metrics (Cube)"
# dataset (a Cube view, i.e. a table of the SQL API) -> the chart's metric (Cube measure via MEASURE()) and filter
CHARTS = {
    "net_revenue": ("UK net revenue, Q3 2026 (GBP)", "MEASURE(net_revenue)",
                    [{"col": "country", "op": "==", "val": "GB"}], "business_date"),
    "stock_availability": ("Stock availability at close, Q3 2026", "MEASURE(availability)", [], "business_date"),
    "conversion": ("Conversion (sessions contract)", "MEASURE(conversion_rate)", [], None),
}  # fmt: skip


def superset() -> dict:
    s = Superset()
    uri = f"postgresql+psycopg2://superset:{env('CUBE_SQL_PASSWORD')}@cube:15432/cube"
    db = s.find("database", "database_name", DATABASE)
    if db is None:
        db = s.call("POST", "/api/v1/database/", {"database_name": DATABASE, "sqlalchemy_uri": uri})["id"]
    print(f"database {db}: {DATABASE} -> postgresql://cube:15432 (Cube's SQL API)")
    out = {}
    for table, (title, metric, filters, time_col) in CHARTS.items():
        ds = s.find("dataset", "table_name", table)
        if ds is None:
            ds = s.call("POST", "/api/v1/dataset/", {"database": db, "schema": "public", "table_name": table})["id"]
        params = {"viz_type": "big_number_total", "datasource": f"{ds}__table",
                  "metric": {"expressionType": "SQL", "sqlExpression": metric, "label": table},
                  "adhoc_filters": [{"expressionType": "SIMPLE", "clause": "WHERE", "subject": f["col"],
                                     "operator": f["op"], "comparator": f["val"]} for f in filters]}  # fmt: skip
        if time_col:
            params["adhoc_filters"].append({"expressionType": "SIMPLE", "clause": "WHERE", "subject": time_col,
                                            "operator": "TEMPORAL_RANGE",
                                            "comparator": "2026-07-01 : 2026-10-01"})  # fmt: skip
        chart = s.find("chart", "slice_name", title)
        body = {"slice_name": title, "viz_type": "big_number_total", "datasource_id": ds,
                "datasource_type": "table", "params": json.dumps(params)}  # fmt: skip
        if chart is None:
            chart = s.call("POST", "/api/v1/chart/", body)["id"]
        else:
            s.call("PUT", f"/api/v1/chart/{chart}", body)
        query = {"filters": [{"col": f["col"], "op": f["op"], "val": f["val"]} for f in filters],
                 "metrics": [params["metric"]], "row_limit": 10}  # fmt: skip
        if time_col:
            query["filters"].append({"col": time_col, "op": "TEMPORAL_RANGE", "val": "2026-07-01 : 2026-10-01"})
        data = s.call("POST", "/api/v1/chart/data", {"datasource": {"id": ds, "type": "table"}, "queries": [query],
                                                     "result_format": "json", "result_type": "full"})  # fmt: skip
        result = data["result"][0]
        value = result["data"][0][table]
        out[table] = {"chart": chart, "dataset": ds, "value": value, "query": result.get("query", "")}
        print(f"chart {chart} '{title}' (dataset {ds} = view {table}): {value}")
    return out


def superset_sql(table: str = "net_revenue") -> str:
    """The SQL Superset sent to Cube's SQL API for one chart (from /api/v1/chart/data's `query`)."""
    q = superset()[table]["query"]
    print(q)
    return q


# ---------- business dates and time zones ----------


def timezone_demo() -> dict:
    """The same Q3 question with the query's time zone set: Cube converts time dimensions into it."""
    out = {}
    for tz in (None, "Europe/London", "America/New_York"):
        q = json.loads(json.dumps(UK_Q3_REVENUE)) | ({"timezone": tz} if tz else {})
        data, _, _ = load(q)
        out[tz or "UTC (default)"] = Decimal(data[0]["net_revenue.net_revenue"])
        print(f"timezone {tz or 'UTC (default)':<17} UK net revenue Q3 GBP {out[tz or 'UTC (default)']:>16,}")
    return out


def limit_demo() -> dict:
    """A detailed query (store x day) without a limit: Cube applies CUBEJS_DB_QUERY_DEFAULT_LIMIT (10,000)."""
    day = {"dimension": "net_revenue.business_date", "granularity": "day", "dateRange": Q3}
    q = {"measures": ["net_revenue.net_revenue"], "dimensions": ["net_revenue.store_id"], "timeDimensions": [day]}
    rows = len(load(q)[0])
    data, raw, _ = load(q | {"total": True})
    out = {"rows": rows, "total": raw.get("total"), "with_limit": len(load(q | {"limit": 50000})[0])}
    print(
        f"store x day, Q3, no limit: {rows:,} rows; total: true says {out['total']:,}; limit 50000: "
        f"{out['with_limit']:,} rows"
    )
    return out


def meta() -> list[dict]:
    out, _ = cube("meta")
    for c in out["cubes"]:
        print(f"{c['name']} ({c.get('type')}): {c.get('description')}")
        print(f"   meta: {json.dumps(c.get('meta'))}")
        print(
            f"   measures: {', '.join(m['name'].split('.')[1] for m in c['measures'])}; "
            f"dimensions: {', '.join(d['name'].split('.')[1] for d in c['dimensions'])}"
        )
    return out["cubes"]


def polls(runs: int = 3) -> list:
    """The conversion query (no parameters), run by Cube's Trino driver (cache: no-cache), which waits 800 ms
    between polls, and the same SQL run here with results fetched at once: Trino's created-to-end time for each."""
    out, _ = cube("sql", {"query": CONVERSION})
    sql = out["sql"]["sql"][0].replace(" gold.", " governed.gold.")  # Cube sets the session catalog; we name it
    for _ in range(runs):
        load(CONVERSION, cache="no-cache")
        lab23.must(sql, "cube")
    rows = lab23.must(
        "SELECT \"user\", coalesce(source, '(lab script)'), date_diff('millisecond', created, \"end\") "
        "FROM system.runtime.queries WHERE query LIKE 'SELECT 1e0 * count(CASE WHEN (\"web_sessions\".converted)%' "
        f"ORDER BY created DESC LIMIT {2 * runs}"
    )
    print(f"{'user':<6} {'client':<14} {'ms':>5}")
    for user, client, ms in rows:
        print(f"{user:<6} {client:<14} {ms:>5}")
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = ["token", "opinions", "metrics", "sql", "check", "scopes", "cache", "sql-api", "superset",
                "superset-sql", "timezone", "limit", "meta", "polls"]  # fmt: skip
    ap.add_argument("command", choices=commands)
    ap.add_argument("--scope", default="all")
    a = ap.parse_args()
    if a.command == "token":
        print(token(a.scope))
    elif a.command == "opinions":
        opinions()
    elif a.command == "metrics":
        metrics(a.scope)
    elif a.command == "sql":
        generated_sql()
    elif a.command == "check":
        sys.exit(0 if all(ok for _, _, ok in check()["rows"]) else 1)
    elif a.command == "scopes":
        scopes()
    elif a.command == "cache":
        cache_demo()
    elif a.command == "sql-api":
        sql_api()
    elif a.command == "superset":
        superset()
    elif a.command == "superset-sql":
        superset_sql()
    elif a.command == "limit":
        limit_demo()
    elif a.command == "meta":
        meta()
    elif a.command == "polls":
        polls()
    else:
        timezone_demo()


if __name__ == "__main__":
    main()
