"""Part 33: an MCP server that gives an agent Larkspur's context: the semantic layer (Cube, Part 31), the data
contracts (Part 20, contracts/*.odcs.yaml) and the catalog's classification (Part 23), plus a governed query tool.

Two modes, so the post can compare them on the same questions:
  governed  list_metrics, describe_metric, query_metric (+ recent_tickets for support roles). Every number comes from
            Cube, as a Trino identity OPA filters; contracts and the catalog decide what may be asked.
  raw       list_tables, describe_table, run_sql: the "text-to-SQL over the warehouse" design, same Trino identity.

The agent never chooses who it is: the role and the scope are fixed when the host starts the server
(`--role`), not tool arguments, so nothing the model reads can widen them.

uv run python -m scripts.part33_mcp --mode governed --role head_office    # stdio; the agent host starts it
uv run python -m scripts.part33_mcp --mode raw
"""

import argparse
import json
import os
import urllib.request
from decimal import Decimal
from pathlib import Path

import psycopg
import yaml
from mcp.server import MCPServer

from scripts import part31_serve as lab31

ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATION = ROOT / "opa/larkspur/classification/data.json"  # the catalog's labels, exported by Part 23

# Who the agent acts for. The scope picks the Cube token, so the Trino identity, so OPA's row filters (Part 31).
ROLES = {
    "head_office": {"scope": "all", "tools": {"metrics"}},
    "store_ops_gb": {"scope": "GB", "tools": {"metrics", "tickets"}},
}
# Each served view: its contract (if any) and the gold tables it reads (lineage; production asks the lineage
# backend, Part 22). The time dimension is the one a date range applies to.
VIEWS = {
    "net_revenue": {"contract": "contracts/net_revenue.odcs.yaml", "time": "business_date",
                    "sources": ["gold.fct_basket", "gold.fct_refund", "gold.dim_fx_rate", "gold.dim_date"]},
    "stock_availability": {"contract": None, "time": "business_date", "sources": ["gold.fct_stock_daily"]},
    "conversion": {"contract": "contracts/clickstream_sessions.odcs.yaml", "time": "started_at",
                   "sources": ["gold.web_sessions"]},
}  # fmt: skip
TICKETS_DSN = "host=localhost port=5432 dbname=larkspur user=agent_support_reader password=agent_support_reader"
RAW_IDENTITY = "cube"  # raw mode reads as the same Trino identity Cube uses: same rights, no semantic layer
LAB_MISTAKES = os.environ.get("LAB_MISTAKES", "").split(",")  # the post's "before" runs: no-scope-note


class Refused(Exception):
    """A request the governed layer won't serve; the message says which rule and where it is written."""


def contract(view: str) -> dict | None:
    path = VIEWS[view]["contract"]
    return yaml.safe_load((ROOT / path).read_text()) if path else None


def personal_columns(view: str) -> dict[str, str]:
    labels = json.loads(CLASSIFICATION.read_text())
    out = {}
    for table in VIEWS[view]["sources"]:
        for col, cls in labels.get(table, {}).get("columns", {}).items():
            out[col] = f"{table}.{col} is classified '{cls}' (personal data) in the catalog"
    return out


def row_scope(view: str) -> str | None:
    """The column the governed engine filters this view's rows by, if the catalog says it does (Part 23)."""
    labels = json.loads(CLASSIFICATION.read_text())
    return next((labels[t]["row-scope"] for t in VIEWS[view]["sources"] if "row-scope" in labels.get(t, {})), None)


def meta() -> dict[str, dict]:
    """Cube's /v1/meta: each public view with its members, descriptions and meta."""
    out, _ = lab31.cube("meta")
    return {c["name"]: c for c in out["cubes"] if c.get("type") == "view" and c["name"] in VIEWS}


def fmt(value, member: dict) -> str:
    """Numbers are formatted here, by the layer, so the model copies them instead of doing arithmetic."""
    if value is None:
        return "null"
    if member.get("format") == "currency":
        return f"£{Decimal(value):,.2f}"
    if member.get("format") == "percent":
        return f"{100 * float(value):.2f}%"
    if member.get("aggType") == "count":
        return f"{int(value):,}"
    return str(value)


# ---------- governed tools ----------


def list_metrics() -> list[dict]:
    """Every metric (Cube view) this agent may use, with its description, owner and freshness promise."""
    return [
        {"metric": name, "description": v.get("description"), "owner": (v.get("meta") or {}).get("owner"),
         "freshness": (v.get("meta") or {}).get("freshness"), "contract": VIEWS[name]["contract"]}
        for name, v in meta().items()
    ]  # fmt: skip


def values(metric: str, view: dict, scope: str, only: set[str] | None = None) -> dict[str, list[str]]:
    """The values of each small string dimension, as this agent's scope sees them: filters must use these."""
    out = {}
    for d in view["dimensions"]:
        name = d["name"].split(".")[1]
        if d["type"] == "string" and name not in ("store_id", "city") and (only is None or name in only):
            data, _, _ = lab31.load({"dimensions": [d["name"]], "limit": 31}, scope)
            found = sorted(str(r[d["name"]]) for r in data)
            if len(found) <= 30:
                out[name] = found
    return out


def present(metric: str, name: str, vals: list[str], scope: str) -> set[str]:
    """Which of these values of one dimension this scope can see (Cube runs it as the scope's Trino identity)."""
    member = f"{metric}.{name}"
    flt = [{"member": member, "operator": "equals", "values": vals}]
    data, _, _ = lab31.load({"dimensions": [member], "filters": flt, "limit": 500}, scope)
    return {str(r[member]) for r in data}


def describe_metric(metric: str, role: str) -> dict:
    """Measures and dimensions of one metric with their definitions, its contract's terms and who may read it."""
    views = meta()
    if metric not in views:
        raise Refused(f"unknown metric {metric!r}; metrics: {', '.join(views)}")
    v, c, scope = views[metric], contract(metric), ROLES[role]["scope"]
    out = {
        "metric": metric,
        "description": v.get("description"),
        "meta": v.get("meta"),
        "measures": {m["name"].split(".")[1]: m.get("description") for m in v["measures"]},
        "dimensions": {d["name"].split(".")[1]: d.get("description") for d in v["dimensions"]},
        "values": values(metric, v, scope),
        "date_range_applies_to": VIEWS[metric]["time"],
        "built_from": VIEWS[metric]["sources"],
        "personal_data_not_served": sorted(personal_columns(metric)),
    }
    if c:
        out["contract"] = {
            "id": c["id"], "version": c["version"], **{k: c["description"].get(k) for k in ("purpose", "usage",
                                                                                             "limitations")},
            "readable_by_roles": [r["role"] for r in c.get("roles", [])] or "any",
            "sla": [f"{s['property']}: {s['value']} {s.get('unit', '')}".strip() for s in c.get("slaProperties", [])],
        }  # fmt: skip
    return out


def check_role(metric: str, role: str) -> None:
    c = contract(metric)
    allowed = [r["role"] for r in (c or {}).get("roles", [])]
    if allowed and role not in allowed:
        raise Refused(
            f"the {c['name']} contract ({VIEWS[metric]['contract']}, version {c['version']}) grants read to "
            f"{', '.join(allowed)} only; this agent runs as {role}. Limitation: {c['description']['limitations']}"
        )


def query_metric(
    role: str,
    metric: str,
    measures: list[str],
    dimensions: list[str] | None = None,
    filters: dict[str, list[str]] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    granularity: str | None = None,
) -> dict:
    """Run one governed query through Cube, as this agent's scope; refuse what a contract or the catalog forbids."""
    dimensions, filters = dimensions or [], filters or {}
    views = meta()
    if metric not in views:
        raise Refused(f"unknown metric {metric!r}; metrics: {', '.join(views)}")
    check_role(metric, role)
    v = views[metric]
    members = {m["name"].split(".")[1]: m for m in v["measures"] + v["dimensions"]}
    personal = personal_columns(metric)
    for name in [*measures, *dimensions, *filters]:
        if name in personal:
            raise Refused(f"{personal[name]}; the {metric} metric serves aggregates only")
        if name not in members:
            raise Refused(f"{name!r} is not part of the {metric} metric; members: {', '.join(members)}")
    scope, scoped_by = ROLES[role]["scope"], row_scope(metric)
    visible = values(metric, v, scope, set(filters) | {scoped_by})  # what the governed engine lets this identity see
    for name, vals in filters.items():  # every filter value must exist for this identity: an empty slice isn't 0
        vals = [str(x) for x in vals]
        found = present(metric, name, vals, scope)  # one Cube query per filter, as this scope
        hidden = [x for x in vals if x not in found]
        if hidden:
            seen = f"; it sees {', '.join(visible[name]) or 'none'}" if name in visible else ""
            raise Refused(
                f"{name} {', '.join(hidden)}: no such value visible to this agent (scope {scope}{seen}). Either it "
                "doesn't exist or the governed engine (OPA) filters those rows for this identity; any figure for it "
                "would be wrong, not zero."
            )
    query = {
        "measures": [f"{metric}.{m}" for m in measures],
        "dimensions": [f"{metric}.{d}" for d in dimensions],
        "filters": [{"member": f"{metric}.{k}", "operator": "equals", "values": [str(x) for x in vals]}
                    for k, vals in filters.items()],
        "limit": 500,
    }  # fmt: skip
    if date_from or date_to or granularity:
        td = {"dimension": f"{metric}.{VIEWS[metric]['time']}"}
        if date_from or date_to:
            td["dateRange"] = [date_from or "2026-01-01", date_to or date_from]
        if granularity:
            td["granularity"] = granularity
        query["timeDimensions"] = [td]
    data, out, _ = lab31.load(query, scope)
    rows = [{k.split(".", 1)[1].split(".")[0]: fmt(val, members.get(k.split(".", 1)[1].split(".")[0], {}))
             for k, val in r.items()} for r in data]  # fmt: skip
    result = {
        "rows": rows,
        "definitions": {m: members[m].get("description") for m in measures},
        "freshness": (v.get("meta") or {}).get("freshness"),
        "last_refresh": out.get("lastRefreshTime"),
        "scope": scope,
    }
    # say what the rows do NOT cover; a model will otherwise label them as it likes
    if scoped_by in visible and "no-scope-note" not in LAB_MISTAKES:
        result["rows_limited_to"] = (
            f"{scoped_by} {', '.join(visible[scoped_by])} only: rows of any other {scoped_by} are hidden from this "
            f"agent and are not in this result. Never report these rows as another {scoped_by}'s."
        )
    # no rows, or one row of NULL ratios and 0 counts from zero rows (e.g. values that exist only apart, or no dates)
    if all(r.get(m) in ("null", "0", None) for r in rows for m in measures):
        result["note"] = f"no data for this query within this agent's scope ({scope}); this is not a zero"
    return result


def recent_tickets(limit: int = 3) -> list[dict]:
    """The most recent support tickets of the agent's country: subject and thread, personal data redacted."""
    with psycopg.connect(TICKETS_DSN) as con:
        rows = con.execute(
            "SELECT ticket_id, subject, body, status, created_at FROM agent.support_tickets "
            "ORDER BY created_at DESC LIMIT %s",
            (min(int(limit), 10),),
        ).fetchall()
    return [{"ticket_id": t, "subject": s, "body": b, "status": st, "created_at": c.isoformat()}
            for t, s, b, st, c in rows]  # fmt: skip


# ---------- raw tools: the warehouse without the context layer ----------


def gold_tables() -> list[str]:
    return sorted(t.split(".")[1] for t in json.loads(CLASSIFICATION.read_text()) if t.startswith("gold."))


def describe_table(table: str) -> dict:
    """Trino reports column names and types with a zero-row result; analysts can't read information_schema."""
    if table.removeprefix("gold.") not in gold_tables():
        raise Refused(f"unknown table {table!r}")
    name = table.removeprefix("gold.")
    return {"table": f"gold.{name}", "columns": trino_types(f"SELECT * FROM governed.gold.{name} LIMIT 0")}


def trino(sql: str) -> tuple[list[dict], list, str | None]:
    """One statement as RAW_IDENTITY with governed.gold as the default catalog and schema."""
    headers = {"X-Trino-User": RAW_IDENTITY, "X-Trino-Catalog": "governed", "X-Trino-Schema": "gold"}
    req = urllib.request.Request("http://localhost:8080/v1/statement", data=sql.encode(), headers=headers)
    page, cols, rows = json.load(urllib.request.urlopen(req)), [], []
    while True:
        cols = cols or page.get("columns", [])
        rows += page.get("data", [])
        if "error" in page:
            return cols, [], page["error"]["message"]
        if "nextUri" not in page:
            return cols, rows, None
        page = json.load(urllib.request.urlopen(page["nextUri"]))


def trino_types(sql: str) -> dict[str, str]:
    cols, _, _ = trino(sql)
    return {c["name"]: c["type"] for c in cols}


def run_sql(sql: str) -> dict:
    sql = sql.strip().rstrip(";")
    if not sql.lower().startswith(("select", "with")):
        raise Refused("read-only: SELECT or WITH statements only")
    cols, rows, err = trino(sql)
    if err:
        return {"error": err}
    return {"columns": [c["name"] for c in cols], "rows": rows[:50], "row_count": len(rows)}


# ---------- the server ----------


def build(mode: str, role: str) -> MCPServer:
    """The tools a host gets for this mode and role. A refusal comes back as {"refused": reason}, not an error."""
    server = MCPServer(f"larkspur-{mode}")

    def safe(fn, *args):
        try:
            return fn(*args)
        except Refused as e:
            return {"refused": str(e)}

    if mode == "raw":

        @server.tool(name="list_tables")
        def _list_tables() -> list[str]:
            """The tables in the warehouse's gold schema (Trino catalog governed)."""
            return gold_tables()

        @server.tool(name="describe_table")
        def _describe_table(table: str) -> dict:
            """Column names and types of one gold table."""
            return safe(describe_table, table)

        @server.tool(name="run_sql")
        def _run_sql(sql: str) -> dict:
            """Run one read-only Trino SQL statement (default catalog governed, schema gold). At most 50 rows return."""
            return safe(run_sql, sql)

        return server

    @server.tool(name="list_metrics")
    def _list_metrics() -> list[dict]:
        """Every metric (a governed definition) this agent may use, with description, owner and freshness."""
        return list_metrics()

    @server.tool(name="describe_metric")
    def _describe_metric(metric: str) -> dict:
        """One metric's measures and dimensions with their definitions, its contract and who may read it."""
        return safe(describe_metric, metric, role)

    @server.tool(name="query_metric")
    def _query_metric(
        metric: str,
        measures: list[str],
        dimensions: list[str] | None = None,
        filters: dict[str, list[str]] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        granularity: str | None = None,
    ) -> dict:
        """Query a metric. measures/dimensions: member names from describe_metric. filters: {dimension: [values]}
        (equality). date_from/date_to: YYYY-MM-DD, inclusive, on the metric's date. granularity: day, month or
        quarter, or omit it for one period. Omit dimensions for one total; group by a dimension only when asked for a
        breakdown. Values come back formatted; quote them as given."""
        return safe(query_metric, role, metric, measures, dimensions, filters, date_from, date_to, granularity)

    if "tickets" in ROLES[role]["tools"]:

        @server.tool(name="recent_support_tickets")
        def _recent_support_tickets(limit: int = 3) -> list[dict]:
            """The most recent support tickets of this agent's country (personal data redacted)."""
            return recent_tickets(limit)

    return server


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["governed", "raw"], default="governed")
    p.add_argument("--role", choices=sorted(ROLES), default="head_office")
    a = p.parse_args()
    build(a.mode, a.role).run("stdio")


if __name__ == "__main__":
    main()
