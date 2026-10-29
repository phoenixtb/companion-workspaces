"""Part 22: know before they do. OpenLineage events from Airflow, dbt and Spark; OpenMetadata as the lineage backend;
service level objectives for the daily revenue report, measured from the runs and Part 21's control table; and the
first queries of the "the dashboard is wrong" runbook.

uv run python -m scripts.part22_observe spark-lineage --day D   # Part 20's WAP job with the OpenLineage listener
uv run python -m scripts.part22_observe dbt-lineage             # dbt-ol over the models upstream of sales and stock
uv run python -m scripts.part22_observe emitted                 # what each integration emitted (the event files)
uv run python -m scripts.part22_observe om-setup                # OpenMetadata: service, schemas, OpenLineage settings
uv run python -m scripts.part22_observe replay                  # register the tables, POST every COMPLETE event
uv run python -m scripts.part22_observe trace TABLE [--column C]  # upstream lineage from OpenMetadata
uv run python -m scripts.part22_observe controls-history --from D --to D   # Part 21's controls for past days
uv run python -m scripts.part22_observe slo --at T              # the 06:00 check; gold.slo_results; exit 1 on FAIL
uv run python -m scripts.part22_observe triage --day D --country C   # the runbook's first five minutes
uv run python -m scripts.part22_observe otel                    # what Airflow sent the OpenTelemetry Collector
uv run python -m scripts.part22_observe logs TASK --dag DAG --day D   # a task's output and its alert, from Airflow
uv run python -m scripts.part22_observe reset-slo               # drop gold.slo_results and the published-through mark

Event files live in data/lab/part22/openlineage (the Airflow containers write theirs there); `reset-events` empties it.
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import yaml

from scripts import part09_catalog as cat9
from scripts import part17_dbt as p17
from scripts import part19_pipeline as p19
from scripts import part21_reconcile as p21

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "data/lab/part22"
EVENTS = LAB / "openlineage"
OTEL = LAB / "otel"
SPARK_JAR = "openlineage-spark_2.13-1.54.0.jar"
SPARK_JAR_URL = f"https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.13/1.54.0/{SPARK_JAR}"
PRODUCERS = {  # event file -> who wrote it
    "airflow.jsonl": "Airflow provider",
    "dbt.jsonl": "dbt-ol in Airflow",
    "dbt-host.jsonl": "dbt-ol on a laptop",
    "spark.jsonl": "Spark listener",
}
REPORT = "gold.rpt_daily_net_revenue_v2"
SLO_RESULTS = "gold.slo_results"
LONDON = ZoneInfo("Europe/London")
DEADLINE_HOUR = 6  # Farah's numbers by 06:00 London the morning after the upload day


def events(name: str) -> list[dict]:
    path = EVENTS / name
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()] if path.exists() else []


# ---------- 1. capture: Spark and dbt (Airflow's provider writes its own file from inside the containers) ----------


def spark_lineage(day: str) -> None:
    """Part 20's Write-Audit-Publish job for one upload day, with OpenLineage's Spark listener writing to a file."""
    jar = LAB / "jars" / SPARK_JAR
    if not jar.exists():
        jar.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(SPARK_JAR_URL, jar)
    from scripts.part16_cdc import SPARK_PIPELINE

    conf = {
        **SPARK_PIPELINE,
        "spark.app.name": "part20_wap",
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.namespace": "larkspur",
        "spark.openlineage.transport.type": "file",
        "spark.openlineage.transport.location": "/tmp/openlineage-spark.jsonl",
    }
    exec_ = ["docker", "compose", "exec", "-T", "spark"]
    subprocess.run([*exec_, "rm", "-f", "/tmp/openlineage-spark.jsonl"], cwd=ROOT, check=True)
    cmd = [*exec_, "/opt/spark/bin/spark-submit", "--master", "local[*,3]"]
    cmd += ["--jars", f"/opt/lakehouse/lab/part22/jars/{SPARK_JAR}"]
    for k, v in conf.items():
        cmd += ["--conf", f"{k}={v}"]
    started = time.monotonic()
    run = subprocess.run(
        [*cmd, "/opt/lakehouse/jobs/part20_wap.py", "--day", day], cwd=ROOT, capture_output=True, text=True
    )
    print(run.stdout.strip().splitlines()[-2] if run.stdout.strip() else run.stderr[-2000:])
    EVENTS.mkdir(parents=True, exist_ok=True)
    out = EVENTS / "spark.jsonl"
    subprocess.run(
        ["docker", "compose", "cp", "spark:/tmp/openlineage-spark.jsonl", str(out)], cwd=ROOT, check=True,
        capture_output=True,
    )  # fmt: skip
    print(
        f"spark-submit exit {run.returncode} in {time.monotonic() - started:.1f} s; {len(events('spark.jsonl'))} events"
    )


def dbt_lineage() -> None:
    """dbt-ol over every model the sales and stock reports read, except the four the daily DAG builds (they get their
    events from dbt-ol inside Airflow): views and small tables, about half a minute."""
    out = EVENTS / "dbt-host.jsonl"
    out.unlink(missing_ok=True)
    os.environ |= {
        "LAB_DBT_OPENLINEAGE": "1",
        "OPENLINEAGE_NAMESPACE": "larkspur",
        "OPENLINEAGE__TRANSPORT__TYPE": "file",
        "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH": str(out),
        "OPENLINEAGE__TRANSPORT__APPEND": "true",
    }
    run = p17.dbt(
        "run", "--target", "prod", "--select", "+rpt_daily_net_revenue_v2 +fct_stock_daily",
        "--exclude", "fct_sales_line fct_basket fct_refund rpt_daily_net_revenue_v2", echo=False,
    )  # fmt: skip
    done = [x for x in run.stdout.splitlines() if "Done." in x]
    print(done[-1].split("  ")[-1] if done else run.stdout[-1500:])
    print(f"{len(events('dbt-host.jsonl'))} events")


# ---------- 2. what each integration emitted ----------


def emitted() -> dict:
    """Per producer: events, jobs, the datasets in its events, how many outputs carry a columnLineage facet."""
    report = {}
    for name, who in PRODUCERS.items():
        evs = events(name)
        if not evs:
            continue
        outputs, with_cll, namespaces = set(), set(), Counter()
        for e in evs:
            for d in e.get("inputs", []) + e.get("outputs", []):
                namespaces[d["namespace"]] += 1
            for d in e.get("outputs", []):
                outputs.add(d["name"])
                if "columnLineage" in d.get("facets", {}):
                    with_cll.add(d["name"])
        types = Counter(e["eventType"] for e in evs)
        report[who] = {
            "events": len(evs),
            "types": dict(types),
            "jobs": len({e["job"]["name"] for e in evs}),
            "namespaces": sorted(namespaces),
            "outputs": len(outputs),
            "outputs_with_column_lineage": len(with_cll),
            "without": sorted(n.split(".")[-1].split("/")[-1] for n in outputs - with_cll),
        }
        print(f"{who:<20} {len(evs):>3} events ({', '.join(f'{k} {v}' for k, v in sorted(types.items()))}), "
              f"{report[who]['jobs']} jobs, outputs {len(outputs)}, with columnLineage {len(with_cll)}")  # fmt: skip
        print(f"{'':<20} namespaces: {', '.join(report[who]['namespaces'])}")
        if report[who]["without"]:
            print(f"{'':<20} outputs without column lineage: {', '.join(report[who]['without'])}")
    for title, name, dataset, column in (
        ("dbt-ol", "dbt.jsonl", "rpt_daily_net_revenue_v2", "net_minor"),
        ("Spark", "spark.jsonl", "fct_pos_line", "amount_minor"),
    ):
        cll = column_lineage(events(name), dataset)
        if column in cll:
            src = ", ".join(
                f"{f['name'].split('.')[-1].split('/')[-1]}.{f['field']}"
                + "".join(f" [{t['type']} {t.get('subtype', '')}]" for t in f.get("transformations", []))
                for f in cll[column]["inputFields"]
            )
            print(f"{title} column lineage, {dataset}.{column} <- {src}")
    return report


def column_lineage(evs: list[dict], dataset: str) -> dict:
    for e in reversed(evs):
        for d in e.get("outputs", []):
            if d["name"].endswith(dataset) and "columnLineage" in d.get("facets", {}):
                return d["facets"]["columnLineage"]["fields"]
    return {}


# ---------- 3. OpenMetadata: setup, replay, trace ----------

OM = os.environ.get("LAB_OPENMETADATA_URL", "http://localhost:8585/api/v1")
SERVICE, DATABASE, PIPELINES = "larkspur_trino", "lakehouse_pipeline", "openlineage"
# Two names for one Trino: `trino:8080` inside the containers, `localhost:8080` on a laptop. Both are one service.
NAMESPACES = {"trino://trino:8080": SERVICE, "trino://localhost:8080": SERVICE}


def om_token() -> str:
    body = {"email": "admin@open-metadata.org", "password": base64.b64encode(b"admin").decode()}  # lab default login
    return om("POST", "/users/login", body, token="")["accessToken"]


def om(method: str, path: str, body=None, token: str | None = None) -> dict:
    token = om_token() if token is None else token
    headers = {"Content-Type": "application/json"} | ({"Authorization": f"Bearer {token}"} if token else {})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(OM + path, data=data, headers=headers, method=method)
    try:
        return json.load(urllib.request.urlopen(req, timeout=120))
    except urllib.error.HTTPError as e:
        raise SystemExit(f"OpenMetadata {method} {path}: {e.code} {e.read().decode()[:500]}") from e


def om_setup() -> None:
    """The containers lineage needs before it can land: a database service for the lake (what OpenMetadata's Trino
    connector would ingest), its database and namespaces, a pipeline service for jobs, and the OpenLineage settings."""
    tok = om_token()
    om("PUT", "/services/databaseServices", {
        "name": SERVICE, "serviceType": "Trino",
        "connection": {"config": {"type": "Trino", "hostPort": "trino:8080", "username": "dbt", "catalog": DATABASE}},
    }, tok)  # fmt: skip
    om("PUT", "/databases", {"name": DATABASE, "service": SERVICE}, tok)
    for ns in ("raw", "silver", "gold"):
        om("PUT", "/databaseSchemas", {"name": ns, "database": f"{SERVICE}.{DATABASE}"}, tok)
    om("PUT", "/services/pipelineServices", {
        "name": PIPELINES, "serviceType": "OpenLineage", "connection": {"config": {"type": "OpenLineage"}},
    }, tok)  # fmt: skip
    settings = {
        "enabled": True,
        "autoCreateEntities": True,
        "eventTypeFilter": ["COMPLETE"],
        "ownershipUpdateMode": "replace",
        "defaultPipelineService": PIPELINES,
        "namespaceToServiceMapping": NAMESPACES,
    }
    om("PUT", "/system/settings", {"config_type": "openLineageSettings", "config_value": settings}, tok)
    print(json.dumps(om("GET", "/system/settings/openLineageSettings", token=tok)["config_value"]))


ICEBERG_TO_OM = {"string": "STRING", "long": "BIGINT", "int": "INT", "boolean": "BOOLEAN", "date": "DATE",
                 "timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMP", "double": "DOUBLE"}  # fmt: skip
TRINO_TO_OM = {"varchar": "STRING", "bigint": "BIGINT", "integer": "INT", "boolean": "BOOLEAN", "date": "DATE",
               "timestamp": "TIMESTAMP", "double": "DOUBLE", "time": "TIME"}  # fmt: skip


def om_column(name: str, kind: str, trino: bool) -> dict:
    base = kind.split("(")[0].split("<")[0].strip().lower()
    if base.startswith("decimal"):
        return {"name": name, "dataType": "DECIMAL"}
    if base in ("array", "list"):
        return {"name": name, "dataType": "ARRAY", "arrayDataType": "STRUCT"}
    if base in ("row", "struct"):
        return {"name": name, "dataType": "STRUCT"}
    return {"name": name, "dataType": (TRINO_TO_OM if trino else ICEBERG_TO_OM).get(base, "UNKNOWN")}


def lake_datasets(evs: list[dict]) -> dict[str, list[dict]]:
    """Every lake table the events name, as namespace.table -> columns: from the catalog when it's a table there
    (views aren't), else from the event's schema facet. Spark names tables by path; its symlinks facet has the name."""
    found: dict[str, list[dict]] = {}
    cat = cat9.pyiceberg("pipeline")
    for e in evs:
        for d in e.get("inputs", []) + e.get("outputs", []):
            if d["namespace"] in NAMESPACES:
                name = d["name"].removeprefix(f"{DATABASE}.")
            else:
                links = d.get("facets", {}).get("symlinks", {}).get("identifiers", [])
                if not links:
                    continue
                name = links[0]["name"]
            if name.count(".") != 1 or found.get(name):  # skip metadata tables such as fct_pos_line.snapshots
                continue
            try:
                schema = cat.load_table(name).schema()
                found[name] = [om_column(f.name, str(f.field_type), trino=False) for f in schema.fields]
            except Exception:
                fields = d.get("facets", {}).get("schema", {}).get("fields", [])
                found[name] = [om_column(f["name"], f.get("type", ""), trino=True) for f in fields]
    return found


def normalise(e: dict, fixes: Counter) -> dict:
    """Three things OpenMetadata 2.0.7 gets wrong with events that are valid OpenLineage: an eventTime with a "+00:00"
    offset (the Airflow provider's) is refused ("Invalid request format"); schema-facet fields without a type (dbt-ol's
    without a catalog.json) are refused ("query param type must not be null"); a parent job whose name has a dot
    (Airflow's dag.task) never matches its pipeline, because the pipeline's FQN is built unquoted. The replay adapts
    all three; lineage needs none of them as sent."""
    if e["eventTime"].endswith("+00:00"):
        e = {**e, "eventTime": e["eventTime"].removesuffix("+00:00") + "Z"}
        fixes["eventTime +00:00 -> Z"] += 1
    parent = e.get("run", {}).get("facets", {}).get("parent", {}).get("job", {})
    if "." in parent.get("name", ""):
        parent["name"] = parent["name"].replace(".", "_")
        fixes["parent job name: dots -> underscores"] += 1
    for d in e.get("inputs", []) + e.get("outputs", []):
        fields = d.get("facets", {}).get("schema", {}).get("fields", [])
        if any("type" not in f for f in fields):
            d["facets"].pop("schema")
            fixes["schema facet without types dropped"] += 1
    return e


def replay() -> dict:
    """Register the tables and pipelines the events name, then POST each producer's COMPLETE events (OpenMetadata
    processes only COMPLETE by default). Registering first stands in for OpenMetadata's catalog ingestion, and works
    around 2.0.7 failing to auto-create entities on PostgreSQL (issue #27548, fixed on main for 2.1)."""
    tok = om_token()
    all_events = {name: events(name) for name in PRODUCERS}
    flat = [e for evs in all_events.values() for e in evs]
    tables = lake_datasets(flat)
    for name, columns in sorted(tables.items()):
        schema, table = name.split(".")
        cols = columns or [{"name": "unknown", "dataType": "UNKNOWN"}]
        om("PUT", "/tables", {"name": table, "databaseSchema": f"{SERVICE}.{DATABASE}.{schema}", "columns": cols}, tok)
    parents = set()
    for e in flat:
        job = e["run"].get("facets", {}).get("parent", {}).get("job") or e["job"]
        parents.add(f"{job['namespace']}-{job['name'].replace('.', '_')}")
    for p in sorted(parents):
        om("PUT", "/pipelines", {"name": p, "service": PIPELINES}, tok)
    print(f"registered {len(tables)} tables and {len(parents)} pipelines")
    totals: dict[str, dict] = {}
    fixes: Counter = Counter()
    for name, evs in all_events.items():
        complete = [normalise(e, fixes) for e in evs if e["eventType"] == "COMPLETE"]
        complete.sort(key=lambda e: e["eventTime"])
        if not complete:
            continue
        res = om("POST", "/openlineage/lineage/batch", {"events": complete}, tok)
        totals[name] = {**res["summary"], "edges": res.get("lineageEdgesCreated", 0)}
        s = totals[name]
        print(f"{PRODUCERS[name]:<20} COMPLETE {s['received']:>3}: lineage from {s['successful']:>2}, "
              f"no resolvable table {s['skipped']:>2}, failed {s['failed']}; edges {s['edges']}")  # fmt: skip
    print("adapted for OpenMetadata 2.0.7: " + ", ".join(f"{k} ({v})" for k, v in fixes.items()))
    unmapped = Counter(
        d["namespace"] for e in flat for d in e.get("inputs", []) + e.get("outputs", [])
        if d["namespace"] not in NAMESPACES and not d.get("facets", {}).get("symlinks")
    )  # fmt: skip
    for ns, n in sorted(unmapped.items()):
        print(f"not resolvable: {n} dataset references in namespace {ns} (no service, no symlink)")
    return totals


def fqn(table: str) -> str:
    return f"{SERVICE}.{DATABASE}.{table}"


def upstream(table: str, tok: str) -> dict[str, list[tuple[str, str, list]]]:
    """table -> [(source table, job, column lineage)], walked one level at a time (the API allows depth 3 at most)."""
    ups: dict[str, list[tuple[str, str, list]]] = defaultdict(list)
    todo, done = [table], set()
    while todo:
        node = todo.pop()
        if node in done:
            continue
        done.add(node)
        g = om("GET", f"/lineage/table/name/{fqn(node)}?upstreamDepth=1&downstreamDepth=0", token=tok)
        names = {n["id"]: n["fullyQualifiedName"].removeprefix(f"{SERVICE}.{DATABASE}.") for n in g["nodes"]}
        for e in g["upstreamEdges"]:
            if e["toEntity"] != g["entity"]["id"] or e["fromEntity"] not in names:
                continue
            det = e.get("lineageDetails") or {}
            job = (det.get("pipeline") or {}).get("name", "?")
            src = names[e["fromEntity"]]
            ups[node].append((src, job, det.get("columnsLineage") or []))
            todo.append(src)
    return ups


def trace(table: str, column: str | None = None) -> dict:
    """Upstream lineage of `table` from OpenMetadata, printed as a tree; each edge names the job (OpenMetadata's
    pipeline: the OpenLineage parent job) that made it. With `column`, follow column lineage as far as it goes."""
    ups = upstream(table, om_token())
    seen: set[str] = set()

    def walk(node: str, indent: int) -> None:
        for src, job in sorted({(s, j) for s, j, _ in ups.get(node, []) if s != node}):
            note = "   (above)" if src in seen else "" if ups.get(src) else "   [no upstream]"
            print(f"{'  ' * indent}<- {src}  ({job}){note}")
            if src not in seen:
                seen.add(src)
                walk(src, indent + 1)

    if column is None:
        print(table)
        walk(table, 1)
    else:
        print(f"{table}.{column}")
        wanted, hops = [f"{fqn(table)}.{column}"], 0
        while wanted and hops < 8:
            nxt = []
            for col in wanted:
                for edges in ups.values():
                    for _, job, cll in edges:
                        for c in cll:
                            if c.get("toColumn") == col:
                                for f in c.get("fromColumns", []):
                                    short = f.removeprefix(f"{SERVICE}.{DATABASE}.")
                                    print(f"{'  ' * (hops + 1)}<- {short}  ({job})")
                                    nxt.append(f)
            wanted, hops = list(dict.fromkeys(nxt)), hops + 1
    return {"tables": len({s for v in ups.values() for s, _, _ in v} | {table}), "edges": sum(map(len, ups.values()))}


# ---------- 4. SLOs for the daily revenue report ----------


def contract_columns() -> dict[str, str]:
    """The report's v2 columns and types from the dbt contract (models/marts/_marts.yml)."""
    doc = yaml.safe_load((ROOT / "dbt/larkspur/models/marts/_marts.yml").read_text())
    model = next(m for m in doc["models"] if m["name"] == "rpt_daily_net_revenue")
    base = {c["name"]: c["data_type"] for c in model["columns"]}
    v2 = next(v for v in model["versions"] if v["v"] == 2)
    out = {}
    for c in v2["columns"]:
        if "include" in c:
            out |= {k: t for k, t in base.items() if k not in c.get("exclude", [])}
        else:
            out[c["name"]] = c["data_type"]
    return out


ICEBERG_TO_SQL = {"string": "varchar", "long": "bigint", "int": "integer", "date": "date", "boolean": "boolean"}


def expected_day(at: datetime) -> date:
    """The upload day the 06:00 London check at `at` expects published: yesterday's, once it's past 06:00 there."""
    local = at.astimezone(LONDON)
    return local.date() - timedelta(days=1 if local.hour >= DEADLINE_HOUR else 2)


def publishes(first: date, last: date) -> pa.Table:
    """One row per upload day in [first, last]: the state of the `publish` task in that day's run (one run per logical
    date). The lab's runs are backfills made today, so a run's own times say nothing about the 06:00 deadline: the
    check asks only whether the day *is* published at the moment it runs, which in production is the same question."""
    q = f"logical_date_gte={first}T00:00:00Z&logical_date_lte={last}T00:00:00Z&limit=100"
    rows = []
    for r in p19.api(f"/dags/larkspur_daily_revenue/dagRuns?{q}")["dag_runs"]:
        try:
            ti = p19.api(f"/dags/larkspur_daily_revenue/dagRuns/{r['dag_run_id']}/taskInstances/publish")
            state = ti["state"] or "none"
        except urllib.error.HTTPError:
            state = "none"
        rows.append({"upload_date": date.fromisoformat(r["logical_date"][:10]), "state": state})
    return pa.Table.from_pylist(rows, schema=pa.schema([("upload_date", pa.date32()), ("state", pa.string())]))


RECORDED = pa.schema([("expected_day", pa.date32()), ("sli", pa.string()), ("status", pa.string())])


def slo(at: datetime) -> list[dict]:
    """Evaluate the report's SLOs for the check at `at` and record them in gold.slo_results. The 06:00 check is the
    one the objective is about: its record is written once and never replaced (fixing the data later doesn't un-breach
    the morning), and the error budget counts those records. Any other time is a re-check: it measures now, records
    itself under its own time, and counts the 06:00 records too."""
    import warnings

    from pyiceberg.expressions import EqualTo

    warnings.filterwarnings("ignore", "Delete operation did not match any records")  # a check's first run
    started = time.monotonic()
    e = expected_day(at)
    first = e - timedelta(days=27)
    deadline = at.astimezone(LONDON).time() == datetime.min.time().replace(hour=DEADLINE_HOUR)
    cat = cat9.pyiceberg("pipeline")
    con = duckdb.connect()
    con.execute(f"SET VARIABLE expected_day = DATE '{e}'")
    con.execute(f"SET VARIABLE first_day = DATE '{first}'")
    report = cat.load_table(REPORT)
    through = report.properties.get(p19.PUBLISHED_THROUGH)  # set by the commit that moved the `published` tag
    con.execute(f"SET VARIABLE published_through = {f'DATE {chr(39)}{through}{chr(39)}' if through else 'NULL::DATE'}")
    con.register("publishes", publishes(first, e))
    controls = cat.load_table(p21.CONTROLS).scan(row_filter=f"run_day >= '{first - timedelta(days=8)}'").to_arrow()
    con.register("controls", controls)
    recorded = RECORDED.empty_table()
    if cat.table_exists(SLO_RESULTS):
        past = cat.load_table(SLO_RESULTS).scan(row_filter=f"expected_day >= '{first}'").to_arrow().to_pylist()
        mine = [r for r in past if r["check_at"] == at.astimezone(UTC)]
        if deadline and mine:  # the 06:00 measurement is a fact about that morning: report it, don't redo it
            when = min(r["evaluated_at"] for r in mine)
            print(f"the 06:00 check for upload day {e} was recorded at {when:%Y-%m-%d %H:%M} UTC; not measured again:")
            for r in mine:
                print(f"{r['sli']:<13} {r['scope']:<9} {r['status']:<5} {r['detail']}")
            return [r for r in mine if r["status"] == "FAIL"]
        kept = [r for r in past if r["check_at"].astimezone(LONDON).hour == DEADLINE_HOUR
                and r["check_at"].astimezone(LONDON).minute == 0 and r["check_at"] != at.astimezone(UTC)]  # fmt: skip
        recorded = pa.Table.from_pylist([{k: r[k] for k in RECORDED.names} for r in kept], schema=RECORDED)
    con.register("recorded", recorded)
    tag = report.metadata.refs.get("published")
    schema = report.schema()  # the published snapshot's schema, which can differ from main's
    if tag:
        schema = report.schemas()[report.metadata.snapshot_by_id(tag.snapshot_id).schema_id]
    actual = {f.name: ICEBERG_TO_SQL.get(str(f.field_type), str(f.field_type)) for f in schema.fields}
    expected = contract_columns()
    con.register("contract", pa.Table.from_pylist([
        {"column_name": c, "expected": expected.get(c), "actual": actual.get(c)}
        for c in dict.fromkeys([*expected, *actual])
    ]))  # fmt: skip
    rows = con.sql((ROOT / "sql/part22/slo.sql").read_text()).fetchall()
    cols = ["sli", "scope", "status", "good_days", "window_days", "budget_left", "measured", "detail"]
    out = [dict(zip(cols, r, strict=True)) for r in rows]
    kind = "the 06:00 check" if deadline else "a re-check"
    print(f"{kind} at {at.astimezone(LONDON):%Y-%m-%d %H:%M} London: upload day {e} due (window {first}..{e})")
    for r in out:
        days = f"{r['good_days']}/{r['window_days']} days, budget left {r['budget_left']}" if r["window_days"] else ""
        print(f"{r['sli']:<13} {r['scope']:<9} {r['status']:<5} {days:<30} {r['detail']}")
    measured = out[0]["measured"] + deadline  # this check measures its own day
    print(f"budget: {measured} days as measured by 06:00 checks, {out[0]['window_days'] - measured} from run history")
    record = pa.Table.from_pylist([
        {**{k: v for k, v in r.items() if k != "measured"}, "check_at": at.astimezone(UTC), "expected_day": e,
         "evaluated_at": datetime.now(UTC)} for r in out
    ])  # fmt: skip
    if not cat.table_exists(SLO_RESULTS):
        cat.create_table(SLO_RESULTS, schema=record.schema)
    t = cat.load_table(SLO_RESULTS)
    same = EqualTo("check_at", at.astimezone(UTC).isoformat())
    t.overwrite(record.cast(t.schema().as_arrow()), overwrite_filter=same)
    failed = [r for r in out if r["status"] == "FAIL"]
    print(f"{len(failed)} failed; {time.monotonic() - started:.1f} s")
    return failed


def controls_history(first: date, last: date) -> None:
    """Part 21's daily control for upload days that have none yet (Part 19's September backfill ran before the
    `reconcile` task existed), so the SLO window has history. Each day takes a few seconds."""
    t = p21.catalog().load_table(p21.CONTROLS)
    have = set(t.scan(selected_fields=("run_day",)).to_arrow().column("run_day").to_pylist())
    d = first
    while d <= last:
        if d not in have:
            failed = p21.control(d)
            print(f"{d}: {len(failed)} failed")
        d += timedelta(days=1)


# ---------- 5. the runbook's first queries ----------


def triage(day: date, country: str) -> None:
    """'Yesterday's number is wrong.' What the dashboard shows and since when, what main says, which controls fail
    for that day, and (when OpenMetadata is up) what the report is built from, by which jobs."""
    cat = cat9.pyiceberg("pipeline")
    report = cat.load_table(REPORT)
    con = duckdb.connect()
    for ref in ("published", "main"):
        snap = report.metadata.refs[ref].snapshot_id
        s = report.metadata.snapshot_by_id(snap)
        rows = report.scan(
            row_filter=f"country = '{country}' AND business_date = '{day}'", snapshot_id=snap,
            selected_fields=("net_minor", "is_provisional"),
        ).to_arrow().to_pylist()  # fmt: skip
        at = datetime.fromtimestamp(s.timestamp_ms / 1000, UTC)
        value = f"{rows[0]['net_minor'] / 100:,.2f}, provisional {rows[0]['is_provisional']}" if rows else "no row"
        print(f"{ref:<10} snapshot {snap} committed {at:%Y-%m-%d %H:%M} UTC: {country} {day} net {value}")
    props = report.properties
    print(f"published through upload day {props.get(p19.PUBLISHED_THROUGH)}, at {props.get(p19.PUBLISHED_AT)}")
    controls = cat.load_table(p21.CONTROLS).scan(row_filter=f"scope_date = '{day}'").to_arrow()
    con.register("c", controls)
    rows = con.sql(f"""SELECT run_day, check_name, status, source_count, target_count, target_minor - source_minor
                       FROM c WHERE (country = '{country}' OR country IS NULL) AND rule <> 'info'
                       QUALIFY run_day = max(run_day) OVER () ORDER BY status, check_name""").fetchall()
    fails = [r for r in rows if r[2] == "FAIL"]
    print(f"controls for {day} {country} (newest run, upload day {rows[0][0] if rows else '-'}): "
          f"{len(rows) - len(fails)} pass, {len(fails)} FAIL")  # fmt: skip
    for r in fails:
        print(f"  FAIL {r[1]}: source {r[3]}, target {r[4]}, money difference {p21.money(r[5])}")
    try:
        ups = upstream(REPORT, om_token())
    except (urllib.error.URLError, OSError, SystemExit) as err:
        print(f"lineage: OpenMetadata not reachable ({str(err)[:60]})")
        return
    direct = sorted({(src, job) for src, job, _ in ups[REPORT]})
    tables = {src for edges in ups.values() for src, _, _ in edges}
    sources = sorted(t for t in tables if not ups.get(t))
    print(f"lineage: {REPORT} <- " + ", ".join(f"{src} ({job})" for src, job in direct))
    print(f"  {len(tables)} tables upstream; {len(sources)} with no upstream edge: {', '.join(sources)}")


# ---------- 6. OpenTelemetry ----------


def otel() -> dict:
    """What Airflow sent the collector: metric names by family, the newest DAG-run durations, and the spans."""
    names: set[str] = set()
    runs: dict[str, float] = {}
    path = OTEL / "metrics.jsonl"
    for line in path.read_text().splitlines() if path.exists() else []:
        for rm in json.loads(line)["resourceMetrics"]:
            for sm in rm["scopeMetrics"]:
                for m in sm["metrics"]:
                    names.add(m["name"])
                    kind = next(k for k in m if k in ("sum", "gauge", "histogram", "exponentialHistogram"))
                    if m["name"].startswith("airflow.dagrun.duration.") and m["name"].count(".") == 4:
                        for p in m[kind]["dataPoints"]:
                            if int(p.get("count", 0)):
                                runs[m["name"]] = float(p["sum"]) / int(p["count"]) / 1000
    families = Counter(n.split(".")[1] for n in names)
    print(f"{len(names)} metric names; by family: " + ", ".join(f"{k} {v}" for k, v in families.most_common(6)))
    for name, secs in sorted(runs.items()):
        print(f"  {name:<58} mean {secs:6.1f} s")
    spans: list[tuple[str, float, dict]] = []
    tpath = OTEL / "traces.jsonl"
    for line in tpath.read_text().splitlines() if tpath.exists() else []:
        for rs in json.loads(line)["resourceSpans"]:
            for ss in rs["scopeSpans"]:
                for s in ss["spans"]:
                    attrs = {a["key"]: next(iter(a["value"].values())) for a in s.get("attributes", [])}
                    took = (int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e9
                    spans.append((s["name"], took, attrs))
    print(f"{len(spans)} spans; by name: " + ", ".join(f"{k} {v}" for k, v in Counter(n for n, _, _ in spans).items()))
    for name, took, attrs in spans:
        if name.startswith("dag_run."):
            print(f"  {name:<40} {took:6.1f} s  {attrs.get('airflow.dag_run.run_id')}")
    return {"metrics": len(names), "spans": len(spans)}


def logs(dag_id: str, task_id: str, day: date) -> None:
    """A task's output and any alert its callback logged, for the run whose logical date falls on `day`."""
    run = next(
        r for r in p19.api(f"/dags/{dag_id}/dagRuns?limit=100")["dag_runs"] if r["logical_date"][:10] == str(day)
    )
    base = f"/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances/{task_id}"
    ti = p19.api(base)
    print(f"{dag_id} {run['dag_run_id']} {task_id}: {ti['state']}")
    for c in p19.api(f"{base}/logs/{ti['try_number']}?full_content=true")["content"]:
        if not isinstance(c, dict) or str(c.get("timestamp", "")) < (ti["start_date"] or ""):
            continue  # a cleared and re-run try can share a log file with an older run of the same ID
        logger, line = str(c.get("logger", "")), str(c.get("event", ""))
        if logger == "larkspur_alerts" or (
            logger.endswith("SubprocessHook") and not line.startswith(("Running command", "Output:", "Command", "Tmp"))
        ):
            print(line)


def reset_slo() -> None:
    """Start the SLO story over: drop gold.slo_results and the report's published-through properties."""
    cat9.drop_table("gold", "slo_results", cat9.token())
    t = cat9.pyiceberg("pipeline").load_table(REPORT)
    with t.transaction() as tx:
        tx.remove_properties(p19.PUBLISHED_THROUGH, p19.PUBLISHED_AT)
    print("dropped gold.slo_results; removed the report's published-through properties")


def reset_events() -> None:
    for f in [*EVENTS.glob("*.jsonl"), *OTEL.glob("*.jsonl")]:
        f.unlink()
    print(f"emptied {EVENTS} and {OTEL}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command")
    p.add_argument("target", nargs="?")
    p.add_argument("--day", type=date.fromisoformat)
    p.add_argument("--from", dest="first", type=date.fromisoformat)
    p.add_argument("--to", dest="last", type=date.fromisoformat)
    p.add_argument("--at", type=datetime.fromisoformat)
    p.add_argument("--column")
    p.add_argument("--country", default="GB")
    p.add_argument("--dag", default="larkspur_data_slo")
    a = p.parse_args()
    if a.command == "spark-lineage":
        spark_lineage(str(a.day))
    elif a.command == "dbt-lineage":
        dbt_lineage()
    elif a.command == "emitted":
        emitted()
    elif a.command == "om-setup":
        om_setup()
    elif a.command == "replay":
        replay()
    elif a.command == "trace":
        trace(a.target, a.column)
    elif a.command == "controls-history":
        controls_history(a.first, a.last)
    elif a.command == "slo":
        at = a.at or datetime.now(UTC)
        sys.exit(1 if slo(at if at.tzinfo else at.replace(tzinfo=LONDON)) else 0)
    elif a.command == "triage":
        triage(a.day, a.country)
    elif a.command == "otel":
        otel()
    elif a.command == "logs":
        logs(a.dag, a.target, a.day)
    elif a.command == "reset-slo":
        reset_slo()
    elif a.command == "reset-events":
        reset_events()
    else:
        raise SystemExit(f"unknown command {a.command}")


if __name__ == "__main__":
    main()
