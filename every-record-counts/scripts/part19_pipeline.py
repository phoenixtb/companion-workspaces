"""Part 19: the steps Airflow runs for the daily revenue pipeline, as plain commands. The DAGs in airflow/dags only
call these; everything they decide (which days to rebuild, which models, which tables to maintain) lives here, in SQL,
or in the dbt project.

uv run python -m scripts.part19_pipeline setup               # raw.pos_uploads partitioned by upload_date (once)
uv run python -m scripts.part19_pipeline ingest --day D      # land upload day D, replacing whatever D held
uv run python -m scripts.part19_pipeline build --day D       # dbt build of the revenue models for the days D touches
uv run python -m scripts.part19_pipeline check --day D       # sql/part19/checks.sql; exit 1 if any rule breaks
uv run python -m scripts.part19_pipeline publish --day D     # move the `published` tag to the checked report
uv run python -m scripts.part19_pipeline maintain [--table gold.fct_sales_line] [--retention 7d]  # default: MAINTAINED
uv run python -m scripts.part19_pipeline fx-correction --day D  # a hand-made rate change on a closed day
uv run python -m scripts.part19_pipeline fingerprint --day D # hashes of what a run for D rebuilds (idempotency)
uv run python -m scripts.part19_pipeline q3                  # Q3 net revenue by country, as published
uv run python -m scripts.part19_pipeline runs --dag-id ID    # Airflow's runs and task states (host, REST API)
uv run python -m scripts.part19_pipeline wait --dag-id A,B   # until those DAGs have nothing queued or running

Inside the Airflow containers the endpoints come from LAB_* variables (compose.yaml); on the host, localhost.
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb

from scripts import part09_catalog as cat9
from scripts import part17_dbt as p17
from scripts import part23_privacy as privacy

ROOT = Path(__file__).resolve().parents[1]
CHECKS = ROOT / "sql/part19/checks.sql"
CAT = "lakehouse_pipeline"  # Trino catalog of the pipeline principal (Part 17): vended, table-scoped credentials
REPORT = "gold.rpt_daily_net_revenue_v2"
TAG = "published"
POS = "SELECT * EXCLUDE (schema_version) FROM raw_pos_events WHERE {where}"


def trino(sql: str, session: dict[str, str] | None = None) -> list:
    """One Trino statement as user `lab`; `session` sets session properties (X-Trino-Session) for that statement."""
    if not session:
        rows, err = cat9.trino(sql)
        if err:
            raise SystemExit(f"Trino: {err}")
        return rows
    headers = {"X-Trino-User": "lab", "X-Trino-Session": ",".join(f"{k}={v}" for k, v in session.items())}
    req = urllib.request.Request(f"{cat9.TRINO}/v1/statement", data=sql.encode(), headers=headers)
    page, rows = json.load(urllib.request.urlopen(req)), []
    while True:
        rows += page.get("data", [])
        if "error" in page:
            raise SystemExit(f"Trino: {page['error']['message']}")
        if "nextUri" not in page:
            return rows
        page = json.load(urllib.request.urlopen(page["nextUri"]))


# ---------- ingest: one upload day, replaced as a whole ----------


def setup() -> None:
    """Part 17 landed raw.pos_uploads unpartitioned, so replacing one upload day rewrote files holding other days.
    Partitioned by upload_date, a day's replace drops that day's files and writes new ones: nothing else moves."""
    cat = cat9.pyiceberg("pipeline")
    table = cat.load_table("raw.pos_uploads")
    if any(f.name == "upload_date" for f in table.spec().fields):
        print(f"raw.pos_uploads already partitioned: {table.spec()}")
        return
    data = p17.raw_views().sql(POS.format(where="true") + " ORDER BY upload_date, uploaded_at").to_arrow_table()
    data = privacy.suppress(data, tombstone=p17.SUPPRESSED["pos_uploads"])  # Part 23: erased people stay erased
    cat9.drop_table("raw", "pos_uploads", cat9.token())
    table = cat.create_table("raw.pos_uploads", schema=data.schema)
    with table.update_spec() as spec:  # on the empty table: every file written from now on holds one upload day
        spec.add_identity("upload_date")
    table.append(data)
    files = trino(f'SELECT count(*) FROM {CAT}.raw."pos_uploads$files"')[0][0]
    print(f"raw.pos_uploads: {data.num_rows:,} events re-landed, partitioned by upload_date, {files} files")


def ingest(day: date) -> dict:
    """Replace upload day `day` in raw.pos_uploads with the raw zone's files for that day: delete-by-partition and
    append in one Iceberg transaction. Running it twice leaves the table as running it once."""
    from pyiceberg.expressions import EqualTo

    folder = ROOT / f"data/raw/pos/events/upload_date={day}"
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    data = con.sql(f"SELECT * EXCLUDE (schema_version) FROM read_json('{folder}/*.jsonl.gz', hive_partitioning = true)")
    table = cat9.pyiceberg("pipeline").load_table("raw.pos_uploads")
    data = data.to_arrow_table().cast(table.schema().as_arrow())  # one day's JSON, typed as the table's columns
    data = privacy.suppress(data, tombstone=p17.SUPPRESSED["pos_uploads"])  # Part 23: erased people stay erased
    table.overwrite(data, overwrite_filter=EqualTo("upload_date", day.isoformat()))
    held = trino(f"SELECT count(*) FROM {CAT}.raw.pos_uploads WHERE upload_date = DATE '{day}'")[0][0]
    out = {"upload_date": str(day), "events": data.num_rows, "in_table": held}
    print(json.dumps(out))
    if held != data.num_rows:
        raise SystemExit(f"upload day {day}: {data.num_rows} events read, {held} in the table")
    return out


# ---------- build: the dbt models behind the daily revenue report, for the days one upload day can touch ----------

# Uploads of day D carry sales rung up on D-1 and D (Part 18 measured lateness under one UTC day), so a run for D
# rebuilds exactly those microbatches; fct_refund and the report are tables, rebuilt whole.
BUILD_MODELS = ["fct_sales_line", "fct_basket", "fct_refund", "rpt_daily_net_revenue"]
LATENESS_DAYS = 1


def build(day: date) -> int:
    start, end = day - timedelta(days=LATENESS_DAYS), day + timedelta(days=1)
    run = p17.dbt("build", "--target", "prod", "--select", *BUILD_MODELS,
                  "--event-time-start", str(start), "--event-time-end", str(end))  # fmt: skip
    return run.returncode


# ---------- checks and publish ----------


def checks(day: date) -> dict[str, list]:
    """Run each named query in sql/part19/checks.sql; a rule passes when its query returns no rows."""
    names = re.findall(r"^-- (\w+):", CHECKS.read_text(), flags=re.M)
    has_tag = bool(trino(f"""SELECT 1 FROM {CAT}.gold."rpt_daily_net_revenue_v2$refs" WHERE name = '{TAG}'"""))
    out = {}
    for name, sql in zip(names, p17.statements(CHECKS), strict=True):
        if "FOR VERSION AS OF" in sql and not has_tag:
            print(f"{name:<20} skipped: nothing published yet")
            continue
        out[name] = trino(sql.replace("{day}", day.isoformat()))
        print(f"{name:<20} {'pass' if not out[name] else f'FAIL {len(out[name])} rows: {out[name][:5]}'}")
    return out


PUBLISHED_THROUGH = "larkspur.published-through-upload-date"
PUBLISHED_AT = "larkspur.published-at"


def publish(day: date) -> dict:
    """Point the `published` tag at the report snapshot the checks just passed. Readers who must see only checked
    numbers read `FOR VERSION AS OF 'published'`; the table's main branch moves with every dbt build."""
    from pyiceberg.table.update.snapshot import ManageSnapshots

    table = cat9.pyiceberg("pipeline").load_table(REPORT)
    snap = table.current_snapshot().snapshot_id
    previous = table.metadata.refs.get(TAG)
    # Part 22: the same commit records which upload days the published snapshot covers, so a freshness check can ask
    # the table, not the scheduler. A re-run of an older day doesn't move it back.
    covered = max(str(day), table.properties.get(PUBLISHED_THROUGH, ""))
    with table.transaction() as tx:
        with ManageSnapshots(transaction=tx) as ms:
            if previous:
                ms.remove_tag(TAG)
            ms.create_tag(snap, TAG)
        tx.set_properties({PUBLISHED_THROUGH: covered, PUBLISHED_AT: datetime.now(UTC).isoformat(timespec="seconds")})
    uk = trino(f"""SELECT net_minor FROM {CAT}.{REPORT} FOR VERSION AS OF '{TAG}'
                   WHERE country = 'GB' AND business_date = DATE '{day}'""")
    out = {
        "upload_date": str(day),
        "snapshot_id": snap,
        "previous_snapshot_id": previous.snapshot_id if previous else None,
        "uk_net_minor": uk[0][0] if uk else None,
    }
    print(json.dumps(out))
    return out


# ---------- maintenance: Trino's Iceberg table procedures ----------

DEFAULT_RETENTION = "7d"
MAINTAINED = ["raw.pos_uploads", "gold.fct_sales_line", "gold.fct_basket"]  # the tables the daily run churns


def maintain_all(retention: str = DEFAULT_RETENTION, tables: list[str] = MAINTAINED) -> list[dict]:
    """Expire before removing orphans: files that only expired snapshots used are then gone too."""
    return [maintain(t, retention) for t in tables]


def maintain(table: str, retention: str = DEFAULT_RETENTION) -> dict:
    """Compaction, snapshot expiry and orphan-file removal for one table, through Trino as the pipeline principal."""
    ns, name = table.split(".")

    def state() -> dict:
        files = dict(trino(f'SELECT content, count(*) FROM {CAT}.{ns}."{name}$files" GROUP BY content'))
        snaps = trino(f'SELECT count(*) FROM {CAT}.{ns}."{name}$snapshots"')[0][0]
        return {"data_files": files.get(0, 0), "delete_files": files.get(1, 0), "snapshots": snaps}

    before = state()
    started = time.monotonic()
    out = {"table": table, "before": before}
    out["optimize"] = trino(f"ALTER TABLE {CAT}.{table} EXECUTE optimize(file_size_threshold => '100MB')")
    # Trino refuses a retention under its 7-day minimum (iceberg.*.min-retention) unless the session lowers it. The
    # lab's history is hours old, so its Airflow Variable says 1h; production keeps 7d and sends no session override.
    session = None
    if retention != DEFAULT_RETENTION:
        session = {
            f"{CAT}.expire_snapshots_min_retention": retention,
            f"{CAT}.remove_orphan_files_min_retention": retention,
        }
    out["expire_snapshots"] = trino(
        f"ALTER TABLE {CAT}.{table} EXECUTE expire_snapshots(retention_threshold => '{retention}')", session
    )
    out["remove_orphan_files"] = trino(
        f"ALTER TABLE {CAT}.{table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')", session
    )
    out["after"] = state()
    out["seconds"] = round(time.monotonic() - started, 1)
    print(json.dumps(out, default=str))
    return out


# ---------- a restatement the checks must stop (Hands-On) ----------


def fx_correction(day: date, pct: float = 1.0) -> list:
    """Someone 'corrects' one closed day's INR->GBP rate in gold.dim_fx_rate by hand. The next run rebuilds the report
    from it; closed_days_moved must refuse to publish. `dbt run --select dim_fx_rate` puts the rate back."""
    rows = trino(f"""UPDATE {CAT}.gold.dim_fx_rate SET rate = rate * (1 + {pct} / 100)
                     WHERE "date" = DATE '{day}' AND base_currency = 'INR' AND quote_currency = 'GBP'""")
    print(f"dim_fx_rate INR->GBP on {day}: {rows[0][0]} row(s) changed by {pct:+}%")
    return rows


# ---------- evidence ----------


def fingerprint(day: date) -> dict:
    """What a run for upload day `day` rebuilds, reduced to hashes: the sales fact's rows for UTC days day-1 and day,
    and the published report. Equal fingerprints before and after a re-run = the run is idempotent."""
    lo, hi = day - timedelta(days=1), day + timedelta(days=1)
    cols = ", ".join(f"coalesce(cast({c} as varchar), '~')" for c in p17_columns())
    rows = trino(f"""
        SELECT count(*), sum(amount_minor),
               to_hex(md5(to_utf8(array_join(array_sort(array_agg(to_hex(md5(to_utf8(concat_ws('|', {cols}))))))
                                             , ''))))
        FROM {CAT}.gold.fct_sales_line
        WHERE event_ts >= TIMESTAMP '{lo} 00:00:00 UTC' AND event_ts < TIMESTAMP '{hi} 00:00:00 UTC'""")[0]
    report = trino(f"""
        SELECT count(*), sum(net_minor),
               to_hex(md5(to_utf8(array_join(array_sort(array_agg(
                   concat_ws('|', country, cast(business_date AS varchar), cast(net_minor AS varchar)))), ','))))
        FROM {CAT}.{REPORT}""")[0]
    raw = trino(f"SELECT count(*) FROM {CAT}.raw.pos_uploads WHERE upload_date = DATE '{day}'")[0][0]
    out = {
        "raw_events": raw,
        "fact_lines": rows[0],
        "fact_minor": rows[1],
        "fact_hash": rows[2][:16],
        "report_rows": report[0],
        "report_net_minor": report[1],
        "report_hash": report[2][:16],
    }
    print(json.dumps(out))
    return out


def p17_columns() -> list[str]:
    from scripts import part18_dbt as p18

    return p18.COLUMNS


Q3 = f"""
    SELECT country, sum(net_minor)
    FROM lakehouse_analyst.{REPORT} FOR VERSION AS OF '{TAG}'
    WHERE business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
    GROUP BY country ORDER BY country"""


def q3() -> dict:
    """Q3 net revenue by country from the published report, read as Farah's team reads it (the analyst principal),
    against the manifest's correct UK figure."""
    want = json.loads((ROOT / "data/raw/_manifest.json").read_text())["uk_net_revenue_q3"]["correct_minor"]
    rows = trino(Q3)
    for country, minor in rows:
        mark = ("  manifest " + ("exact" if minor == want else f"OFF by {minor - want}")) if country == "GB" else ""
        print(f"{country}  {minor / 100:>16,.2f}{mark}")
    return dict(rows)


# ---------- Airflow, read from the host through its REST API ----------

AIRFLOW = os.environ.get("LAB_AIRFLOW_URL", "http://localhost:8090")


def api(path: str) -> dict:
    return json.load(urllib.request.urlopen(f"{AIRFLOW}/api/v2{path}"))


def runs(dag_id: str, limit: int = 100) -> list[dict]:
    """Every run of `dag_id` with how it was created (scheduled, backfill, asset, manual) and its tasks' states."""
    out = []
    for r in api(f"/dags/{dag_id}/dagRuns?limit={limit}&order_by=logical_date")["dag_runs"]:
        tis = api(f"/dags/{dag_id}/dagRuns/{r['dag_run_id']}/taskInstances")["task_instances"]
        took = sum(t["duration"] or 0 for t in tis)
        out.append({**r, "tasks": {t["task_id"]: (t["state"], t["try_number"]) for t in tis}, "task_seconds": took})
        logical = (r.get("logical_date") or "")[:10] or "-"
        tries = max((t["try_number"] for t in tis), default=0)
        print(f"{r['run_type']:<15} {logical:<11} {r['state']:<8} {len(tis)} tasks {took:6.1f} s  max try {tries}")
    kinds: dict[tuple, list[float]] = {}
    for r in out:
        kinds.setdefault((r["run_type"], r["state"]), []).append(r["task_seconds"])
    for (kind, state), secs in sorted(kinds.items()):
        secs.sort()
        med = secs[len(secs) // 2]
        print(f"total: {len(secs)} {kind} {state}, task time median {med:.1f} s, sum {sum(secs):.0f} s")
    return out


def wait(dag_ids: list[str], quiet_for: int = 30) -> None:
    """Block until none of `dag_ids` has a queued or running backfill, manual or asset-triggered run for `quiet_for`
    seconds in a row (asset-triggered runs appear a few seconds after the run that emitted the event). Scheduled runs
    are ignored: one may wait hours in its sensor for uploads the lab never gets."""
    calm = 0
    while calm < quiet_for:
        busy = sum(
            1
            for d in dag_ids
            for r in api(f"/dags/{d}/dagRuns?state=queued&state=running&limit=100")["dag_runs"]
            if r["run_type"] != "scheduled"
        )
        calm = 0 if busy else calm + 5
        time.sleep(5)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command")
    p.add_argument("--day", type=date.fromisoformat)
    p.add_argument("--table")
    p.add_argument("--retention", default=DEFAULT_RETENTION)
    p.add_argument("--dag-id")
    a = p.parse_args()
    if a.command == "setup":
        setup()
    elif a.command == "ingest":
        ingest(a.day)
    elif a.command == "check":
        sys.exit(1 if any(checks(a.day).values()) else 0)
    elif a.command == "publish":
        publish(a.day)
    elif a.command == "maintain":
        maintain_all(a.retention, [a.table] if a.table else MAINTAINED)
    elif a.command == "build":
        sys.exit(build(a.day))
    elif a.command == "fx-correction":
        fx_correction(a.day)
    elif a.command == "fingerprint":
        fingerprint(a.day)
    elif a.command == "q3":
        q3()
    elif a.command == "runs":
        runs(a.dag_id)
    elif a.command == "wait":
        wait(a.dag_id.split(","))
    else:
        raise SystemExit(f"unknown command {a.command}")


if __name__ == "__main__":
    main()
