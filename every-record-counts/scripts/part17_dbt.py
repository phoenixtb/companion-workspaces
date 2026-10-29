"""Part 17: the Season 1 star schema as a dbt project on the lakehouse (dbt-core 1.12 + dbt-trino), and a subset
of it under dbt v2 (dbt-oss) on a local DuckDB file.

uv run python -m scripts.part17_dbt setup       # grants dbt needs in Polaris (catalog role `transformer`)
uv run python -m scripts.part17_dbt land        # the raw files Season 2 hasn't ingested yet, as raw.* Iceberg tables
uv run python -m scripts.part17_dbt dbt build   # any dbt command, in dbt/larkspur against Trino (the `prod` target)
uv run python -m scripts.part17_dbt replay      # run `dbt snapshot` once per morning, 1 June to 4 October
uv run python -m scripts.part17_dbt history     # what the snapshots recorded
uv run python -m scripts.part17_dbt check       # the dbt-built facts vs Part 5's corrected numbers
uv run python -m scripts.part17_dbt extract     # a few lake tables into data/lab/part17/extract.duckdb
uv run python -m scripts.part17_dbt v2 parse    # the same project under dbt-oss 2.0.8 (its own environment)
uv run python -m scripts.part17_dbt timing      # parse and build times, v1 vs v2, on the DuckDB subset

Needs `make up-trino` and Part 16's raw.orders/raw.refunds (CDC replicas); without them `land` copies the
5 October extract instead. dbt-core and dbt-trino come from `uv sync --extra dbt`; dbt-oss lives in .venv-dbt2
(`make dbt-oss-install`), because the two distributions both install a `dbt` command and can't share an environment.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from scripts import part09_catalog as cat9
from scripts.part11_single_node import container_peak

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "dbt/larkspur"
RAW = ROOT / "data/raw"
LAB = ROOT / "data/lab/part17"
EXTRACT = LAB / "extract.duckdb"
DBT2 = ROOT / ".venv-dbt2/bin/dbt"
FIRST_DAY, LAST_DAY = date(2026, 6, 1), date(2026, 10, 4)

# Polaris: Part 9's lake_writer role lets the pipeline principal create, write and drop tables. Trino also reads
# namespace properties before CREATE TABLE AS, and dbt's staging models are views, so dbt needs a little more.
TRANSFORMER = "transformer"
TRANSFORMER_GRANTS = [
    "NAMESPACE_READ_PROPERTIES",
    "TABLE_READ_PROPERTIES",
    "TABLE_WRITE_PROPERTIES",
    "VIEW_CREATE",
    "VIEW_DROP",
    "VIEW_LIST",
    "VIEW_READ_PROPERTIES",
    "VIEW_WRITE_PROPERTIES",
]
DBT_NAMESPACES = ["silver", "gold"]

# The raw zone's files that no Season 2 ingestion has landed yet (Part 15 did suppliers, Part 16 orders/refunds).
# Each becomes one raw.* Iceberg table, written by the pipeline principal: what ingestion would land, nothing more.
LANDED = {
    "stores": "SELECT * FROM raw_stores",
    "pos_uploads": "SELECT * EXCLUDE (schema_version) FROM raw_pos_events",  # every upload day, nested lines kept
    "order_lines": "SELECT * FROM raw_order_lines",
    "catalog_products": "SELECT * FROM raw_catalog_products",  # one full extract per day
    "promotion_versions": "SELECT * FROM raw_promotion_versions",
    "fx_rates": "SELECT * FROM raw_fx_rates",
    "stock_snapshots": "SELECT * FROM raw_stock_snapshots",
    "stock_adjustments": "SELECT * FROM raw_stock_adjustments",
    "suppliers": "SELECT * FROM raw_suppliers",
    "po_lines": "SELECT * FROM raw_po_lines",
}
FALLBACK = {  # only if Part 16's CDC replicas are missing: the 5 October extract (same Q3 rows, Part 16 step 1)
    "orders": "SELECT * EXCLUDE (snapshot_date) FROM raw_orders",
    "refunds": "SELECT * EXCLUDE (snapshot_date) FROM raw_refunds",
}


# ---------- setup and landing ----------


def setup() -> None:
    tok = cat9.token()
    mgmt, catalog = cat9.MGMT, cat9.CATALOG
    status, _ = cat9.call(
        "POST", f"{mgmt}/catalogs/{catalog}/catalog-roles", {"catalogRole": {"name": TRANSFORMER}}, tok=tok
    )
    cat9.ok(status, f"catalog role {TRANSFORMER}", (201, 409))
    for ns in DBT_NAMESPACES:
        for privilege in TRANSFORMER_GRANTS:
            grant = {"type": "namespace", "namespace": [ns], "privilege": privilege}
            path = f"{mgmt}/catalogs/{catalog}/catalog-roles/{TRANSFORMER}/grants"
            cat9.ok(cat9.call("PUT", path, {"grant": grant}, tok=tok)[0], f"grant {privilege}")
    body = {"catalogRole": {"name": TRANSFORMER}}
    cat9.ok(cat9.call("PUT", f"{mgmt}/principal-roles/pipelines/catalog-roles/{catalog}", body, tok=tok)[0], "assign")
    # Trino drops Iceberg tables with purgeRequested=true; Polaris refuses that unless the catalog allows it.
    status, cat = cat9.call("GET", f"{mgmt}/catalogs/{catalog}", tok=tok)
    props = cat.get("properties", {})
    if props.get("polaris.config.drop-with-purge.enabled") != "true":
        new = {**props, "polaris.config.drop-with-purge.enabled": "true"}
        body = {"currentEntityVersion": cat["entityVersion"], "properties": new}
        cat9.ok(cat9.call("PUT", f"{mgmt}/catalogs/{catalog}", body, tok=tok)[0], "catalog properties")
    print(f"pipeline principal: + catalog role {TRANSFORMER} on {', '.join(DBT_NAMESPACES)}:")
    print("  " + ", ".join(TRANSFORMER_GRANTS))
    print(f"catalog {catalog}: polaris.config.drop-with-purge.enabled = true")


def raw_views() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # Arrow timestamps in UTC; PyIceberg rejects other zones
    con.execute(f"SET VARIABLE raw_root = '{RAW}'")
    con.execute((ROOT / "sql/raw/views.sql").read_text())
    return con


def land(only: list[str] | None = None) -> dict[str, int]:
    cat = cat9.pyiceberg("pipeline")
    tok = cat9.token()
    con = raw_views()
    have = {t[-1] for t in cat.list_tables("raw")}
    todo = dict(LANDED)
    for name, query in FALLBACK.items():
        if name not in have:
            todo[name] = query
            print(f"raw.{name}: Part 16's CDC replica is missing, landing the 5 October extract instead")
    out = {}
    for name, query in todo.items():
        if only and name not in only:
            continue
        started = time.monotonic()
        data: pa.Table = con.sql(query).to_arrow_table()
        cat9.drop_table("raw", name, tok)  # re-runs start from an empty table
        table = cat.create_table(f"raw.{name}", schema=data.schema)
        table.append(data)
        out[name] = data.num_rows
        print(f"raw.{name:<20} {data.num_rows:>11,} rows  {time.monotonic() - started:5.1f}s")
    if "pos_uploads" in out:  # Part 21: stg_pos_sale_lines reads only accepted events; decide any day not yet decided
        from scripts import part21_reconcile as p21

        p21.ensure_dispositions()
    return out


# ---------- dbt ----------


def dbt(*args: str, v2: bool = False, project: Path = PROJECT, echo: bool = True) -> subprocess.CompletedProcess:
    """Run one dbt command in a project directory (its profiles.yml lives there too). v2 = dbt-oss.
    Part 22: with LAB_DBT_OPENLINEAGE=1 (set in the Airflow containers), dbt-ol wraps dbt and emits OpenLineage
    events for every node it ran, configured by the OPENLINEAGE__* variables."""
    wrapper = "dbt-ol" if os.environ.get("LAB_DBT_OPENLINEAGE") == "1" else "dbt"
    exe = [str(DBT2)] if v2 else [str(Path(sys.executable).parent / wrapper)]
    env = {**os.environ, "DBT_PROFILES_DIR": str(project), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"  # dbt-ol starts `dbt` from PATH
    run = subprocess.run([*exe, *args], cwd=project, env=env, capture_output=True, text=True)
    if echo:
        print(run.stdout, end="")
        print(run.stderr, end="", file=sys.stderr)
    return run


def trino(sql: str) -> list:
    rows, err = cat9.trino(sql)
    if err:
        raise SystemExit(f"Trino: {err}")
    return rows


def replay(first: date = FIRST_DAY, last: date = LAST_DAY, every: int = 1) -> list[tuple]:
    """One `dbt build` of the two snapshots (and the views they read) per morning, in this process."""
    from dbt.cli.main import dbtRunner

    for name in ("snp_product", "snp_promotion"):  # history starts empty: a replay can't be re-run on top of itself
        for table in (name, f"{name}__dbt_tmp"):  # a run killed mid-snapshot leaves its staging table behind
            trino(f"DROP TABLE IF EXISTS lakehouse_pipeline.silver.{table}")
    os.environ["DBT_PROFILES_DIR"] = str(PROJECT)
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    cwd = os.getcwd()
    os.chdir(PROJECT)
    runner, select = dbtRunner(), "stg_catalog_on_morning stg_promotions_on_morning snp_product snp_promotion"
    changes, started, day = [], time.monotonic(), first
    try:
        while day <= last:
            res = runner.invoke(["build", "--quiet", "--select", select, "--vars", f'{{as_of: "{day}"}}'])
            if not res.success:
                raise SystemExit(f"dbt snapshot failed on {day}: {res.exception}")
            rows = {
                r.node.name: r.adapter_response.get("rows_affected") for r in res.result if r.node.name[:4] == "snp_"
            }
            changes.append((day, rows["snp_product"], rows["snp_promotion"]))
            day += timedelta(days=every)
    finally:
        os.chdir(cwd)
    took = time.monotonic() - started
    print(f"{len(changes)} runs, every {every} day(s) from {first}: {took:.0f} s ({took / len(changes):.1f} s per run)")
    busy = [c for c in changes[1:] if c[1] or c[2]]
    print(f"  first run creates the tables: snp_product {changes[0][1]:,} rows, snp_promotion {changes[0][2]:,}")
    print(
        f"  later runs that merged anything: {len(busy)} of {len(changes) - 1} "
        f"({sum(c[1] for c in busy):,} product and {sum(c[2] for c in busy):,} promotion rows)"
    )
    return changes


HISTORY = {
    "snapshot rows": """
        SELECT 'snp_product', count(*), count(DISTINCT sku),
               count(*) FILTER (WHERE dbt_valid_to < TIMESTAMP '9999-12-31 UTC')
        FROM lakehouse_pipeline.silver.snp_product
        UNION ALL
        SELECT 'snp_promotion', count(*), count(DISTINCT promo_id),
               count(*) FILTER (WHERE dbt_valid_to < TIMESTAMP '9999-12-31 UTC')
        FROM lakehouse_pipeline.silver.snp_promotion""",
    "snapshot vs extracts": """
        WITH s AS (SELECT sku, CAST(dbt_valid_from AS date) AS valid_from FROM lakehouse_pipeline.silver.snp_product),
             m AS (SELECT sku, valid_from FROM lakehouse_pipeline.silver.int_product_versions)
        SELECT (SELECT count(*) FROM s), (SELECT count(*) FROM m),
               (SELECT count(*) FROM (SELECT * FROM s EXCEPT SELECT * FROM m)),
               (SELECT count(*) FROM (SELECT * FROM m EXCEPT SELECT * FROM s))""",
    "snapshot vs admin history": """
        WITH s AS (SELECT promo_id, version, dbt_valid_from AS version_from
                   FROM lakehouse_pipeline.silver.snp_promotion),
             m AS (SELECT promo_id, version, updated_at FROM lakehouse_pipeline.raw.promotion_versions)
        SELECT (SELECT count(*) FROM s), (SELECT count(*) FROM m),
               (SELECT count(*) FROM (SELECT * FROM s EXCEPT SELECT * FROM m)),
               (SELECT count(*) FROM (SELECT * FROM m EXCEPT SELECT * FROM s))""",
    "lost versions": """
        WITH v AS (
            SELECT promo_id, version, updated_at,
                   coalesce(lead(updated_at) OVER (PARTITION BY promo_id ORDER BY version),
                            TIMESTAMP '9999-12-31 UTC') AS replaced_at
            FROM lakehouse_pipeline.raw.promotion_versions
        )
        SELECT v.promo_id, v.version, CAST(v.updated_at AS varchar), CAST(v.replaced_at AS varchar),
               (SELECT count(*) FROM lakehouse_pipeline.silver.int_sales_lines l
                WHERE l.promo_id = v.promo_id AND l.event_ts >= v.updated_at AND l.event_ts < v.replaced_at)
        FROM v
        WHERE (v.promo_id, v.version) NOT IN (SELECT promo_id, version FROM lakehouse_pipeline.silver.snp_promotion)""",
    "one promotion": """
        SELECT promo_id, version, discount_pct, CAST(dbt_valid_from AS varchar), CAST(dbt_valid_to AS varchar)
        FROM lakehouse_pipeline.silver.snp_promotion WHERE promo_id = 'PR-0001' ORDER BY version""",
}


def history() -> dict[str, list]:
    out = {name: trino(sql) for name, sql in HISTORY.items()}
    print(f"{'snapshot':<14} {'rows':>6} {'keys':>6} {'closed':>7}")
    for name, rows, keys, closed in out["snapshot rows"]:
        print(f"{name:<14} {rows:>6,} {keys:>6,} {closed:>7,}")
    for label in ("snapshot vs extracts", "snapshot vs admin history"):
        a, b, only_a, only_b = out[label][0]
        print(f"{label:<26} {a:,} vs {b:,} versions: {only_a} only in the snapshot, {only_b} only in the source")
    for promo, version, start, end, lines in out["lost versions"]:
        print(f"never seen: {promo} v{version}, in force {start} to {end}, priced {lines:,} sale lines")
    print("PR-0001 in snp_promotion:")
    for _, version, pct, start, end in out["one promotion"]:
        print(f"  v{version}  {pct}%  dbt_valid_from {start}  dbt_valid_to {end}")
    return out


def statements(path: Path) -> list[str]:
    """SQL statements of a file, comments removed (the file's comments contain no semicolons)."""
    text = "\n".join(line for line in path.read_text().splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


FACTS = ["fct_sales_line", "fct_basket", "fct_refund", "fct_stock_daily", "fct_stock_adjustment", "fct_po_line"]
DIMS = ["dim_product", "dim_promotion", "dim_store", "dim_fx_rate"]
PART05 = ROOT / "data/lab/part05/larkspur.duckdb"


VERSION_USE = """
    SELECT p.sku, p.version, pr.promo_id, pr.version, count(*), sum(l.amount_minor)
    FROM {db}fct_sales_line l
    JOIN {db}dim_product p ON p.product_key = l.product_key
    JOIN {db}dim_promotion pr ON pr.promo_key = l.promo_key
    GROUP BY 1, 2, 3, 4"""


def check() -> dict:
    """Row counts against Part 5's DuckDB star, and Part 5's corrected numbers against the manifest."""
    m = json.loads((RAW / "_manifest.json").read_text())
    counts = {t: trino(f"SELECT count(*) FROM lakehouse_analyst.gold.{t}")[0][0] for t in FACTS + DIMS}
    part05 = {}
    if PART05.exists():
        con = duckdb.connect(str(PART05), read_only=True)
        part05 = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in FACTS + DIMS}
    print(f"{'table':<22} {'dbt on Trino':>13} {'Part 5 star':>12}")
    for t in FACTS + DIMS:
        print(
            f"{t:<22} {counts[t]:>13,} {part05.get(t, 0):>12,}  {'same' if counts[t] == part05.get(t) else 'DIFFERENT'}"
        )
    same = None
    if part05:  # which product and promotion version each sale line points at, counted per version
        lake = trino(VERSION_USE.format(db="lakehouse_analyst.gold."))
        star = con.execute(VERSION_USE.format(db="")).fetchall()
        same = sorted(map(str, map(tuple, lake))) == sorted(map(str, map(tuple, star)))
        print(f"sale lines per (product version, promotion version): {len(lake):,} groups, ", end="")
        print("same" if same else "DIFFERENT")
    revenue, stock, fill = (trino(q) for q in statements(ROOT / "sql/part17/check.sql"))
    uk = next(minor for country, minor in revenue if country == "GB")
    want = m["uk_net_revenue_q3"]["correct_minor"]
    s = m["stock_availability_q3"]
    verdict = "exact" if uk == want else "OFF"
    print(f"\nUK net revenue, Q3, GBP       {uk / 100:>14,.2f}   manifest {want / 100:,.2f}   {verdict}")
    print(f"stock available, as reported  {stock[0][0]:>14.6f}   manifest {s['per_day_reported_snapshot']:.6f}")
    print(f"stock available, at close     {stock[0][1]:>14.6f}   manifest {s['correct_close_of_day']:.6f}")
    clean = m["supplier_fill_q3"]
    print(
        f"supplier fill rate / OTIF     {fill[0][0]:>8.4f} / {fill[0][1]:.4f}   clean data {clean['fill_rate_q3']} / ",
        end="",
    )
    print(clean["otif_q3"])
    return {
        "counts": counts,
        "part05": part05,
        "versions_same": same,
        "uk_minor": uk,
        "stock": stock[0],
        "fill": fill[0],
    }


def builds(table: str = "gold.fct_sales_line") -> list:
    """Each `dbt build` replaced the table in one commit; its Iceberg snapshots are the builds (time travel)."""
    rows = trino(f"""
        SELECT snapshot_id, CAST(committed_at AS varchar), operation, summary['total-records']
        FROM lakehouse_analyst.{table.split(".")[0]}."{table.split(".")[1]}$snapshots" ORDER BY committed_at""")
    for snapshot_id, committed, operation, total in rows:
        print(f"{snapshot_id:>20}  {committed}  {operation:<8} {int(total):>11,} rows")
    first = rows[0][0]
    n = trino(f"SELECT count(*) FROM lakehouse_analyst.{table} FOR VERSION AS OF {first}")[0][0]
    print(f"SELECT count(*) … FOR VERSION AS OF {first}: {n:,}")
    return rows


def unit_demo() -> str:
    """Delete the line that keeps only accepted events (Part 21; until then, the line that dropped re-sent baskets'
    later copies) and run the unit test: it must fail. The file is restored after."""
    model = PROJECT / "models/staging/stg_pos_sale_lines.sql"
    original = model.read_text()
    line = "    where disposition = 'accepted'\n"
    assert line in original
    try:
        model.write_text(original.replace(line, ""))
        run = dbt("test", "--select", "stg_pos_sale_lines,test_type:unit", echo=False)
    finally:
        model.write_text(original)
    out = re.sub(r"\x1b\[[0-9;]*m", "", run.stdout)
    print(out)
    return out


SUBSET = ROOT / "dbt/larkspur_subset"


EXTRACT_TABLES = ["stores", "fx_rates", "catalog_products"]  # the inputs of the `portable` models


def extract(storage_version: str | None = None) -> None:
    """Copy a few raw lake tables into a local DuckDB file, schema `raw`: dbt-oss's DuckDB driver can't load httpfs
    or iceberg, so it can't read the lake; it reads this file."""
    LAB.mkdir(parents=True, exist_ok=True)
    EXTRACT.unlink(missing_ok=True)
    cat = cat9.pyiceberg("pipeline")
    con = duckdb.connect()
    option = f" (STORAGE_VERSION '{storage_version}')" if storage_version else ""
    con.execute(f"ATTACH '{EXTRACT}' AS extract{option}")
    con.execute("CREATE SCHEMA extract.raw")
    for name in EXTRACT_TABLES:
        data = cat.load_table(f"raw.{name}").scan().to_arrow()
        con.register("t", data)
        con.execute(f"CREATE TABLE extract.raw.{name} AS SELECT * FROM t")
        con.unregister("t")
        print(f"extract.raw.{name:<17} {data.num_rows:>9,} rows")
    con.execute("DETACH extract")
    print(
        f"{EXTRACT.relative_to(ROOT)}: {EXTRACT.stat().st_size / 2**20:.1f} MiB, written by DuckDB {duckdb.__version__}"
        + (f", storage version {storage_version}" if storage_version else "")
    )


def timing(repeats: int = 3) -> dict[str, list[float]]:
    """Cold parse and full build of the subset, v1 (dbt-core + dbt-duckdb) vs v2 (dbt-oss), on the same file."""
    runs = {
        "v1 start-up (dbt --help)": (False, SUBSET, ["--help"]),
        "v2 start-up (dbt --help)": (True, SUBSET, ["--help"]),
        "v1 parse (33 models, Trino adapter)": (False, PROJECT, ["parse", "--no-partial-parse"]),
        "v1 parse (subset)": (False, SUBSET, ["parse", "--no-partial-parse"]),
        "v2 parse (subset)": (True, SUBSET, ["parse"]),
        "v1 build (subset)": (False, SUBSET, ["build"]),
        "v2 build (subset)": (True, SUBSET, ["build"]),
    }
    out = {}
    for label, (v2, project, args) in runs.items():
        times = []
        for _ in range(repeats):
            started = time.monotonic()
            run = dbt(*args, v2=v2, project=project, echo=False)
            times.append(time.monotonic() - started)
            if run.returncode:
                print(run.stdout[-2000:])
                raise SystemExit(f"{label}: exit {run.returncode}")
        out[label] = times
        print(
            f"{label:<38} "
            + "  ".join(f"{t:5.2f}s" for t in times)
            + f"   median {sorted(times)[len(times) // 2]:.2f}s"
        )
    return out


def strict() -> dict[str, subprocess.CompletedProcess]:
    """A copy of the subset with one misspelt config key (British `materialised`), parsed by v1 and v2; then v1-only
    flags handed to v2. The copy lives in a temporary directory; the real files aren't touched."""
    import shutil
    import tempfile

    out = {}
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / "larkspur_subset"
        shutil.copytree(SUBSET, copy, symlinks=False, ignore=shutil.ignore_patterns("target", "logs"))
        profile = (
            (copy / "profiles.yml").read_text().replace("../../data/lab/part17/extract.duckdb", f"{tmp}/strict.duckdb")
        )
        (copy / "profiles.yml").write_text(profile)
        project = (copy / "dbt_project.yml").read_text()
        (copy / "dbt_project.yml").write_text(
            project.replace("+materialized: table", "+materialized: table\n    +materialised: view")
        )
        for label, v2, args in [
            ("v1 parse, with `+materialised: view`", False, ["parse", "--no-partial-parse"]),
            ("v2 parse, with `+materialised: view`", True, ["parse"]),
        ]:
            out[label] = dbt(*args, v2=v2, project=copy, echo=False)
        (copy / "dbt_project.yml").write_text(project)
        for label, args in [("v2 parse --no-partial-parse", ["parse", "--no-partial-parse"]),
                            ("v2 run -m dim_date", ["run", "-m", "dim_date"])]:  # fmt: skip
            out[label] = dbt(*args, v2=True, project=copy, echo=False)
    for label, run in out.items():
        text = re.sub(r"\x1b\[[0-9;]*m", "", run.stdout + run.stderr)
        errors = [line.strip() for line in text.splitlines() if "[error]" in line]
        warnings = [line.strip() for line in text.splitlines() if "warn" in line.lower() or "dbt1700" in line]
        note = errors[0][:110] if errors else (warnings[0][:110] if warnings else "no error, no warning")
        print(f"{label:<40} exit {run.returncode}  {note}")
    return out


def peak(command: list[str]) -> None:
    """Run a command while sampling the Trino container's memory; print its peak."""
    stop, peaks = threading.Event(), [0.0]
    sampler = threading.Thread(target=container_peak, args=("every-record-counts-trino", stop, peaks), daemon=True)
    sampler.start()
    try:
        subprocess.run(command, cwd=ROOT, check=True)
    finally:
        stop.set()
        sampler.join()
    print(f"Trino container peak: {max(peaks):,.0f} MiB")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command")
    p.add_argument("args", nargs=argparse.REMAINDER)
    a = p.parse_args()
    if a.command == "setup":
        setup()
    elif a.command == "land":
        land(a.args or None)
    elif a.command in ("dbt", "v2", "subset", "subset-v2"):
        project = SUBSET if a.command.startswith("subset") else PROJECT
        sys.exit(dbt(*a.args, v2=a.command.endswith("v2"), project=project).returncode)
    elif a.command == "replay":
        replay(every=int(a.args[a.args.index("--every") + 1]) if "--every" in a.args else 1)
    elif a.command == "history":
        history()
    elif a.command == "check":
        check()
    elif a.command == "builds":
        builds()
    elif a.command == "unit-demo":
        unit_demo()
    elif a.command == "extract":
        extract(a.args[0] if a.args else None)
    elif a.command == "strict":
        strict()
    elif a.command == "timing":
        timing()
    elif a.command == "peak":
        peak(a.args)
    else:
        raise SystemExit(f"unknown command {a.command}")
