"""Part 7: what the post claims about Iceberg, checked against the tables Spark builds through Polaris.

The module fixture resets the part07 namespace and runs every Spark step (needs `make up-spark`, about two minutes).
It then checks the metadata tree as plain files, that schema and partition evolution rewrote nothing, time travel by
tag, branch isolation, what copy-on-write and merge-on-read MERGEs commit, the v3 table's deletion vector, the
cross-engine conflict and the Java API demo. `test_trino_reads` runs in a separate session with only Trino up.
"""

import json
import subprocess

import duckdb
import fastavro
import pyarrow.parquet as pq
import pytest

from scripts import part04_star as star
from scripts import part07_iceberg as lab
from tests.conftest import ROOT, needs_spark, needs_trino, trino_rows

STAGED = ROOT / "data/lab/part07/pos_lines"
UK_Q3 = "country = 'GB' AND business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'"


def staged(sql: str):
    """Query the staged POS lines (every event, re-sends included) with DuckDB."""
    con = duckdb.connect()
    con.execute(f"CREATE VIEW pos_lines AS FROM read_parquet('{STAGED}/*/*.parquet', hive_partitioning = true)")
    return con.execute(sql).fetchone()


@pytest.fixture(scope="module")
def built() -> dict:
    if not STAGED.exists():
        subprocess.run(
            ["uv", "run", "python", "scripts/duck.py", "--local", "sql/part07/stage.sql"], cwd=ROOT, check=True
        )
    lab.reset()
    files = [
        "01_load.sql",
        "02_evolve.sql",
        "03_late_lines.sql",
        "04_time_travel.sql",
        "05_merge_modes.sql",
        "06_v3.sql",
    ]
    code, out = lab.spark(files, capture=True)
    assert code == 0
    cat = lab.catalog()
    return {"out": out, "cat": cat, "sales": cat.load_table("part07.sales_line")}


@needs_spark
def test_load_keeps_one_row_per_basket_line(built):
    tag = built["sales"].snapshot_by_name("q3_close").snapshot_id
    loaded = built["sales"].scan(snapshot_id=tag, selected_fields=("basket_id",)).to_arrow().num_rows
    expected = staged(
        "SELECT count(*) FROM (SELECT DISTINCT basket_id, line_no FROM pos_lines WHERE upload_date <= '2026-09-30')"
    )[0]
    assert loaded == expected == 5_848_820


@needs_spark
def test_the_metadata_tree_is_plain_files(built):
    fs = star.fs()
    location = lab.load_table_response("sales_line")["metadata-location"]
    assert location == built["sales"].metadata_location  # the catalog's pointer is the table
    meta = json.loads(fs.cat(location))
    tag = meta["refs"]["q3_close"]["snapshot-id"]
    snap = next(s for s in meta["snapshots"] if s["snapshot-id"] == tag)
    with fs.open(snap["manifest-list"], "rb") as f:
        manifests = list(fastavro.reader(f))
    assert [m["added_files_count"] for m in manifests] == [123]
    with fs.open(manifests[0]["manifest_path"], "rb") as f:
        entries = list(fastavro.reader(f))
    assert len(entries) == 123 and sum(e["data_file"]["record_count"] for e in entries) == 5_848_820
    with fs.open(entries[0]["data_file"]["file_path"], "rb") as f:
        footer = pq.ParquetFile(f).schema_arrow
    assert footer.field("basket_id").metadata[b"PARQUET:field_id"] == b"1"  # files carry column IDs, not just names


@needs_spark
def test_schema_and_partition_evolution_rewrite_nothing(built):
    t = built["sales"]
    old = t.schemas()[0]
    assert old.find_field("amount_minor").field_id == t.schema().find_field("net_amount_minor").field_id == 12
    files = {f["file_path"]: f["spec_id"] for f in t.inspect.files().to_pylist()}
    tag = t.snapshot_by_name("q3_close").snapshot_id
    tagged = {f["file_path"] for f in t.inspect.files(snapshot_id=tag).to_pylist()}
    assert tagged <= files.keys()  # every file of the tagged snapshot is still live: none was rewritten
    assert sorted(set(files.values())) == [0, 1] and list(files.values()).count(0) == 123
    assert [f.name for f in t.spec().fields] == ["event_ts_day", "country"]


@needs_spark
def test_late_lines_are_inserted_once_and_change_closed_q3(built):
    new_keys = staged(
        """SELECT count(*) FROM (SELECT DISTINCT basket_id, line_no FROM pos_lines WHERE upload_date = '2026-10-01'
           EXCEPT SELECT DISTINCT basket_id, line_no FROM pos_lines WHERE upload_date <= '2026-09-30')"""
    )[0]
    assert "      1           6    46384" in built["out"]
    late = built["cat"].load_table("part07.sales_line").inspect.files().to_pylist()
    on_30_sep = [
        f["record_count"] for f in late if f["spec_id"] == 1 and str(f["partition"]["event_ts_day"]) == "2026-09-30"
    ]
    assert sum(on_30_sep) == 329 and len(on_30_sep) == 3  # three countries' late baskets
    assert new_keys == 46_384
    q3_close, now = staged(f"""
        WITH lines AS (SELECT *, row_number() OVER (PARTITION BY basket_id, line_no ORDER BY uploaded_at) AS n
                       FROM pos_lines WHERE upload_date <= '2026-10-01')
        SELECT sum(amount_minor) FILTER (WHERE upload_date <= '2026-09-30'), sum(amount_minor)
        FROM lines WHERE n = 1 AND {UK_Q3}""")
    assert f"tag q3_close  1508591      {q3_close}" in built["out"]
    assert f"        main  1508774      {now}" in built["out"]


@needs_spark
def test_snapshot_schema_and_branch_isolation(built):
    assert "cannot be resolved" in built["out"].split("VERSION AS OF 'q3_close'\nERROR")[1].splitlines()[0]
    t = built["cat"].load_table("part07.sales_line")
    assert set(t.refs()) == {"main", "q3_close", "rohan_dev"}
    branch = t.scan(snapshot_id=t.snapshot_by_name("rohan_dev").snapshot_id, selected_fields=("country",)).to_arrow()
    assert "SG" not in set(branch["country"].to_pylist())
    assert t.scan(selected_fields=("basket_id",)).to_arrow().num_rows == 5_848_820 + 46_384  # main untouched


@needs_spark
def test_copy_on_write_rewrites_files_merge_on_read_adds_deletes(built):
    cat = built["cat"]
    summaries = {}
    for mode in ("cow", "mor"):
        t = cat.load_table(f"part07.daily_store_sales_{mode}")
        summaries[mode] = t.current_snapshot().summary
    cow, mor = summaries["cow"], summaries["mor"]
    assert int(cow["deleted-data-files"]) == 2 and cow.get("added-delete-files") is None
    assert mor.get("deleted-data-files") is None and int(mor["added-delete-files"]) == 2
    for s in (cow, mor):  # the same logical change: 159 store-days updated, 612 inserted
        assert (s["spark.merge-into.num-target-rows-updated"], s["spark.merge-into.num-target-rows-inserted"]) == (
            "159",
            "612",
        )
    assert int(cow["spark.merge-into.num-target-rows-copied"]) > 100 * int(mor["added-position-deletes"])
    rows = {}
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    for mode in ("cow", "mor"):
        rows[mode] = con.execute(
            f"SELECT count(*), sum(baskets), sum(sales_minor) FROM lake.part07.daily_store_sales_{mode}"
        ).fetchone()
    truth = con.execute("""
        SELECT count(*), sum(b), sum(s) FROM (
            SELECT store_id, business_date, count(DISTINCT basket_id) AS b, sum(net_amount_minor) AS s
            FROM lake.part07.sales_line GROUP BY ALL)""").fetchone()
    assert rows["cow"] == rows["mor"] == truth  # both MERGEs give the full rebuild's answer


@needs_spark
def test_v3_deletion_vector_and_engine_support(built):
    out = built["out"]
    assert "    v2        1      PARQUET          5176" in out
    assert "    v3        1       PUFFIN          5176" in out
    assert "setting default values in Spark is currently unsupported" in out
    t = built["cat"].load_table("part07.clicks_v2")
    assert t.format_version == 2 and t.scan().to_arrow().num_rows == 59_881 - 5_176
    with pytest.raises(Exception, match="variant"):
        built["cat"].load_table("part07.clicks_v3")  # PyIceberg 0.12 has no variant type
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute((ROOT / "sql/part01/polaris_attach.sql").read_text())
    assert con.execute("SELECT count(*) FROM lake.part07.clicks_v3").fetchone()[0] == 59_881 - 5_176


@needs_spark
def test_conflicting_writers(built, capfd, caplog):
    lab.conflict()
    out = capfd.readouterr()
    assert "retrying" in (out.err + caplog.text).lower()  # the catalog refused PyIceberg's first swap ...
    assert "ValidationException" in out.out  # ... and the retry found Spark's change to the same rows


@needs_spark
def test_java_commit_anatomy(built):
    run = subprocess.run(
        ["./gradlew", "-q", ":iceberg-api-demo:run"], cwd=ROOT, capture_output=True, text=True, timeout=600
    )
    assert run.returncode == 0, run.stderr[-2000:]
    lines = run.stdout.splitlines()
    commits, attempts = [int(w) for w in next(x for x in lines if x.startswith("2 writers")).split() if w.isdigit()][2:]
    assert commits == 20 and attempts >= commits
    assert any(x.startswith("C failed: ValidationException: Found conflicting files") for x in lines)
    assert "history: 21 snapshots" in lines


@needs_trino
def test_trino_reads():
    """Run with Spark stopped and Trino up, after the Spark tests built the tables."""
    tag = trino_rows(f"""SELECT count(*), sum(amount_minor)
                         FROM lakehouse.part07.sales_line FOR VERSION AS OF 'q3_close' WHERE {UK_Q3}""")
    assert tag == [[1_508_591, 2_782_694_870]]
    assert trino_rows("SELECT typeof(payload), count(*) FROM lakehouse.part07.clicks_v3 GROUP BY 1") == [
        ["variant", 59_881 - 5_176]
    ]
