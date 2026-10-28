"""Part 9: what the post claims about Polaris principals, roles, grants and credential vending.

The module fixture stages Part 8's gold file if needed, resets the Part 9 tables and roles, runs `setup` and
`load` (the pipeline principal writes raw.pos_events and gold.daily_store_sales). Trino and Spark checks run only
when those services are up.
"""

import subprocess

import duckdb
import pytest

from scripts import part09_catalog as lab
from tests.conftest import ROOT, needs_spark, needs_stack, needs_trino

pytestmark = needs_stack


@pytest.fixture(scope="module")
def loaded() -> dict:
    if not lab.GOLD_SOURCE.exists():
        stage = ["uv", "run", "python", "scripts/duck.py", "--local", "sql/part08/stage.sql"]
        subprocess.run(stage, cwd=ROOT, check=True)
    lab.reset()
    lab.setup()
    counts = lab.load()
    staged = duckdb.sql(f"SELECT count(*), sum(sales_minor)::BIGINT FROM '{lab.GOLD_SOURCE}'").fetchall()[0]
    return {"counts": counts, "staged": staged}


@pytest.fixture(scope="module")
def access(loaded) -> dict[str, str]:
    return lab.access()


def test_catalog_vends_credentials(loaded):
    _, cat = lab.call("GET", f"{lab.MGMT}/catalogs/{lab.CATALOG}")
    storage = cat["storageConfigInfo"]
    assert storage.get("stsUnavailable") is not True
    assert storage["roleArn"] == lab.VENDING["roleArn"]


def test_grants_as_designed(loaded):
    tok = lab.token()
    reader = lab.grants_by_securable("gold_reader", tok)
    assert reader == {"catalog": ["NAMESPACE_LIST"], "namespace gold": ["TABLE_LIST", "TABLE_READ_DATA"]}
    writer = lab.grants_by_securable("lake_writer", tok)
    assert set(writer) == {"catalog", "namespace gold", "namespace raw", "namespace silver"}
    assert all("TABLE_WRITE_DATA" in privs for where, privs in writer.items() if where != "catalog")


def test_pipeline_loaded_both_tables(loaded):
    raw_rows = duckdb.sql(
        f"SELECT count(*) FROM read_json('{ROOT}/data/raw/pos/events/upload_date={lab.RAW_DAY}/*.jsonl.gz')"
    ).fetchall()[0][0]
    assert loaded["counts"] == {"raw.pos_events": raw_rows, "gold.daily_store_sales": loaded["staged"][0]}


def test_host_engines_agree_with_the_staged_file(loaded):
    rows = {r[0]: r[3:] for r in lab.engines()}
    assert rows["PyIceberg 0.12"] == loaded["staged"]
    assert rows["DuckDB 2.0"] == loaded["staged"]


@needs_trino
def test_trino_as_analyst_agrees(loaded):
    rows, err = lab.trino(lab.GOLD_TOTALS.format(t="lakehouse_analyst.gold.daily_store_sales"))
    assert err is None
    assert tuple(rows[0]) == loaded["staged"]


@needs_spark
def test_spark_as_analyst_agrees(loaded):
    assert lab.spark_totals() == loaded["staged"]


def test_analyst_refused_raw_by_the_catalog(access):
    for engine in ["PyIceberg", "DuckDB"]:
        assert "Principal 'analyst'" in access[f"{engine} read raw"]
        assert "is not authorized for op LOAD_TABLE" in access[f"{engine} read raw"]
    assert "403" in access["DuckDB read raw"]
    assert access["PyIceberg read gold"] == "ok, 73,246 rows"


@needs_trino
def test_trino_analyst_refused_raw_and_write(access):
    assert "not authorized for op LOAD_TABLE_WITH_READ_DELEGATION" in access["Trino read raw"]
    assert "AccessDenied" in access["Trino write gold"]
    assert "raw" in access["Trino list schemas"]  # NAMESPACE_LIST shows names, not data


def test_analyst_writes_refused_by_storage(access, loaded):
    # Read-only vended credentials (PyIceberg) and a read-only static key (DuckDB) both stop at the PUT.
    assert "ACCESS_DENIED during CreateMultipartUpload" in access["PyIceberg write gold"]
    assert "HTTP 403" in access["DuckDB write gold"]
    snapshots = lab.pyiceberg("pipeline").load_table("gold.daily_store_sales").snapshots()
    assert len(snapshots) == 1


def test_vended_credentials_are_table_scoped(access):
    assert access["gold metadata, vended key"].startswith("ok")
    assert access["raw file, vended key"] == "denied: AWS Error ACCESS_DENIED during HeadObject operation"
    assert access["vended session policy"] == (
        "s3:GetBucketLocation, s3:GetObject, s3:GetObjectVersion, s3:ListBucket; "
        "objects larkspur-lake/warehouse/gold/daily_store_sales/*; list prefix warehouse/gold/daily_store_sales/*"
    )
    # A static key for the bucket ignores the catalog's grants entirely.
    assert access["raw files, static read key"].startswith("ok, ")


def test_catalog_stores_a_pointer(loaded):
    out = lab.pointer()
    assert out["location"] == out["rest"]
    assert out["location"].endswith(".metadata.json")
    assert out["row_bytes"] < 2_000
    assert sum(sum(sizes) for sizes in out["kinds"].values()) > 400_000


def test_load_table_carries_vended_keys_in_both_places(loaded):
    body = lab.load_table_answer()
    assert list(body) == ["metadata-location", "metadata", "config", "storage-credentials"]
    assert "s3.session-token" in body["config"]
    [cred] = body["storage-credentials"]
    assert cred["prefix"] == "s3://larkspur-lake/warehouse/gold/daily_store_sales"
    assert "s3.endpoint" not in body["config"]  # why DuckDB's vended mode falls back to AWS (duckdb-vended step)
