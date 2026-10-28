"""Part 8: what the post claims about Iceberg, Delta Lake and DuckLake, checked on the tables the lab builds.

The module fixture stages the gold table, resets the part08 tables, loads all four (Iceberg and DuckLake from DuckDB,
two Delta tables from Spark), applies the 20 late baskets and records what each commit wrote (needs `make up-spark`,
about two minutes). Flush and the interop checks run last, in file order.
"""

import json
import subprocess

import duckdb
import pytest

from scripts import part04_star as star
from scripts import part08_formats as lab
from tests.conftest import ROOT, needs_spark

LATE = 20 * 1999  # 20 late baskets of £19.99


@pytest.fixture(scope="module")
def built() -> dict:
    subprocess.run(["uv", "run", "python", "scripts/duck.py", "--local", "sql/part08/stage.sql"], cwd=ROOT, check=True)
    staged = duckdb.sql(f"SELECT count(*), sum(sales_minor)::BIGINT FROM '{lab.STAGED}'").fetchall()[0]
    lab.reset()
    lab.load()
    loaded = lab.objects(), lab.catalog_rows()
    return {"staged": staged, "loaded": loaded, **lab.late()}


def added(before: dict, after: dict, fmt: str) -> dict[str, int]:
    return {p: s for p, s in after[fmt].items() if p not in before[fmt]}


def kinds(files: dict[str, int]) -> list[str]:
    return sorted(lab.kind(p) for p in files)


def delta_commit(table: str, version: int) -> dict:
    path = f"{lab.PREFIX[table]}/_delta_log/{version:020d}.json"
    lines = [json.loads(line) for line in star.fs().cat(path).decode().splitlines()]
    return next(entry["commitInfo"] for entry in lines if "commitInfo" in entry)


@needs_spark
def test_staged_table(built):
    assert built["staged"] == (73_246, 322_704_113_319)  # sum of mixed-currency minor units: a checksum, not money


@needs_spark
def test_where_each_format_keeps_its_metadata(built):
    objs, rows = built["loaded"]
    assert {"Iceberg metadata (JSON)", "Iceberg manifest list (Avro)", "Iceberg manifest (Avro)"} <= set(
        kinds(objs["Iceberg"])
    )
    assert "Delta commit (JSON)" in kinds(objs["Delta (DVs)"])
    assert kinds(objs["DuckLake"]) == ["data (Parquet)"]  # nothing but data in object storage
    assert rows["ducklake_data_file"] == 1 and rows["ducklake_snapshot"] == 2  # the metadata is rows in PostgreSQL


@needs_spark
def test_one_late_basket(built):
    o0, o1, _ = built["objects"]
    c0, c1, _ = built["catalog"]
    assert kinds(added(o0, o1, "Iceberg")) == sorted(
        ["data (Parquet)", "delete file (Parquet)", "Iceberg metadata (JSON)", "Iceberg manifest list (Avro)"]
        + ["Iceberg manifest (Avro)"] * 2
    )
    assert "deletion vector (.bin)" in kinds(added(o0, o1, "Delta (DVs)"))
    assert added(o0, o1, "DuckLake") == {}  # data inlining: no file at all
    assert c1["ducklake_inlined_data_1_1"] - c0.get("ducklake_inlined_data_1_1", 0) == 1
    assert c1["ducklake_inlined_delete_1"] - c0.get("ducklake_inlined_delete_1", 0) == 1

    dv = delta_commit("Delta (DVs)", 2)["operationMetrics"]
    uniform = delta_commit("Delta (UniForm)", 2)["operationMetrics"]
    assert (dv["numCopiedRows"], dv["numDeletionVectorsAdded"]) == ("0", "1")
    assert (uniform["numCopiedRows"], uniform["numDeletionVectorsAdded"]) == ("73245", "0")  # copy-on-write


@needs_spark
def test_twenty_late_baskets(built):
    o0, _, o2 = built["objects"]
    assert len([p for p in added(o0, o2, "Iceberg") if lab.kind(p) == "data (Parquet)"]) == 20
    assert kinds(added(o0, o2, "Delta (DVs)")).count("Delta checkpoint (Parquet)") == 2  # versions 10 and 20
    assert added(o0, o2, "DuckLake") == {}
    uniform_data = [s for p, s in added(o0, o2, "Delta (UniForm)").items() if lab.kind(p) == "data (Parquet)"]
    assert len(uniform_data) == 20 and min(uniform_data) > 400_000  # every UPDATE rewrote the whole file


@needs_spark
def test_uniform_iceberg_metadata(built):
    meta = lab.uniform_metadata()
    assert [v for v, _ in meta] == list(range(1, 22))  # Delta versions 1..21: the insert and 20 updates
    fs = star.fs()
    for _, path in meta:
        snapshots = json.loads(fs.cat(path))["snapshots"]
        assert len(snapshots) == 1  # in this lab each conversion starts a fresh Iceberg table


@needs_spark
def test_every_reader_agrees(built):
    expected = (73_246, built["staged"][1] + LATE)
    rows = lab.interop()
    assert len(rows) == 5 and all((n, total) == expected for _, n, total in rows)


@needs_spark
def test_duckdb_cannot_update_delta(built):
    con = lab.duck()
    con.execute(f"ATTACH 's3://{lab.PREFIX['Delta (DVs)']}' AS delta_dv (TYPE delta)")
    with pytest.raises(duckdb.BinderException):
        con.execute(lab.late_basket_sql("delta_dv.main.delta_dv", "GB-0021"))


@needs_spark
def test_flush_moves_inlined_rows_to_parquet(built):
    before = len(lab.objects()["DuckLake"])
    assert lab.flush() == 0
    assert len(lab.objects()["DuckLake"]) == before + 2  # one data file, one delete file
    con = lab.duck()
    total = con.execute("SELECT sum(sales_minor) FROM dl.daily_store_sales").fetchall()[0][0]
    assert total == built["staged"][1] + LATE
