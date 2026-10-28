"""Part 11: what the post claims about DuckDB and Polars on one machine, over Part 10's Iceberg tables.

Needs `make up-core` and Part 10's tables (`uv run pytest tests/test_part10.py` with Spark up rebuilds them); the
module skips when they're missing. The Spark comparison runs only when Spark is up.
"""

import json
import os
import subprocess
import sys

import pytest

from scripts import part11_single_node as lab
from tests.conftest import ROOT, needs_spark, needs_stack

pytestmark = needs_stack


@pytest.fixture(scope="module")
def answers() -> dict:
    from scripts import part07_iceberg as p7

    tables = {ident[-1] for ident in p7.catalog().list_tables("part10")}
    if not {"pos_monthly", "pos_daily"} <= tables:
        pytest.skip("needs Part 10's tables: run tests/test_part10.py with `make up-spark` first")
    return {(e, q): lab.run(e, q, "pos_monthly", None, None) for e in ("duckdb", "polars") for q in lab.QUERIES}


def measure(engine: str, query: str, threads: int | None = None, memory_mb: int | None = None) -> dict:
    """lab.run in a fresh process, so the memory settings and peak RSS belong to this query alone."""
    env = dict(os.environ)
    cmd = [sys.executable, "-m", "scripts.part11_single_node", "run", engine, query]
    if threads:
        env["POLARS_MAX_THREADS"] = str(threads)
        cmd += ["--threads", str(threads)]
    if memory_mb:
        env["POLARS_OOC_MEMORY_BUDGET_MB"] = str(memory_mb)
        cmd += ["--memory-mb", str(memory_mb)]
    done = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=True)
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_duckdb_and_polars_give_the_same_answers(answers):
    for q in lab.QUERIES:
        assert answers[("duckdb", q)]["digest"] == answers[("polars", q)]["digest"], q
    assert answers[("duckdb", "history")]["rows"] == 153 and answers[("duckdb", "baskets")]["rows"] == 35
    assert answers[("duckdb", "baskets")]["baskets"] == answers[("polars", "baskets")]["baskets"] > 27_000_000


def test_duckdb_reads_only_the_columns_and_files_it_needs(answers):
    table_mib = sum(f.file.file_size_in_bytes for f in _monthly().scan().plan_files()) / 1048576
    history, revenue = answers[("duckdb", "history")], answers[("duckdb", "revenue")]
    assert history["fetched_mib"] < table_mib / 5  # five of thirteen columns, compressed
    assert revenue["fetched_mib"] < 5  # one month's files, four columns
    assert answers[("duckdb", "baskets")]["spilled_mib"] == 0  # 28.7 GiB default limit: nothing spills


def _monthly():
    from scripts import part07_iceberg as p7

    return p7.catalog().load_table("part10.pos_monthly")


def test_a_memory_limit_makes_duckdb_spill_not_fail(answers):
    limited = measure("duckdb", "baskets", threads=4, memory_mb=1500)
    assert limited["digest"] == answers[("duckdb", "baskets")]["digest"]
    assert limited["spilled_mib"] > 500 and limited["buffer_mib"] <= 1500
    assert limited["peak_rss_mib"] < answers[("duckdb", "baskets")]["peak_rss_mib"]


def test_a_memory_budget_makes_polars_spill(answers):
    free, budget = measure("polars", "baskets"), measure("polars", "baskets", threads=4, memory_mb=1500)
    assert free["digest"] == budget["digest"] == answers[("polars", "baskets")]["digest"]
    assert budget["peak_rss_mib"] < free["peak_rss_mib"] / 2


def test_polars_group_order_needs_maintain_order(answers):
    firsts = lab.order()
    with_order = firsts[3:]
    assert with_order[0] == with_order[1] == with_order[2] == ["GB-0001", "GB-0002", "GB-0003"]


def test_arrow_hand_off_and_duckdb_over_polars(answers):
    out = lab.arrow()
    assert out["rows"] == 1_434_587  # September's lines (Part 10's revenue query read the same rows)
    assert len(out["top"]) == 3


def test_duckdb_publishes_and_polars_reads(answers):
    rows = lab.publish()
    assert lab.digest(rows) == answers[("duckdb", "history")]["digest"]


def test_one_writer_process_per_duckdb_file():
    seen = lab.lock()
    assert all("Could not set lock on file" in line for line in seen)


def test_duckdb_2_files_need_a_storage_version_for_1_5():
    default, pinned = lab.oldfile()
    assert "version number" in default
    assert pinned.endswith("(42,)")


@needs_spark
def test_spark_gives_the_same_answers(answers):
    for r in lab.spark("pos_monthly", lab.QUERIES):
        assert r["digest"] == answers[("duckdb", r["query"])]["digest"], r["query"]
