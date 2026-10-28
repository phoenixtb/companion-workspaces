"""Part 12: what the post claims about the PySpark silver/gold job, ANSI mode, and the hot-store join.

The module fixture drops Part 12's tables, runs the pipeline twice (the second run must replace, not append), the
ANSI demo, builds the campaign table and runs the five join variants (needs `make up-spark` and Part 10's
part10.pos_monthly; about five minutes). Everything is rebuilt each time, so the tests can be re-run.
"""

import re

import pytest

from scripts import part12_spark as lab
from tests.conftest import needs_spark

pytestmark = needs_spark
HOT_LINES = 7_000_000  # GB-0001 carries ~7.5 million of the ~25 million campaign lines


def variants(out: str) -> dict[str, dict]:
    """Parse part12_skew.py's output: per variant its time, totals, final plan lines and stage rows."""
    found: dict[str, dict] = {}
    current = None
    for line in out.splitlines():
        m = re.match(r"variant (\w+): ([\d.]+) s .*?, ([\d,]+) rows, ([\d,]+) lines, checksum (-?[\d,]+)", line)
        if m:
            current = found[m[1]] = {
                "seconds": float(m[2]),
                "rows": int(m[3].replace(",", "")),
                "lines": int(m[4].replace(",", "")),
                "checksum": int(m[5].replace(",", "")),
                "plan": [],
                "stages": [],
            }
        elif current and re.match(r"\s*\d+\s+\d+\s+[\d.]+\s+[\d.]+\s", line):
            c = line.split()
            current["stages"].append(
                {
                    "tasks": int(c[1]),
                    "median_s": float(c[2]),
                    "max_s": float(c[3]),
                    "read_mib": float(c[4]),
                    "max_task_rows": int(c[6].replace(",", "")),
                    "write_mib": float(c[7]),
                    "spill_mem_mib": float(c[8]),
                    "spill_disk_mib": float(c[9]),
                }
            )
        elif current and line.startswith("  "):
            current["plan"].append(line.strip())
    for v in found.values():  # the join stage is the one that reads the lines' shuffle (or, broadcast, the scan)
        v["join"] = max(v["stages"], key=lambda s: (s["read_mib"], s["tasks"]))
    return found


@pytest.fixture(scope="module")
def run() -> dict:
    from scripts import part07_iceberg as p7

    if not p7.catalog().table_exists("part10.pos_monthly"):
        pytest.skip("needs Part 10's tables: run tests/test_part10.py with `make up-spark` first")
    lab.reset()
    out = {}
    for key, job, args in [
        ("pipeline", "part12_pipeline.py", []),
        ("pipeline_again", "part12_pipeline.py", []),
        ("ansi", "run_sql.py", ["/opt/lakehouse/sql/part12/ansi.sql"]),
        ("build", "part12_skew.py", ["build"]),
        ("skew", "part12_skew.py", ["run", *lab.VARIANTS]),
    ]:
        code, text, peak = lab.submit(job, args, capture=True)
        assert code == 0, key
        out[key], out[f"{key}_peak"] = text, peak
    out["variants"] = variants(out["skew"])
    return out


def table_counts(out: str) -> dict[str, tuple[int, str]]:
    counts = {}
    for line in out.splitlines():
        m = re.match(r"(\S+\.\S+)\s+([\d,]+) rows  snapshots: (.*)", line)
        if m:
            counts[m[1]] = (int(m[2].replace(",", "")), m[3])
    return counts


def test_pipeline_writes_silver_and_gold_and_reruns_replace(run):
    first, again = table_counts(run["pipeline"]), table_counts(run["pipeline_again"])
    assert first["silver.dim_store"] == (600, "append")
    assert first["silver.sales_line"][0] == again["silver.sales_line"][0] > 4_000_000
    assert first["gold.daily_sales_by_format"][0] == again["gold.daily_sales_by_format"][0]
    for table in first:  # the second run replaced its partitions (or the table) instead of appending
        assert again[table][1] == "append, overwrite", table


def test_gold_counts_every_silver_line_once(run):
    from scripts import part07_iceberg as p7

    cat = p7.catalog()
    silver = cat.load_table("silver.sales_line").scan(selected_fields=("amount_minor",)).to_arrow()
    gold = cat.load_table("gold.daily_sales_by_format").scan(selected_fields=("lines", "sales_minor")).to_arrow()
    assert sum(gold["lines"].to_pylist()) == silver.num_rows
    assert sum(gold["sales_minor"].to_pylist()) == sum(silver["amount_minor"].to_pylist())


def test_pipeline_plan_broadcasts_the_store_dimension_and_is_lazy(run):
    out = run["pipeline"]
    assert "BroadcastHashJoin Inner BuildRight" in out and "SortMergeJoin" not in out
    assert "no data read yet" in out
    assert "partial_count(distinct basket_id" in out  # the distinct count adds a second aggregation shuffle


def test_ansi_mode_fails_bad_values_instead_of_nulls(run):
    out = run["ansi"]
    assert "spark.sql.ansi.enabled   true" in out
    assert "CAST_INVALID_INPUT" in out and "ARITHMETIC_OVERFLOW" in out
    assert "NULL  -9223372036854775808" in out  # ANSI off: a NULL and a silently wrapped sum


def test_one_store_carries_thirty_percent(run):
    m = re.search(r"\|\s*GB-0001\|\s*(\d+)\|\s*([\d.]+)\|", run["build"])
    assert m and int(m[1]) > HOT_LINES and 29.9 <= float(m[2]) <= 30.1


def test_every_variant_gives_the_same_answer(run):
    v = run["variants"]
    assert set(v) == set(lab.VARIANTS)
    assert len({(x["rows"], x["lines"], x["checksum"]) for x in v.values()}) == 1
    assert v["merge"]["lines"] > 24_000_000


def test_broadcast_shuffles_almost_nothing(run):
    v = run["variants"]
    assert any("BroadcastHashJoin" in line for line in v["broadcast"]["plan"])
    assert not any("SortMergeJoin" in line for line in v["broadcast"]["plan"])
    broadcast_written = sum(s["write_mib"] for s in v["broadcast"]["stages"])
    merge_written = sum(s["write_mib"] for s in v["merge"]["stages"])
    assert broadcast_written * 10 < merge_written  # lines aggregated before the shuffle, not shuffled to join


def test_sort_merge_join_puts_the_hot_store_in_one_task_and_spills(run):
    join = run["variants"]["merge"]["join"]
    assert any("SortMergeJoin Inner" in line for line in run["variants"]["merge"]["plan"])
    assert join["tasks"] == 200 and join["max_task_rows"] > HOT_LINES
    assert join["max_s"] > 10 * max(join["median_s"], 0.01)
    assert join["spill_disk_mib"] > 0 and join["spill_mem_mib"] > join["spill_disk_mib"]


def test_aqe_defaults_ignore_a_hot_partition_under_256_mb(run):
    v = run["variants"]["aqe_default"]
    assert not any("skew=true" in line for line in v["plan"])
    assert v["join"]["max_task_rows"] > HOT_LINES and v["join"]["spill_disk_mib"] > 0


def test_aqe_skew_join_splits_the_hot_partition(run):
    v, merge = run["variants"]["aqe_scaled"], run["variants"]["merge"]["join"]
    assert any("SortMergeJoin(skew=true)" in line for line in v["plan"])
    assert any("[skewed]" in line for line in v["plan"])
    assert v["join"]["tasks"] > 200 and v["join"]["max_task_rows"] < HOT_LINES / 5
    assert v["join"]["spill_disk_mib"] == 0 and v["join"]["max_s"] < merge["max_s"] / 3


def test_salting_spreads_the_hot_store(run):
    v, merge = run["variants"]["salted"], run["variants"]["merge"]["join"]
    assert v["join"]["max_task_rows"] < HOT_LINES / 5
    assert v["join"]["spill_disk_mib"] == 0 and v["join"]["max_s"] < merge["max_s"] / 3


def test_spark_stays_inside_its_container_limit(run):
    assert max(run[k] for k in run if k.endswith("_peak")) < 2560
