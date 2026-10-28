"""Part 10: what the post claims about partitioning, file layout and table maintenance.

The module fixture resets the part10 namespace and runs the whole sequence in Spark (needs `make up-spark`, about
seven minutes): three layouts of ~72.6 million POS lines, the two queries against each, a Z-order rewrite, 41
micro-batches and a merge-on-read delete, compaction, snapshot expiry and orphan-file removal.
"""

import subprocess

import duckdb
import pytest

from scripts import part10_layout as lab
from tests.conftest import ROOT, needs_spark

pytestmark = needs_spark
STAGED = ROOT / "data/lab/part07/pos_lines"


def measured(out: str) -> dict[tuple[str, str], dict]:
    """Parse part10_measure.py's table: (table, query) -> its columns."""
    rows = {}
    for line in out.splitlines():
        cells = line.split()
        if len(cells) == 9 and cells[0].startswith("pos_"):
            files, skipped, deletes, mib, rows_read = (float(c.replace(",", "")) for c in cells[2:7])
            rows[(cells[0], cells[1])] = {
                "files": files,
                "skipped": skipped,
                "deletes": deletes,
                "mib": mib,
                "rows": rows_read,
                "digest": cells[8],
            }
    return rows


def table_rows(out: str, header: str) -> list[list[str]]:
    """The rows of the run_sql.py table whose header line starts with `header`."""
    lines = out.splitlines()
    start = next(i for i, line in enumerate(lines) if line.split()[:1] == [header])
    rows = []
    for line in lines[start + 2 :]:
        if not line.strip() or line.startswith("("):
            break
        rows.append(line.split())
    return rows


@pytest.fixture(scope="module")
def run() -> dict:
    if not STAGED.exists():
        stage = ["uv", "run", "python", "scripts/duck.py", "--local", "sql/part07/stage.sql"]
        subprocess.run(stage, cwd=ROOT, check=True)
    lab.reset()
    out = {}
    for key, step in [
        ("build", lambda: lab.spark(["01_build.sql", "02_layouts.sql"], strict=True, capture=True)),
        ("measure", lambda: lab.job("measure", ["pos_flat", "pos_daily", "pos_monthly"], capture=True)),
        ("zorder", lambda: lab.spark(["03_zorder.sql"], strict=True, capture=True)),
        ("measure_z", lambda: lab.job("measure", ["pos_flat"], capture=True)),
        ("micro", lambda: lab.job("microbatches", capture=True)),
        ("october_before", lambda: lab.spark(["05_october.sql"], strict=True, capture=True)),
        ("query_before", lambda: lab.job("measure", ["pos_monthly", "--queries", "october"], capture=True)),
        ("compact", lambda: lab.spark(["06_compact.sql", "05_october.sql"], strict=True, capture=True)),
        ("query_after", lambda: lab.job("measure", ["pos_monthly", "--queries", "october"], capture=True)),
        ("expire", lambda: lab.spark(["07_expire.sql"], strict=True, capture=True)),
    ]:
        code, text = step()
        assert code == 0, key
        out[key] = text
    out["objects_expired"] = lab.objects()
    out["stray"] = lab.crash()
    out["orphans_sql"] = lab.spark(["08_orphans.sql"], capture=True)[1]
    out["orphans"] = lab.job("orphans", capture=True)[1]
    out["objects_clean"] = lab.objects()
    return out


def test_history_is_twelve_shifted_copies(run):
    lines, oct4 = duckdb.sql(
        f"SELECT count(*), count(*) FILTER (WHERE business_date = DATE '2026-10-04') FROM "
        f"(SELECT DISTINCT basket_id, line_no, business_date FROM read_parquet('{STAGED}/*/*.parquet'))"
    ).fetchall()[0]
    rows, first, last, days = table_rows(run["build"], "rows")[0]
    assert int(rows) == 12 * lines - oct4 == 72_620_578
    assert (first, last, int(days)) == ("2022-08-15", "2026-10-03", 1511)


def test_daily_partitions_make_small_files(run):
    layouts = {r[0] + " " + r[1]: r for r in table_rows(run["build"], "layout")}
    daily, monthly = layouts["B daily"], layouts["C monthly,"]
    assert int(daily[2]) == int(daily[3]) == 1511  # one file per day
    assert float(daily[5]) < 1.0  # under 1 MiB on average, against an 8 MiB target
    assert int(monthly[3]) == 51 and int(monthly[4]) < int(daily[3]) / 5


def test_layouts_answer_alike_and_read_differently(run):
    m = measured(run["measure"])
    for query in ("revenue", "store_promo"):
        assert len({m[(t, query)]["digest"] for t in ("pos_flat", "pos_daily", "pos_monthly")}) == 1
    assert m[("pos_flat", "revenue")]["skipped"] == 0  # unpartitioned and unsorted: every file
    assert m[("pos_daily", "revenue")]["files"] == 30  # September: one file per day
    assert m[("pos_monthly", "revenue")]["files"] <= 5
    assert m[("pos_monthly", "store_promo")]["rows"] * 10 < m[("pos_daily", "store_promo")]["rows"]
    assert m[("pos_daily", "store_promo")]["files"] > 300  # a day partition can't skip a store


def test_zorder_makes_both_queries_skip(run):
    before, after = measured(run["measure"]), measured(run["measure_z"])
    for query in ("revenue", "store_promo"):
        assert after[("pos_flat", query)]["digest"] == before[("pos_flat", query)]["digest"]
        assert after[("pos_flat", query)]["rows"] < before[("pos_flat", query)]["rows"]
    assert after[("pos_flat", "revenue")]["skipped"] > 0


def test_micro_batches_then_compaction(run):
    assert "micro-batch 41:" in run["micro"]
    data_before, deletes_before = table_rows(run["october_before"], "content")
    assert int(data_before[1]) == 42 and int(deletes_before[1]) > 0
    q_before, q_after = measured(run["query_before"]), measured(run["query_after"])
    key = ("pos_monthly", "october")
    assert q_before[key]["deletes"] > 0 and q_after[key]["deletes"] == 0
    assert q_before[key]["digest"] == q_after[key]["digest"]  # compaction changes files, not answers
    assert q_after[key]["files"] == 1
    rewritten = table_rows(run["compact"], "rewritten_data_files_count")[0]
    assert int(rewritten[0]) == 42 and int(rewritten[4]) == int(deletes_before[1])
    after = table_rows(run["compact"], "content")
    assert [r[:2] for r in after] == [["0", "1"]]


def test_expiry_deletes_what_only_old_snapshots_used(run):
    deleted = table_rows(run["expire"], "deleted_data_files_count")[0]
    assert int(deleted[0]) == 42 and int(deleted[1]) > 0
    objects = run["objects_expired"]
    assert "delete files (Parquet)" not in objects and objects["manifest lists (Avro)"] == 1


def test_orphans_need_the_action_for_short_intervals(run):
    assert "Cannot remove orphan files with an interval less than 24 hours" in run["orphans_sql"]
    assert run["stray"].endswith(lab.STRAY) and lab.STRAY in run["orphans"]
    assert "deleted 1 orphan file(s)" in run["orphans"]
    expired, clean = run["objects_expired"], run["objects_clean"]
    assert clean["data files (Parquet)"] == expired["data files (Parquet)"]
