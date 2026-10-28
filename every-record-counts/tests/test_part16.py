"""Part 16: what the post claims about CDC from PostgreSQL into Iceberg.

The first test needs no containers: the replayer's history, applied in full, gives the 5 October extract. The rest
share one module fixture that runs the whole sequence from a reset (about four minutes with `make up-stream` and
`make up-spark`): schema and state at the cut-over, the connector's initial snapshot, the replay, a stream that
crashes after batch 3's commits, a restart, then reconcile, incident and maintenance. Everything is dropped and
rebuilt each time, so the tests can be re-run.
"""

import re
import subprocess
import sys

import polars as pl
import pytest

from scripts import part16_cdc as lab
from tests.conftest import ROOT, needs_cdc


def test_history_replays_to_the_extract():
    h = lab.history()
    for t in lab.TABLES:
        state = lab.state_at(h[t], lab.KEYS[t], lab.END).select(lab.COLS[t])
        extract = pl.read_parquet(ROOT / f"data/raw/oltp/{t}/*/*.parquet").select(lab.COLS[t])
        state, extract = (df.with_columns(pl.col(pl.Datetime).dt.cast_time_unit("us")) for df in (state, extract))
        extra = state.join(extract, on=lab.COLS[t], how="anti", nulls_equal=True)
        missing = extract.join(state, on=lab.COLS[t], how="anti", nulls_equal=True)
        assert missing.height == 0
        # The generator's extract leaves out orders cancelled *after* it ran (placed late on 4 October); at 01:00 UTC
        # on 5 October they still existed, so the replayed database has them.
        assert extra.height == (15 if t == "orders" else 0)
    assert h["orders"].filter(pl.col("op") == "d").height == 6_332  # of 6,347 cancellations, 15 after the extract


def cdc(*args: str) -> str:
    run = subprocess.run(
        [sys.executable, "-m", "scripts.part16_cdc", *args], cwd=ROOT, capture_output=True, text=True, timeout=1800
    )
    return run.stdout + f"\nexit {run.returncode}\n"


@pytest.fixture(scope="module")
def run() -> dict[str, str]:
    out = {}
    for step in ("reset", "setup", "connect", "replay", "topics"):
        out[step] = cdc(step)
    out["crash"] = cdc("stream", "--fail-after-batch", "3")
    out["restart"] = cdc("stream")
    for step in ("reconcile", "incident"):
        out[step] = cdc(step)
    out["order"] = cdc("order", "OGB00038256")
    out["maintain"] = cdc("maintain")
    out["reconcile_after"] = cdc("reconcile")
    return out


def num(text: str) -> int:
    return int(text.replace(",", ""))


@needs_cdc
def test_snapshot_then_stream_lands_every_change(run):
    assert "41,745 events for 41,745 rows" in run["connect"]
    assert "2,016,330 changes" in run["replay"]
    topics = {m[1]: num(m[2]) for m in re.finditer(r"larkspur\.shop\.(\w+)\s+([\d,]+) events", run["topics"])}
    # snapshot rows + every streamed change (orders: 41,745 + 140,453 + 135,333 + 4,837)
    assert topics == {"orders": 322_368, "payments": 2_160_409, "refunds": 63_780}


@needs_cdc
def test_a_replayed_batch_is_harmless(run):
    assert "exit 1" in run["crash"] and "simulated crash after batch 3" in run["crash"]
    restart = run["restart"]
    assert "exit 0" in restart
    replayed = restart.split("batch 3:")[1].split("batch 4:")[0]
    assert replayed.count("changes already committed, skipped") == 3
    assert replayed.count("+0 rows +0 position deletes") == 3


@needs_cdc
def test_lake_matches_postgres_row_for_row(run):
    for key in ("reconcile", "reconcile_after"):  # after streaming, and again after compaction
        rows = re.findall(
            r"│ (orders|payments|refunds)\s+│\s+(\d+) │\s+(\d+) │\s+(\d+) │\s+(\d+) │\s+(\d+) │", run[key]
        )
        assert len(rows) == 3
        for _, pg_rows, lake_rows, pg_minor, lake_minor, differing in rows:
            assert pg_rows == lake_rows and pg_minor == lake_minor and differing == "0"


@needs_cdc
def test_deleted_orders_close_revenue_step_2(run):
    out = run["incident"]
    assert re.search(r"4   of which the poller extracted\s+1,176\s+72\s+57,064\.96", out)
    assert re.search(r"board deck minus CDC replica\s+57,064\.96", out)  # docs/injected-defects.md, step 2
    order = run["order"]
    assert "legacy poller extracts: [(datetime.date(2026, 8, 2), 'placed', 2296, 'GBP')]" in order
    assert "PostgreSQL shop.orders: no row" in order and "raw.orders (current state): no row" in order
    assert re.search(r"op=c .*\n\s+op=d ", order)


@needs_cdc
def test_compaction_folds_the_delete_files(run):
    out = run["maintain"]
    assert "exit 0" in out
    tables = re.findall(r"raw\.(orders|payments)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)", out)
    before, after = tables[:2], tables[2:]
    assert all(int(t[2]) > 0 and t[4] == "0" for t in before)  # position deletes only, no equality deletes
    assert all(t[1:] == ("1", "0", "0", "0") for t in after)
