"""Part 30: one stock metric at three latencies. Same answer everywhere; freshness bought with commits and parts."""

import socket

import pytest

from scripts import part30_tiers as p30


def up(*ports: int) -> bool:
    for port in ports:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return True


raw = (p30.ROOT / "data/raw/pos/events").exists()
needs_raw = pytest.mark.skipif(not raw, reason="needs `make data` (the raw zone in data/raw)")
# core (S3, Polaris), Kafka, Apicurio, Flink and ClickHouse: Part 30's reduced variant
needs_realtime = pytest.mark.skipif(
    not (raw and up(8333, 8181, 9092, 8085, 8081, 8123)), reason="needs `make up-realtime` and `make data`"
)
LIMIT = 1_500  # the first 1,500 uploads of the day, replayed in about a minute


@needs_raw
def test_the_batch_answer_for_the_whole_day():
    e = p30.expected()
    assert (e["events"], e["lines"], e["lines_with_copies"]) == (22_276, 58_119, 58_334)
    assert (e["store_skus"], e["at_or_below_zero"], e["units_sold"]) == (121_450, 13_436, 75_396)
    assert e["availability_pct"] == 88.94


@pytest.fixture(scope="module")
def run():
    """A short replay through every tier, 10-second commits, hourly runs every 20 s, compaction after 3 commits.
    Measurements go to data/lab/part30/test-*.json; the post's own files are left alone."""
    p30.PREFIX = "test-"
    p30.cmd_setup(None)
    r = p30.live(240, "10 s", 20, LIMIT, compacted=True, rewrite_after=3)
    yield r
    p30.PREFIX = ""


@needs_realtime
def test_every_tier_reaches_the_batch_answer(run):
    r = p30.check(LIMIT)
    assert r["equal"] == {"hours": True, "minutes": True, "seconds": True}
    lines = r["lines"]
    assert lines["minutes"] == lines["hours"] == lines["seconds_final"] == lines["expected"] == 3_879
    assert r["sold_outside_opening"] == 0
    rows = r["row_level"]  # every store-SKU's level, not just the totals: EXCEPT both ways against the batch
    assert rows["batch_rows"] == 121_450
    for tier in ("hours", "minutes", "seconds"):
        assert rows[tier] == {"rows": 121_450, "not_in_batch": 0, "missing_from_tier": 0}


@needs_realtime
def test_freshness_is_bought_by_the_flush_and_the_commit(run):
    f = p30.freshness()
    assert f["seconds"]["p50"] < 1  # a block every 250 ms
    assert f["seconds, default flush"]["p50"] > 1 and f["seconds, default flush"]["max"] < 7.5 + 2  # 7.5 s flush
    assert f["minutes"]["max"] < 10 + 5  # a commit every 10 s
    assert f["seconds"]["p50"] < f["minutes"]["p50"] < f["hours"]["p50"]


@needs_realtime
def test_clickhouse_keeps_copies_until_final(run):
    rows = p30.ch_rows("SELECT count() FROM realtime.sale_lines_default")[0][0]
    final = p30.ch_rows("SELECT count() FROM realtime.sale_lines_default FINAL")[0][0]
    assert final == 3_879 and rows >= final  # merges remove copies eventually; FINAL removes them now


@needs_realtime
def test_the_hourly_batch_resumes_from_the_offsets_it_committed(run):
    again = p30.hourly()  # nothing new on the topic: the offsets in the last snapshot say so
    assert again["events"] == 0 and again["new_lines"] == 0
    assert p30.table_rows(p30.LINES_1H) == 3_879


@needs_realtime
def test_compaction_rewrites_small_files_without_changing_rows(run):
    c = p30.compact()
    plain, compacted = c[p30.LINES_1M], c[p30.LINES_1M_C]
    assert compacted["rows"] == plain["rows"] == 3_879
    assert compacted["operations"].get("replace", 0) >= 1 and c["rewrites"]
    assert compacted["live_data_files"] < plain["live_data_files"]
    assert plain["operations"] == {"append": plain["commits"]}  # one append per checkpoint that saw data
