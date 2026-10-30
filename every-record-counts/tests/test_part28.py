"""Part 28: streaming SQL. Dedup, an as-of price join, a window and an updating aggregate into Iceberg, Top-N."""

import socket

import pytest

from scripts import part28_flink as p28


def up(*ports: int) -> bool:
    for port in ports:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return True


# core (S3, Polaris), Kafka, Apicurio, Flink
needs_flink_lake = pytest.mark.skipif(not up(8333, 8181, 9092, 8085, 8081), reason="needs `make up-flink-lake`")


@pytest.fixture(scope="module")
def job():
    """The post's sequence from empty tables: setup, the price producer, the job with mini-batch, then without."""
    p28.cmd_setup(None)
    p28.p27.java("prices")
    batched = p28.run(mini_batch=True)
    batched_tables = p28.summaries()
    plain = p28.run()
    return {"batched": batched, "batched_tables": batched_tables, "plain": plain, "tables": p28.summaries()}


@needs_flink_lake
def test_the_bounded_job_finishes_with_an_end_of_input_commit(job):
    assert job["plain"]["state"] == "FINISHED"
    end_of_input = str(2**63 - 1)  # Iceberg's committer marks the commit at the end of a bounded input with Long.MAX
    for ident in p28.TABLES:
        last = p28.p27.catalog().load_table(ident).current_snapshot().summary.additional_properties
        assert last["flink.max-committed-checkpoint-id"] == end_of_input, ident
    lines, hourly, daily = (job["tables"][t] for t in p28.TABLES)
    assert lines["records written"] == 58_119 and lines["equality deletes"] == 0
    assert hourly["equality deletes"] == 0
    assert daily["identifier"] == "business_date,store_id"
    # Every line updated a store-day and wrote a row. Rows replaced within a commit get position deletes; each key
    # gets one equality delete per commit, for any row of it from an earlier commit.
    assert daily["records written"] == 58_119
    if daily["snapshots"] == 1:  # a warm cluster finishes the replay before the first 10 s checkpoint
        assert daily["equality deletes"] == 775 == daily["records written"] - daily["position deletes"]
    else:
        assert daily["equality deletes"] > 775


@needs_flink_lake
def test_mini_batch_writes_fewer_updates_for_the_same_answer(job):
    batched, plain = job["batched_tables"]["stream.store_sales_daily"], job["tables"]["stream.store_sales_daily"]
    assert batched["records written"] < plain["records written"] / 2
    assert batched["equality deletes"] >= 775


@needs_flink_lake
def test_what_each_query_emits(job):
    kinds = {label: (op, emits) for label, op, emits in p28.explain()}
    assert kinds["dedup, first copy by arrival"] == ("Deduplicate FirstRow on PROCTIME", "I")
    assert kinds["dedup, first copy by event time"] == ("Deduplicate FirstRow on ROWTIME", "I")
    assert kinds["versioned view (latest price per key)"][1] == "I,UA,D"
    assert kinds["temporal join (price as of the sale)"] == ("TemporalJoin", "I")
    assert kinds["tumbling window, 1 hour"][1] == "I"
    assert kinds["running total per store and day"] == ("GroupAggregate", "I,UA")
    assert kinds["running total into the Iceberg upsert table"][1] == "I,UB,UA"
    assert kinds["top 3 stores per country"] == ("Rank", "I,UA,D")


@needs_flink_lake
def test_each_line_gets_the_price_in_force_when_it_sold(job):
    r = p28.asof()
    assert r["lines"] == r["priced"] == r["agree"] == 58_119
    versions = [(f"{v:%Y-%m-%d %H:%M}", price, n) for v, price, n, *_ in r["versions"]]
    assert versions == [("2026-07-04 16:00", 1609, 7), ("2026-08-15 05:00", 1409, 22)]  # 05:00 UTC = 13:00 SGT
    assert r["latest_wrong"] == 2_338  # the same lines joined to each SKU's newest price instead


@needs_flink_lake
def test_the_stream_matches_the_batch_answer_or_the_difference_is_explained(job):
    r = p28.reconcile()
    assert (r["store_days"], r["equal"]) == (775, 774)
    assert r["differ"] == [("GB-0044", p28.date(2026, 8, 14), 4, 3, 1_528)]
    # the one basket the stream counted and Part 21 did not: a re-send whose first copy came the day before
    assert r["extra"] == [("GB-0044-20260814-00045", "GB-0044", 1_528, "resend", p28.date(2026, 8, 14))]
    assert r["gold"][:2] == (600, 22_260) and r["stream_15"][:2] == (600, 21_981)
    assert [(str(d), n) for d, n, _ in r["other_uploads"]] == [("2026-08-14", 85), ("2026-08-16", 194)]
    # upload days are UTC: Singapore and India mornings (and a London night under BST) belong to the previous UTC day
    early = {k: n for d, k, n in r["other_by_country"] if str(d) == "2026-08-14"}
    assert early == {"SG": 64, "IN": 20, "GB": 1}
    assert r["gold_stores_equal"] == 600


@needs_flink_lake
def test_the_hourly_window_drops_only_baskets_behind_the_watermark(job):
    r = p28.reconcile()
    (hb, hl, _), (lb, ll, _) = r["hourly"], r["lines"]
    assert (lb, ll) == (22_197, 58_119)
    assert lb - hb == 84 and ll - hl == 207  # the same in every run of this replay so far
    assert r["late_baskets"] == 586
    assert r["covered_by_late"] == r["windows_short"]  # every gap is late lines, never on-time ones
    assert r["mid_lag"] == 0


@needs_flink_lake
def test_top_n_emits_inserts_and_deletes_and_the_upsert_topic_keeps_nine_rows(job):
    r = p28.changelog()
    assert set(r["top_ops"]) == {"+I", "-D"}
    assert r["top_ops"]["+I"] - r["top_ops"]["-D"] == 9
    assert "9 keys alive at the end" in r["upserts"]
    for store in ("GB-0009", "GB-0141", "GB-0198", "IN-0058", "IN-0176", "IN-0268", "SG-0017", "SG-0057", "SG-0094"):
        assert store in r["upserts"]
    ops = [row[0] for row in r["store"]]
    assert ops[0] == "+I" and set(ops[1:]) == {"-U", "+U"}


@needs_flink_lake
def test_the_drill_distinct_count_over_ten_minutes_is_an_append_only_over_window():
    out = p28.flink_sql("drill_over.sql")
    assert "COUNT(DISTINCT till_id)" in out and "RANGE BETWEEN 600000 PRECEDING AND CURRENT ROW" in out
    assert "OverAggregate(" in out and "changelogMode=[I])" in out


@needs_flink_lake
def test_dedup_on_event_time_drops_what_arrives_behind_the_watermark(job):
    kept = p28.dedup()
    assert kept["arrival"] == 22_197  # 22,276 basket events, 79 re-sends within the day
    assert kept["arrival"] - kept["event time"] == 84


@needs_flink_lake
def test_flink_refuses_the_wrong_sink_before_any_data_moves():
    r = p28.errors()
    assert "doesn't support consuming update changes" in r["append"]
    assert "'upsert-kafka' tables require to define a PRIMARY KEY constraint" in r["nokey"]
    assert 'Encountered ". stream"' in r["stream"]
