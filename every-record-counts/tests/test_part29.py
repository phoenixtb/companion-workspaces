"""Part 29: a stateful Flink job in Java. Exactly-once through a crash, an upgrade from a savepoint, lateness."""

import socket

import pytest

from scripts import part29_stock as p29


def up(*ports: int) -> bool:
    for port in ports:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return True


raw = (p29.ROOT / "data/raw/inventory/snapshots").exists()
needs_raw = pytest.mark.skipif(not raw, reason="needs `make data` (the raw zone in data/raw)")
# core (S3, Polaris), Kafka, Apicurio, Flink, plus the raw zone for the batch answer
needs_flink_lake = pytest.mark.skipif(
    not (raw and up(8333, 8181, 9092, 8085, 8081)), reason="needs `make up-flink-lake` and `make data`"
)


@pytest.fixture(scope="module")
def expected():
    return p29.expected()


@pytest.fixture(scope="module")
def lab():
    """Setup and both jars, once: every scenario below resets the tables, topics and TaskManager itself."""
    p29.cmd_setup(None)
    p29.cmd_build(None)


@needs_raw
def test_opening_stock_follows_part_5s_snapshot_rule():
    c = p29.raw_con()
    keys, units, at_zero = c.execute(
        "SELECT count(*), sum(opening_qty), count(*) FILTER (WHERE opening_qty <= 0) FROM opening_stock"
    ).fetchone()
    assert (keys, units, at_zero) == (121_450, 466_728, 6_386)


@needs_raw
def test_the_batch_rule(expected):
    kinds = {k: sum(1 for a in expected["alerts"] if a[2] == k) for k in ("STOCK_OUT", "BOOK_ERROR")}
    assert kinds == {"STOCK_OUT": 6_779, "BOOK_ERROR": 3_054}
    assert expected["lines"] == 56_429 and expected["keys"] == 37_897
    assert len(expected["late"]) == 1_141  # 430 baskets of 15 August uploaded two hours or more after the sale
    # one alert of each kind per store-SKU at most
    assert len({a[:3] for a in expected["alerts"]}) == len(expected["alerts"])


@needs_flink_lake
def test_a_killed_taskmanager_leaves_the_lake_exact_and_the_at_least_once_topic_duplicated(lab, expected):
    r = p29.run(kill_at=0.4)
    assert r["state"] == "FINISHED" and r["restarts"] >= 1
    assert any("no longer reachable" in f for f in r["failures"])
    assert any("restored from checkpoint" in line for line in r["kill"])
    got = p29.check(expected)
    assert got["lake rows"] == got["expected"] == 9_833
    assert got["lake duplicates"] == got["missing"] == got["unexpected"] == 0
    assert got["late equal"]
    ids = [c for c, _ in got["commits"]]
    assert ids == sorted(set(ids))  # one commit per checkpoint, none twice
    eos = got[f"{p29.TOPIC_EOS} read_committed"]
    alo = got[f"{p29.TOPIC_ALO} read_committed"]
    dirty = got[f"{p29.TOPIC_EOS} read_uncommitted"]
    assert eos["records"] == eos["distinct"] == 9_833 and eos["duplicates"] == 0
    assert eos["missing"] == eos["unexpected"] == 0  # row by row against the batch answer
    assert alo["missing"] == alo["unexpected"] == 0
    assert alo["distinct"] == 9_833 and alo["duplicates"] > 0  # re-emitted after the restore
    assert dirty["records"] > 9_833  # aborted transactions are visible to read_uncommitted
    # the throttle is the bottleneck: busy, while the source is back-pressured
    bp = {row["task"]: row for row in r["backpressure"]}
    assert bp["Throttle (lab)"]["busy"] > 800
    assert bp["Source: POS basket events"]["backpressured"] > 800


@needs_flink_lake
def test_upgrade_from_a_savepoint_evolves_state_and_rescales(lab, expected):
    r = p29.upgrade(0.5)
    assert r["v2 state"] == "FINISHED" and r["v2 parallelism"] == 2
    got = p29.check(expected)
    assert got["lake rows"] == 9_833 and got["missing"] == got["unexpected"] == got["lake duplicates"] == 0
    assert got["low stock"] > 0 and set(got["versions"]) == {1, 2}
    assert got["late equal"]


@needs_flink_lake
def test_without_uids_the_savepoint_is_refused_and_skipping_it_duplicates(lab, expected):
    r = p29.upgrade(0.5, uids=False)
    assert "Cannot map checkpoint/savepoint state" in r["refused"]
    got = p29.check(expected)
    assert got["missing"] == 0 and got["lake duplicates"] > 0  # v2 started from nothing and re-sent what v1 sent
    assert got["late rows"] > got["late expected"]


@needs_flink_lake
def test_without_uids_on_either_side_the_same_graph_restores(lab, expected):
    r = p29.upgrade(0.5, uids=False, same_topology=True)
    assert r["v2 state"] == "FINISHED"
    got = p29.check(expected)
    assert got["lake rows"] == 9_833 and got["missing"] == got["unexpected"] == got["lake duplicates"] == 0


@needs_flink_lake
def test_lateness_by_watermark_depends_on_reading_order(lab, expected):
    one_mb = p29.run(None, throttle_ms=0, fetch_bytes=1_048_576)
    assert one_mb["state"] == "FINISHED"
    got = p29.check(expected)
    # the upload-delay test makes the side output exact; the watermark alone caught far fewer late lines
    assert got["late equal"] and got["missing"] == got["unexpected"] == 0
    assert got["late behind watermark"] < 0.5 * got["late expected"]
