"""Part 27: the event backbone. Topics, a strict registry, transactional producers, the Iceberg sink, Flink."""

import json
import socket

import pytest

from scripts import part27_backbone as p27

POS, CLICKS = p27.TABLES[p27.POS_TOPIC], p27.TABLES[p27.CLICK_TOPIC]


def up(*ports: int) -> bool:
    for port in ports:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return True


# core (S3, Polaris), Kafka, Apicurio, Connect
needs_backbone = pytest.mark.skipif(not up(8333, 8181, 9092, 8085, 8083), reason="needs `make up-backbone`")
needs_flink_registry = pytest.mark.skipif(not up(9092, 8085, 8081), reason="needs Kafka, Apicurio and `make up-flink`")


def test_raw_zone_is_the_yardstick(lab_manifest):  # reads the raw zone: skipped without `make data`
    assert p27.raw_lines() == {POS: 22_276, CLICKS: 61_554 + 62_958}


def test_the_compatible_v2_only_adds_an_optional_field():
    v1, v2, rename = (
        json.loads((p27.ROOT / f"kafka/schemas/click_event_{v}.avsc").read_text()) for v in ("v1", "v2", "v2_rename")
    )
    names = [f["name"] for f in v1["fields"]]
    added = [f for f in v2["fields"] if f["name"] not in names]
    assert [f["name"] for f in added] == ["page_path"] and added[0]["default"] is None
    assert [f for f in v2["fields"] if f["name"] in names] == v1["fields"]  # nothing else changed
    assert "page" not in [f["name"] for f in rename["fields"]]


def produce() -> dict[str, str]:
    """The post's producer sequence on empty topics and an empty registry."""
    p27.cmd_reset(None)
    p27.cmd_topics(None)
    p27.cmd_registry(None)
    return {
        "pos": p27.java("pos", "--day", p27.POS_DAY, "--abort", "3"),
        "count": p27.java("count", "--topic", p27.POS_TOPIC),
        "v1": p27.java("clicks", "--day", "2026-08-14", "--schema", "v1"),
        "v2_rename": p27.java("clicks", "--day", "2026-08-15", "--schema", "v2_rename"),
        "v2": p27.java("clicks", "--day", "2026-08-15", "--schema", "v2"),
    }


@pytest.fixture(scope="module")
def produced():
    return produce()


@needs_backbone
def test_an_aborted_transaction_is_in_the_log_but_not_for_read_committed(produced):
    assert "transactions: 23 committed, 1 aborted (1,000 records written" in produced["pos"]
    out = produced["count"]
    assert "end offsets sum to 23,420" in out  # 23,276 records + 144 markers (24 transactions x 6 partitions)
    assert "read_uncommitted consumer reads 23,276 records" in out
    assert "read_committed   consumer reads 22,276 records" in out


@needs_backbone
def test_the_registry_refuses_the_rename_and_accepts_the_additive_change(produced):
    assert "61,554 of 61,554" in produced["v1"]
    assert "schema v2_rename refused" in produced["v2_rename"] and "Incompatible artifact" in produced["v2_rename"]
    assert "schema v2_rename: 0 of 62,958" in produced["v2_rename"]
    assert "62,958 of 62,958" in produced["v2"]
    _, versions = p27.http(
        "GET", f"{p27.REGISTRY}/registry/v3/groups/default/artifacts/{p27.CLICK_TOPIC}-value/versions"
    )
    assert len(versions["versions"]) == 2  # v1 and v2: the refused schema never became a version
    _, latest = p27.http("GET", f"{p27.REGISTRY}/ccompat/v7/subjects/{p27.CLICK_TOPIC}-value/versions/latest")
    assert latest["id"] == versions["versions"][-1]["contentId"]  # the Confluent-compatible ID is the contentId


@needs_backbone
def test_the_sink_lands_every_committed_record_and_evolves_the_table(produced):
    p27.cmd_sink(type("A", (), {"isolation": "read_committed"})())
    assert p27.lake_rows() == p27.raw_lines()
    clicks = p27.catalog().load_table(CLICKS)
    assert clicks.schema().find_field("page_path").optional
    pos = p27.catalog().load_table(POS)
    assert {f.name for f in pos.schema().fields} >= {"_kafka_metadata_partition", "_kafka_metadata_offset"}


@needs_flink_registry
def test_flink_reads_the_apicurio_framing_and_needs_read_committed():
    if not p27.http("GET", f"{p27.REGISTRY}/ccompat/v7/subjects")[1]:
        produce()  # an empty registry: the topics need the producers' records first
    out = p27.flink_rows()
    assert out["as declared above"] == (23_276, 22_276, 22_197)  # the aborted batch is read without the option
    assert out["read_committed"] == (22_276, 22_276, 22_197)
