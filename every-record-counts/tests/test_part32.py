"""Part 32: a retrieval index that follows the source through CDC, re-embedding only what changed, and features that
are correct as of the moment of prediction."""

import socket
import urllib.request

import pytest

from scripts import part32_models as p32
from tests.conftest import stack_up


def up(*ports: int) -> bool:
    for port in ports:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return True


def embeddings_up() -> bool:
    try:
        with urllib.request.urlopen(f"{p32.EMBEDDINGS_URL}/models", timeout=2):
            return True
    except OSError:
        return False


needs_cdc_and_model = pytest.mark.skipif(
    not (stack_up() and up(9092, 8083) and embeddings_up()),
    reason="needs `make up-stream` (core, Kafka, Connect) and a local embeddings server (EMBEDDINGS_URL)",
)
needs_lake = pytest.mark.skipif(not stack_up(), reason="needs `make up-core` and Part 16's raw.orders")


# ---------- no stack needed ----------


def test_personal_data_in_free_text_is_redacted_before_embedding():
    text, n = p32.redact("Call me on +44 7700 900123 or e-mail olivia.smith.42@example.com, order OGB00000042.")
    assert n == 2 and text == "Call me on [phone] or e-mail [email], order OGB00000042."


def test_a_ticket_is_one_chunk_per_message_each_with_the_documents_header():
    row = {"ticket_id": "TGB000001", "issue": "arrived_warm", "country": "GB", "subject": "Chilled items arrived warm",
           "body": "Customer: It was warm. Reach me on +44 7700 900001.\n\nAgent: Refunded."}  # fmt: skip
    chunks, n = p32.chunks_of("tickets", row)
    assert n == 1 and len(chunks) == 2
    assert chunks[0] == "Ticket TGB000001 | arrived warm | GB | Chilled items arrived warm\nCustomer: It was warm. " \
                        "Reach me on [phone]."  # fmt: skip
    assert chunks[1].endswith("\nAgent: Refunded.")


def test_the_generator_is_deterministic_and_its_eval_targets_are_small():
    from datagen import support
    from scripts.part23_privacy import customer_row

    a, b = support.build(customer_row), support.build(customer_row)
    assert a.tickets.equals(b.tickets) and a.reviews.equals(b.reviews) and a.eval_queries.equals(b.eval_queries)
    assert (a.tickets.height, a.reviews.height, a.eval_queries.height) == (4_000, 3_000, 60)
    assert a.eval_queries["expected_ids"].list.len().max() <= 3


def ev(op: str, doc: str, lsn: int, table: str = "tickets") -> tuple[str, dict]:
    key = p32.KEY[table]
    row = {key: doc}
    return f"larkspur.cx.support.{table}", {"op": op, "after": None if op == "d" else row,
                                          "before": row if op == "d" else None, "source": {"lsn": lsn}}  # fmt: skip


@pytest.mark.parametrize(
    "ops, kept",
    [
        (["u", "u"], "u@2"),  # two edits: the second wins
        (["r", "u"], "u@2"),  # snapshot row and an edit in one batch
        (["c", "d"], "d@2"),  # created and deleted
        (["u", "d"], "d@2"),
        (["d", "c"], "c@2"),  # deleted, then a new row with the same key
    ],
)
def test_only_the_last_event_per_document_in_a_batch_is_applied(ops, kept):
    from collections import Counter

    stats = Counter()
    out = p32.latest_per_document([ev(op, "T1", i + 1) for i, op in enumerate(ops)], stats)
    assert [f"{e['op']}@{e['source']['lsn']}" for _, e in out] == [kept]
    assert stats["superseded in the same batch"] == 1 and sum(stats[f"events {o}"] for o in set(ops)) == 2


def test_interleaved_documents_keep_their_own_last_event_in_order():
    from collections import Counter

    stats = Counter()
    batch = [ev("u", "T1", 1), ev("u", "R1", 2, "reviews"), ev("u", "T1", 3), ev("d", "T2", 4),
             ev("u", "R1", 5, "reviews")]  # fmt: skip
    out = p32.latest_per_document(batch, stats)
    kept = [(t.rsplit(".", 1)[1], e["source"]["lsn"]) for t, e in out]
    assert kept == [("tickets", 3), ("tickets", 4), ("reviews", 5)]
    assert stats["superseded in the same batch"] == 2 and stats["events u"] == 4 and stats["events d"] == 1


# ---------- the index, through Debezium and Kafka ----------


@pytest.fixture(scope="module")
def indexed():
    """A fresh lab state: source tables, connector, initial index (about 5 minutes of embedding on an M3 Pro)."""
    p32.reset()
    p32.setup()
    p32.connect()
    return p32.index(False, 5.0, "test-initial")


@needs_cdc_and_model
def test_the_initial_snapshot_embeds_every_chunk_once(indexed):
    assert indexed["events r"] == 7_000 and indexed["documents (re)indexed"] == 7_000
    assert indexed["chunks embedded"] == 15_006 and indexed["chunks re-used"] == 0


@needs_cdc_and_model
def test_a_replay_of_unchanged_rows_costs_no_embeddings(indexed):
    replay = p32.index(True, 5.0, "test-replay")
    assert replay["events r"] == 7_000 and replay["documents unchanged"] == 7_000
    assert replay["embedding requests"] == 0 and replay["chunks embedded"] == 0


@needs_cdc_and_model
def test_edits_re_embed_only_changed_chunks_and_deletes_remove_them(indexed):
    c = p32.change()
    s = p32.index(False, 5.0, "test-changes")
    assert s["documents unchanged"] == c["swept"]  # status-only updates: metadata, no embedding
    assert s[f"  {c['appended']}: embedded 1, re-used 2"] == 1  # one new message
    assert s[f"  {c['renamed']}: embedded 4, re-used 0"] == 1  # the subject is in every chunk's header
    assert s["documents deleted"] == 2 and s["chunks deleted"] == 5 and s["chunks embedded"] == 5
    gone = p32.erased()
    assert (gone["source"], gone["documents"], gone["chunks"], gone["tombstones"]) == (0, 0, 0, 2)
    assert gone["kafka"] == {"r": 2}  # the snapshot events still carry the text until retention deletes them
    again = p32.index(True, 5.0, "test-replay-after")  # old versions in the topic must not win, deletes stay deleted
    old = again["skipped: older than the index"] + again["superseded in the same batch"]
    assert again["embedding requests"] == 0 and old == 2 + 2 + c["swept"]
    assert p32.erased()["chunks"] == 0


@needs_cdc_and_model
def test_two_edits_in_one_batch_apply_the_last_and_embed_once(indexed):
    """Both updates reach Kafka before the indexer runs, so they arrive in one batch: the first is superseded."""
    import time

    import psycopg

    skip = {t for ids in p32.eval_set().column("expected_ids").to_pylist() for t in ids}
    skip |= {p32.json.loads((p32.OUT / "change.json").read_text())[k] for k in ("appended", "renamed")}
    with psycopg.connect(p32.PG, autocommit=True) as con:
        rows = con.execute("SELECT ticket_id, body FROM support.tickets ORDER BY ticket_id").fetchall()
        tid, body = next((t, b) for t, b in rows if t not in skip)
        before = sum(p32.end_offsets().values())
        for k in (1, 2):  # two transactions, two change events
            con.execute("UPDATE support.tickets SET body = body || %s WHERE ticket_id = %s",
                        (f"\n\nAgent: Follow-up {k}.", tid))  # fmt: skip
    deadline = time.monotonic() + 60
    while sum(p32.end_offsets().values()) < before + 2:
        assert time.monotonic() < deadline, "Debezium didn't publish both updates"
        time.sleep(1)
    s = p32.index(False, 5.0, "test-two-edits")
    assert s["events u"] == 2 and s["superseded in the same batch"] == 1
    assert s[f"  {tid}: embedded 2, re-used {body.count(chr(10) * 2) + 1}"] == 1


@needs_cdc_and_model
def test_retrieval_quality_and_the_filtered_search_shortfall(indexed):
    run = p32.evaluate()
    assert run["recall_hybrid"] >= run["recall_vector"] > run["recall_keyword"]
    assert run["recall_vector"] > 0.75 and run["ann_overlap_at_10"] >= 0.9
    f = p32.filtered()
    assert sum(c < 10 for c in f["off"]) > 30 and min(f["relaxed_order"]) == 10 and min(f["strict_order"]) == 10


@needs_cdc_and_model
def test_the_lakehouse_copy_finds_the_same_neighbours(indexed):
    out = p32.export()
    assert out["same"] == 60 and out["rows"] >= 15_000


@needs_cdc_and_model
def test_the_erased_customer_never_reaches_the_text_tables(indexed):
    import psycopg

    with psycopg.connect(p32.PG) as con:
        n = sum(con.execute(f"SELECT count(*) FROM support.{t} WHERE customer_id = 'CGB0000113'").fetchone()[0]
                for t in ("tickets", "reviews"))  # fmt: skip
    assert n == 0


# ---------- features ----------


@needs_lake
def test_a_naive_join_leaks_the_future_and_inflates_the_metric():
    out = p32.features()
    m = out["metrics"]
    naive, event, avail, popular = (m[k][2] for k in m)
    assert naive > popular > avail and naive - avail > 0.3
    assert abs(event - avail) < 0.01
    assert out["skew"][1] == 0 and out["skew"][0] > 0  # serving matches availability-time features only
