"""Part 32: feeding the models. Support tickets and reviews become a retrieval index that follows the source through
change data capture, and online orders become a point-in-time feature table for recommendations.

uv run python -m scripts.part32_models setup           # support.* tables + generated text, the rag database, eval set
uv run python -m scripts.part32_models connect         # Debezium connector larkspur-support; wait for its snapshot
uv run python -m scripts.part32_models index [--from-beginning]   # the CDC consumer: redact, chunk, embed what changed
uv run python -m scripts.part32_models change          # a status sweep, two edits and an erasure in the source
uv run python -m scripts.part32_models erased          # where the erased customer's text still is
uv run python -m scripts.part32_models search "TEXT" [--country GB] [--how vector|keyword|hybrid]
uv run python -m scripts.part32_models filtered        # a country filter after an HNSW scan; iterative index scans
uv run python -m scripts.part32_models eval            # recall@5 on the tagged eval set; HNSW vs exact; run logged
uv run python -m scripts.part32_models batching        # one text embedded alone and in a batch
uv run python -m scripts.part32_models export          # chunks -> Iceberg ml.support_chunks (tagged); DuckDB search
uv run python -m scripts.part32_models features        # point-in-time feature table, training set, leakage
uv run python -m scripts.part32_models reset           # connector, slot, topics, support schema, rag database, ml.*

Needs `make up-core` and, for connect/index/change, `make up-stream` (Kafka, Connect). Embeddings come from a local
OpenAI-compatible server (EMBEDDINGS_URL, default LMForge on http://127.0.0.1:11430/v1; Ollama's /v1 should work,
not tested: set EMBEDDINGS_URL and EMBEDDING_MODEL).
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import urllib.request
import warnings
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import psycopg
import pyarrow as pa

from scripts import part09_catalog as cat9
from scripts import part16_cdc as p16
from scripts import part23_privacy as p23

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql/part32"
OUT = ROOT / "data/lab/part32"
PG = "host=localhost port=5432 dbname=larkspur user=larkspur password=larkspur"
RAG = "host=localhost port=5432 dbname=rag user=larkspur password=larkspur"
CONNECTOR, SLOT = "larkspur-support", "larkspur_support"
CONNECTOR_JSON = ROOT / "kafka/connectors/larkspur-support.json"
TOPICS = ["larkspur.cx.support.tickets", "larkspur.cx.support.reviews"]  # topic.prefix larkspur.cx: one per connector
KEY = {"tickets": "ticket_id", "reviews": "review_id"}
EMBEDDINGS_URL = os.environ.get("EMBEDDINGS_URL", "http://127.0.0.1:11430/v1")
MODEL = os.environ.get("EMBEDDING_MODEL", "qwen3-embed:0.6b:8bit")  # Qwen3-Embedding-0.6B (Apache 2.0), 1024 dims
CHUNKER = "message-v1"
# Qwen3-Embedding: queries carry a one-sentence task, documents don't (model card)
TASK = "Given a customer's description of a problem, retrieve support tickets about the same problem"
EVAL_TABLE, EVAL_TAG = "ml.rag_eval", "rag-eval-v1"
RUNS_TABLE = "ml.rag_eval_runs"
CHUNKS_TABLE, CHUNKS_TAG = "ml.support_chunks", "rag-corpus-v1"
FEATURES_TABLE, TRAINING_TABLE, TRAINING_TAG = "ml.customer_features", "ml.reco_training", "reco-train-v1"


# ---------- embeddings: a local OpenAI-compatible server ----------


class Embedder:
    """POST /v1/embeddings; counts requests, inputs and tokens so every run can say what it spent."""

    def __init__(self) -> None:
        self.requests = self.inputs = self.tokens = 0

    def __call__(self, texts: list[str]) -> list[list[float]]:
        body = json.dumps({"model": MODEL, "input": texts}).encode()
        req = urllib.request.Request(f"{EMBEDDINGS_URL}/embeddings", data=body,
                                     headers={"Content-Type": "application/json"})  # fmt: skip
        with urllib.request.urlopen(req, timeout=120) as r:
            out = json.load(r)
        self.requests += 1
        self.inputs += len(texts)
        self.tokens += out.get("usage", {}).get("prompt_tokens", 0)
        return [d["embedding"] for d in sorted(out["data"], key=lambda d: d["index"])]


def query_text(q: str, instruct: bool = True) -> str:
    return f"Instruct: {TASK}\nQuery:{q}" if instruct else q


def vec(v: list[float]) -> str:
    return "[" + ",".join(repr(x) for x in v) + "]"


# ---------- documents to chunks ----------

EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
PHONE = re.compile(r"\+\d{2}[\d ]{7,14}\d")


def redact(text: str) -> tuple[str, int]:
    """Personal data typed into free text (Part 23): e-mail addresses and phone numbers become placeholders."""
    text, a = EMAIL.subn("[email]", text)
    text, b = PHONE.subn("[phone]", text)
    return text, a + b


def chunks_of(table: str, row: dict) -> tuple[list[str], int]:
    """One chunk per message of a ticket thread, one per review. Each starts with a header line naming the document,
    so a chunk retrieved on its own still says what it belongs to."""
    if table == "tickets":
        subject, n = redact(row["subject"])
        header = f"Ticket {row['ticket_id']} | {row['issue'].replace('_', ' ')} | {row['country']} | {subject}"
        parts = row["body"].split("\n\n")
    else:
        title, n = redact(row["title"])
        header = f"Review {row['review_id']} | {row['rating']}/5 | {row['country']} | {title}"
        parts = [row["body"]]
    out = []
    for p in parts:
        text, k = redact(p)
        out.append(f"{header}\n{text}")
        n += k
    return out, n


def sha(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


# ---------- setup: the source tables, the index database, the evaluation set ----------


def run_sql(conninfo: str, path: Path) -> None:
    with psycopg.connect(conninfo, autocommit=True) as con:
        con.execute(path.read_text())


def setup() -> None:
    from datagen import support

    started = time.monotonic()
    s = support.build(p23.customer_row)
    run_sql(PG, SQL / "postgres/support.sql")
    loaded = {}
    for name, df in (("tickets", s.tickets), ("reviews", s.reviews)):
        data = p23.suppress(df.to_arrow(), delete={"customer_id": "customer_id"})  # Part 23: never load the erased
        cols = data.column_names
        with psycopg.connect(PG) as con, con.cursor().copy(f"COPY support.{name} ({', '.join(cols)}) FROM STDIN") as cp:
            for r in data.to_pylist():
                cp.write_row([r[c] for c in cols])
        loaded[name] = data
        print(f"support.{name}: suppression list (Part 23) dropped {df.height - data.num_rows} of {df.height:,} rows")
    print(f"support.tickets {loaded['tickets'].num_rows:,} rows, support.reviews {loaded['reviews'].num_rows:,} rows "
          f"(generated {s.tickets.height:,} + {s.reviews.height:,}, seed 42)")  # fmt: skip
    with psycopg.connect(PG, autocommit=True) as con:
        if not con.execute("SELECT 1 FROM pg_database WHERE datname = 'rag'").fetchone():
            con.execute("CREATE DATABASE rag")
    run_sql(RAG, SQL / "postgres/rag.sql")
    with psycopg.connect(RAG) as con:
        v = con.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'").fetchone()[0]
    print(f"database rag: vector {v}, tables documents, chunks (HNSW + GIN), consumer_offsets")
    # the evaluation set: queries and the tickets that answer them, kept as a tagged Iceberg snapshot
    kept = set(loaded["tickets"].column("ticket_id").to_pylist())
    ev = s.eval_queries.to_arrow()
    expected = [[t for t in ids if t in kept] for ids in ev.column("expected_ids").to_pylist()]
    column = pa.array(expected, pa.list_(pa.string()))
    ev = ev.set_column(ev.schema.get_field_index("expected_ids"), "expected_ids", column)
    snap = publish(EVAL_TABLE, ev, EVAL_TAG)
    print(f"{EVAL_TABLE}: {ev.num_rows} queries, {sum(map(len, expected))} expected tickets; tag {EVAL_TAG} -> {snap}")
    print(f"setup took {time.monotonic() - started:.0f} s")


def lake():
    cat = cat9.pyiceberg("root", vended=False)  # the lab writes ml.* as root; production gives it a principal (Part 9)
    if ("ml",) not in cat.list_namespaces():
        cat.create_namespace("ml", {"owner": "ml platform team"})
    return cat


def publish(name: str, data: pa.Table, tag: str | None) -> int:
    """Overwrite an Iceberg table with `data` and point `tag` at the new snapshot (moving it if it exists)."""
    cat = lake()
    t = cat.load_table(name) if cat.table_exists(name) else cat.create_table(name, schema=data.schema)
    with warnings.catch_warnings():  # overwriting an empty table warns that its delete matched nothing
        warnings.simplefilter("ignore", UserWarning)
        t.overwrite(data)
    snap = t.current_snapshot().snapshot_id
    if tag:
        with t.manage_snapshots() as ms:
            if tag in t.metadata.refs:
                ms.remove_tag(tag)
            ms.create_tag(snap, tag)
    return snap


# ---------- Kafka Connect ----------


def end_offsets() -> dict[str, int]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "part32-offsets"})
    try:
        md = c.list_topics(timeout=10).topics
        return {t: sum(c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1] for p in md[t].partitions)
                for t in TOPICS if t in md}  # fmt: skip
    finally:
        c.close()


def connect() -> None:
    config = json.loads(CONNECTOR_JSON.read_text())
    with psycopg.connect(PG) as con:
        expected = {f"larkspur.cx.support.{t}": con.execute(f"SELECT count(*) FROM support.{t}").fetchone()[0]
                    for t in KEY}  # fmt: skip
    started = time.monotonic()
    status, body = p16.http("PUT", f"/connectors/{CONNECTOR}/config", config)
    if status not in (200, 201):
        raise SystemExit(f"Connect said {status}: {body}")
    print(f"connector {CONNECTOR}: {'created' if status == 201 else 'updated'}, snapshot.mode=initial")
    while True:
        time.sleep(2)
        got = end_offsets()
        if all(got.get(t, 0) >= n for t, n in expected.items()):
            break
        if time.monotonic() - started > 600:
            raise SystemExit(f"snapshot not finished after 10 minutes: {got}")
    for t, n in expected.items():
        print(f"  {t:<26} {got[t]:>6,} events for {n:,} rows")
    print(f"initial snapshot in Kafka after {time.monotonic() - started:.0f} s")


# ---------- the indexer: Debezium events -> redact, chunk, embed what changed -> pgvector ----------


def doc_key(topic: str, event: dict) -> str:
    return (event["after"] or event["before"])[KEY[topic.rsplit(".", 1)[1]]]


def latest_per_document(batch: list[tuple[str, dict]], stats: Counter) -> list[tuple[str, dict]]:
    """Only each document's last event in a batch is applied; earlier ones are superseded (the last one carries the
    whole row, or the delete), so no version is embedded only to be replaced a moment later."""
    last = {doc_key(t, e): i for i, (t, e) in enumerate(batch)}
    out = []
    for i, (t, e) in enumerate(batch):
        stats[f"events {e['op']}"] += 1
        if last[doc_key(t, e)] == i:
            out.append((t, e))
        else:
            stats["superseded in the same batch"] += 1
    return out


def embed_batch(con: psycopg.Connection, batch: list[tuple[str, dict]], emb: Embedder) -> dict[str, str]:
    """Before applying a batch (one event per document): the chunks that will need a vector (new or changed text, not
    stale), embedded in requests of up to 64 inputs. Returns chunk hash -> vector."""
    docs = {}
    for topic, e in batch:
        if e["op"] != "d":
            table = topic.rsplit(".", 1)[1]
            docs[e["after"][KEY[table]]] = (table, e)
    known = dict(con.execute("SELECT doc_id, source_lsn FROM documents WHERE doc_id = ANY(%s)", (list(docs),)))
    have = {h for (h,) in con.execute("SELECT chunk_hash FROM chunks WHERE doc_id = ANY(%s)", (list(docs),))}
    need: dict[str, str] = {}
    for doc_id, (table, e) in docs.items():
        if known.get(doc_id, -1) > e["source"]["lsn"]:
            continue
        for t in chunks_of(table, e["after"])[0]:
            if sha(MODEL, t) not in have:
                need[sha(MODEL, t)] = t
    hashes, out = list(need), {}
    for i in range(0, len(hashes), 64):
        part = hashes[i : i + 64]
        out |= dict(zip(part, map(vec, emb([need[h] for h in part])), strict=True))
    return out


def apply(con: psycopg.Connection, topic: str, event: dict, fresh: dict[str, str], stats: Counter) -> None:
    table = topic.rsplit(".", 1)[1]
    op, src = event["op"], event["source"]
    lsn, ts = src["lsn"], datetime.fromtimestamp(src["ts_ms"] / 1000, UTC)
    row = event["after"] or event["before"]
    doc_id = row[KEY[table]]
    cur = con.execute("SELECT source_lsn, content_hash FROM documents WHERE doc_id = %s FOR UPDATE",
                      (doc_id,)).fetchone()  # fmt: skip
    if cur and cur[0] > lsn:  # an older version than the one indexed (a replay): never go back in time
        stats["skipped: older than the index"] += 1
        return
    if op == "d":  # drop the chunks; keep a tombstone with no personal data, so a replay can't resurrect them
        n = con.execute("DELETE FROM chunks WHERE doc_id = %s", (doc_id,)).rowcount
        con.execute(
            """INSERT INTO documents (doc_id, source, source_lsn, source_ts, indexed_at, deleted_at)
               VALUES (%s, %s, %s, %s, now(), now())
               ON CONFLICT (doc_id) DO UPDATE SET customer_id = NULL, country = NULL, sku = NULL, status = NULL,
                 content_hash = NULL, source_lsn = EXCLUDED.source_lsn, source_ts = EXCLUDED.source_ts,
                 indexed_at = now(), deleted_at = now()""",
            (doc_id, table[:-1], lsn, ts),
        )
        stats["documents deleted"] += 1
        stats["chunks deleted"] += n
        return
    texts, redactions = chunks_of(table, row)
    digest = sha(MODEL, CHUNKER, *texts)
    meta = (row["customer_id"], row["country"], row["sku"], row["status"], lsn, ts)
    if cur and cur[1] == digest:  # same text, same model, same chunker: metadata only, no embedding
        con.execute("""UPDATE documents SET customer_id = %s, country = %s, sku = %s, status = %s, source_lsn = %s,
                       source_ts = %s, indexed_at = now() WHERE doc_id = %s""", (*meta, doc_id))  # fmt: skip
        stats["documents unchanged"] += 1
        return
    old = dict(con.execute("SELECT chunk_hash, embedding::text FROM chunks WHERE doc_id = %s", (doc_id,)).fetchall())
    hashes = [sha(MODEL, t) for t in texts]
    need = [h for h in hashes if h not in old]  # embedded by embed_batch; everything else keeps its vector
    con.execute(
        """INSERT INTO documents (doc_id, source, customer_id, country, sku, status, content_hash, source_lsn,
                                  source_ts, indexed_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
           ON CONFLICT (doc_id) DO UPDATE SET customer_id = EXCLUDED.customer_id, country = EXCLUDED.country,
             sku = EXCLUDED.sku, status = EXCLUDED.status, content_hash = EXCLUDED.content_hash,
             source_lsn = EXCLUDED.source_lsn, source_ts = EXCLUDED.source_ts, indexed_at = now(), deleted_at = NULL""",
        (doc_id, table[:-1], *meta[:4], digest, lsn, ts),
    )
    con.execute("DELETE FROM chunks WHERE doc_id = %s", (doc_id,))
    with con.cursor() as c:
        c.executemany(
            """INSERT INTO chunks (doc_id, chunk_no, chunk_hash, country, text, embedding, model, embedded_at)
               VALUES (%s, %s, %s, %s, %s, %s::vector, %s, now())""",
            [(doc_id, i, h, row["country"], t, old[h] if h in old else fresh[h], MODEL)
             for i, (t, h) in enumerate(zip(texts, hashes, strict=True))],
        )  # fmt: skip
    stats["documents (re)indexed"] += 1
    stats[f"  {doc_id}: embedded {len(need)}, re-used {len(texts) - len(need)}"] += 1
    stats["chunks embedded"] += len(need)
    stats["chunks re-used"] += len(texts) - len(need)
    stats["redactions"] += redactions


def index(from_beginning: bool, idle: float, label: str) -> Counter:
    """Read both topics until caught up and quiet for `idle` seconds. Each batch of events, its chunks and the next
    offsets commit in one PostgreSQL transaction, so a crash re-reads at most the batch it was applying."""
    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

    stats, emb = Counter(), Embedder()
    stop, peaks = threading.Event(), {}
    threading.Thread(target=sample_memory, args=(stop, peaks), daemon=True).start()
    started = time.monotonic()
    consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "part32-indexer",
                         "enable.auto.commit": False, "enable.partition.eof": True})  # fmt: skip
    with psycopg.connect(RAG, autocommit=True) as con:  # each `con.transaction()` below is one real transaction
        stored = {} if from_beginning else {(t, p): o for t, p, o in con.execute("SELECT * FROM consumer_offsets")}
        md = consumer.list_topics(timeout=10).topics
        assigned = [TopicPartition(t, p, stored.get((t, p), OFFSET_BEGINNING))
                    for t in TOPICS for p in md[t].partitions]  # fmt: skip
        consumer.assign(assigned)
        at_end, quiet_since = set(), time.monotonic()
        while True:
            msgs = consumer.consume(500, 1.0)
            events = [m for m in msgs if not m.error()]
            eof = KafkaError._PARTITION_EOF
            reached = {(m.topic(), m.partition()) for m in msgs if m.error() and m.error().code() == eof}
            if events:
                batch = latest_per_document([(m.topic(), json.loads(m.value())) for m in events if m.value()], stats)
                with con.transaction():
                    fresh = embed_batch(con, batch, emb)
                    for topic, event in batch:
                        apply(con, topic, event, fresh, stats)
                    for m in events:
                        con.execute("""INSERT INTO consumer_offsets VALUES (%s, %s, %s) ON CONFLICT (topic, partition)
                                       DO UPDATE SET next_offset = EXCLUDED.next_offset""",
                                    (m.topic(), m.partition(), m.offset() + 1))  # fmt: skip
                at_end -= {(m.topic(), m.partition()) for m in events}
                quiet_since = time.monotonic()
            at_end |= reached  # partition EOF is reported once, after that partition's last message
            if not events and len(at_end) == len(assigned) and time.monotonic() - quiet_since > idle:
                break
        docs, live, chunks = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE deleted_at IS NULL), (SELECT count(*) FROM chunks) FROM documents"
        ).fetchone()
    consumer.close()
    stop.set()
    took = time.monotonic() - started - idle
    stats |= Counter({"embedding requests": emb.requests, "embedding tokens": emb.tokens})
    start = "from the beginning" if from_beginning or not stored else "from the stored offsets"
    print(f"{label}: {took:.0f} s ({start})")
    per_doc = sorted(k for k in stats if k.startswith("  "))
    for k in sorted(stats, key=lambda k: (not k.startswith("events"), k)):
        if not k.startswith("  "):
            print(f"  {k:<32} {stats[k]:>8,}")
        if k == "documents (re)indexed" and len(per_doc) <= 10:
            print("\n".join(f"  {d}" for d in per_doc))
    print(f"  index now: {live:,} documents ({docs - live:,} tombstones), {chunks:,} chunks")
    lab = {k: v for k, v in peaks.items() if k != "_total"}
    print(f"  lab containers peaked at {peaks.get('_total', 0) / 1024:.2f} GiB ("
          + ", ".join(f"{k.removeprefix('every-record-counts-').removesuffix('-1')} {v / 1024:.2f}"
                      for k, v in sorted(lab.items(), key=lambda kv: -kv[1])) + ")")  # fmt: skip
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"index-{label}.json").write_text(json.dumps({"stats": stats, "seconds": took, "peaks_mib": peaks}))
    return stats


def sample_memory(stop: threading.Event, peaks: dict) -> None:
    units = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}
    while not stop.is_set():
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"],
                             capture_output=True, text=True).stdout  # fmt: skip
        total = 0.0
        for line in out.splitlines():
            name, used = line.split()[:2]
            if name.startswith("every-record-counts-"):
                unit = next(u for u in ("GiB", "MiB", "KiB", "B") if used.endswith(u))
                mib = float(used[: -len(unit)]) * units[unit]
                peaks[name] = max(peaks.get(name, 0.0), mib)
                total += mib
        peaks["_total"] = max(peaks.get("_total", 0.0), total)
        stop.wait(2.0)


# ---------- changes in the source ----------

APPENDED = "Agent: Update from the cold-chain team: the van's chiller was repaired and recalibrated on 3 October."
ERASE_NOTE = "a customer with a ticket and a review, not in the eval set"


def change() -> dict:
    """What happens to support data in a normal week, all through the source tables (the index only sees CDC)."""
    with psycopg.connect(PG, autocommit=True) as con:
        swept = con.execute("UPDATE support.tickets SET status = 'closed', updated_at = now() "
                            "WHERE status <> 'closed'").rowcount  # fmt: skip
        appended = con.execute(
            "SELECT ticket_id FROM support.tickets WHERE issue = 'arrived_warm' AND body NOT LIKE '%%Agent:%%Agent:%%' "
            "ORDER BY ticket_id LIMIT 1").fetchone()[0]  # fmt: skip
        con.execute("UPDATE support.tickets SET body = body || %s, updated_at = now() WHERE ticket_id = %s",
                    ("\n\n" + APPENDED, appended))  # fmt: skip
        renamed = con.execute(
            "SELECT ticket_id FROM support.tickets WHERE body LIKE '%%Agent:%%Agent:%%' ORDER BY ticket_id LIMIT 1"
        ).fetchone()[0]
        con.execute("UPDATE support.tickets SET subject = subject || ' (escalated)', updated_at = now() "
                    "WHERE ticket_id = %s", (renamed,))  # fmt: skip
        in_eval = {t for ids in eval_set().column("expected_ids").to_pylist() for t in ids}
        candidates = con.execute(
            "SELECT customer_id, array_agg(ticket_id) FROM support.tickets t WHERE EXISTS "
            "(SELECT 1 FROM support.reviews r WHERE r.customer_id = t.customer_id) GROUP BY 1 ORDER BY 1"
        ).fetchall()
        erased = next(c for c, ids in candidates if not set(ids) & in_eval)
        gone = [i for (i,) in con.execute("DELETE FROM support.tickets WHERE customer_id = %s RETURNING ticket_id",
                                          (erased,))]  # fmt: skip
        gone += [i for (i,) in con.execute("DELETE FROM support.reviews WHERE customer_id = %s RETURNING review_id",
                                           (erased,))]  # fmt: skip
        t, r = sum(i.startswith("T") for i in gone), sum(i.startswith("R") for i in gone)
    print(f"status sweep: {swept:,} tickets set to closed (text unchanged)")
    print(f"appended an agent message to {appended}")
    print(f"edited the subject of {renamed}")
    print(f"erasure of {erased} ({ERASE_NOTE}): deleted {', '.join(gone)}")
    OUT.mkdir(parents=True, exist_ok=True)
    out = {"swept": swept, "appended": appended, "renamed": renamed, "erased": erased, "tickets": t, "reviews": r,
           "deleted_ids": gone}  # fmt: skip
    (OUT / "change.json").write_text(json.dumps(out))
    return out


def topic_config(topic: str, *keys: str) -> dict[str, str]:
    from confluent_kafka.admin import AdminClient, ConfigResource

    res = ConfigResource(ConfigResource.Type.TOPIC, topic)
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})  # keep it alive until the future resolves
    entries = admin.describe_configs([res])[res].result(timeout=10)
    return {k: entries[k].value for k in keys}


def erased() -> dict:
    """The erased customer after `index`: nothing in the index, a tombstone per document, the text still in Kafka."""
    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

    change_ = json.loads((OUT / "change.json").read_text())
    who, ids = change_["erased"], change_["deleted_ids"]
    with psycopg.connect(RAG) as con:
        docs = con.execute("SELECT count(*) FROM documents WHERE customer_id = %s", (who,)).fetchone()[0]
        chunks = con.execute("SELECT count(*) FROM chunks WHERE doc_id = ANY(%s)", (ids,)).fetchone()[0]
        stones = con.execute("SELECT count(*) FROM documents WHERE doc_id = ANY(%s) AND deleted_at IS NOT NULL "
                             "AND customer_id IS NULL AND content_hash IS NULL", (ids,)).fetchone()[0]  # fmt: skip
    with psycopg.connect(PG) as con:
        src = sum(con.execute(f"SELECT count(*) FROM support.{t} WHERE customer_id = %s", (who,)).fetchone()[0]
                  for t in KEY)  # fmt: skip
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "part32-audit", "enable.partition.eof": True})
    md = c.list_topics(timeout=10).topics
    parts = [TopicPartition(t, p, OFFSET_BEGINNING) for t in TOPICS for p in md[t].partitions]
    c.assign(parts)
    held, done = Counter(), set()
    while len(done) < len(parts):
        for m in c.consume(500, 2.0):
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    done.add((m.topic(), m.partition()))
                continue
            if m.value() and who.encode() in m.value():
                held[json.loads(m.value())["op"]] += 1
    c.close()
    print(f"{who}: source rows {src}; index: documents naming them {docs}, chunks of {', '.join(ids)} {chunks}, "
          f"tombstones (no customer, no text) {stones}")  # fmt: skip
    cfg = topic_config(TOPICS[0], "segment.ms", "retention.ms", "cleanup.policy")
    days = {k: f"{int(v) / 86_400_000:g} days" if k.endswith(".ms") else v for k, v in cfg.items()}
    print(f"Kafka still holds {sum(held.values())} events with their rows ({dict(held)}). Retention deletes whole "
          f"segments: a segment rolls after segment.ms ({days['segment.ms']}) and is deleted once its newest record is "
          f"older than retention.ms ({days['retention.ms']}); cleanup.policy={days['cleanup.policy']}")  # fmt: skip
    return {"source": src, "documents": docs, "chunks": chunks, "tombstones": stones, "kafka": dict(held)}


# ---------- search and evaluation ----------


def statements() -> dict[str, str]:
    text = (SQL / "postgres/search.sql").read_text()
    return {b.split("\n", 1)[0].strip(): b.split("\n", 1)[1] for b in text.split("-- name: ")[1:]}


def ranked_docs(con, how: str, q: str, qv: str, k: int = 5, n: int = 50) -> list[str]:
    """Top-k documents: nearest chunks (or matching words, or both fused), one entry per document."""
    rows = con.execute(statements()[how], {"q": qv, "text": q, "n": n}).fetchall()
    seen: list[str] = []
    for r in rows:
        if r[0] not in seen:
            seen.append(r[0])
    return seen[:k]


def search(q: str, country: str | None, how: str) -> None:
    emb = Embedder()
    qv = vec(emb([query_text(q)])[0])
    with psycopg.connect(RAG) as con:
        if country:
            rows = con.execute(statements()["vector_in_country"], {"q": qv, "country": country, "n": 20}).fetchall()
            ids = list(dict.fromkeys(r[0] for r in rows))[:5]
        else:
            ids = ranked_docs(con, how, q, qv)
        for doc in ids:
            text = con.execute("SELECT text FROM chunks WHERE doc_id = %s ORDER BY chunk_no LIMIT 1", (doc,)).fetchone()
            print((text[0] if text else "").split("\n")[0][:110])


def eval_set(tag: str = EVAL_TAG) -> pa.Table:
    t = lake().load_table(EVAL_TABLE)
    return t.scan(snapshot_id=t.snapshot_by_name(tag).snapshot_id).to_arrow()


def evaluate(tag: str = EVAL_TAG) -> dict:
    """recall@5 per method on the tagged eval set, with and without the query instruction; HNSW vs exact search."""
    ev = eval_set(tag).to_pylist()
    emb = Embedder()
    with_task = emb([query_text(e["query"]) for e in ev])
    bare = emb([query_text(e["query"], instruct=False) for e in ev])
    results: dict[str, float] = {}
    per_query: dict[str, list[float]] = {}
    with psycopg.connect(RAG, autocommit=True) as con:
        for label, vectors, methods in (("instruction", with_task, ("vector", "keyword", "hybrid")),
                                        ("no instruction", bare, ("vector",))):  # fmt: skip
            for how in methods:
                rec = []
                for e, v in zip(ev, vectors, strict=True):
                    got = ranked_docs(con, how, e["query"], vec(v))
                    rec.append(len(set(got) & set(e["expected_ids"])) / len(e["expected_ids"]))
                key = f"{how}, {label}" if how == "vector" else how
                results[key], per_query[key] = sum(rec) / len(rec), rec
        # approximate vs exact: the 10 nearest chunks through HNSW (ef_search 40), then by a full scan.
        # At this size the planner picks the full scan by itself (see `filtered`), so each path is forced.
        overlap, t_ann, t_exact = [], 0.0, 0.0
        for v in with_task:
            ann, t = nearest(con, "SET LOCAL enable_seqscan = off", statements()["vector"], {"q": vec(v), "n": 10})
            t_ann += t
            exact, t = nearest(con, "SET LOCAL enable_indexscan = off", statements()["vector"], {"q": vec(v), "n": 10})
            t_exact += t
            overlap.append(len(ann & exact) / 10)
        offsets = dict(((f"{t}/{p}", o) for t, p, o in con.execute("SELECT * FROM consumer_offsets ORDER BY 1, 2")))
        chunks = con.execute("SELECT count(*) FROM chunks").fetchone()[0]
    n = len(ev)
    base = per_query["vector, instruction"]
    for k, v in results.items():
        diff = ""
        if k != "vector, instruction":  # how many queries move, against vector search with the instruction
            up = sum(a > b for a, b in zip(per_query[k], base, strict=True))
            down = sum(a < b for a, b in zip(per_query[k], base, strict=True))
            diff = f"  vs vector with instruction: {up} queries better, {down} worse"
        print(f"recall@5  {k:<28} {v:.3f}{diff}")
    print(f"HNSW top-10 vs exact top-10: {sum(overlap) / n:.3f} of the exact neighbours found; "
          f"{t_ann / n * 1000:.1f} ms vs {t_exact / n * 1000:.1f} ms per query over {chunks:,} chunks")  # fmt: skip
    t = lake().load_table(EVAL_TABLE)
    run = {"run_at": datetime.now(UTC), "eval_tag": tag, "eval_snapshot": t.snapshot_by_name(tag).snapshot_id,
           "model": MODEL, "chunker": CHUNKER, "index": "hnsw m=16 ef_construction=64 ef_search=40",
           "corpus_offsets": json.dumps(offsets), "chunks": chunks, "queries": n,
           "recall_vector": results["vector, instruction"], "recall_keyword": results["keyword"],
           "recall_hybrid": results["hybrid"], "ann_overlap_at_10": sum(overlap) / n}  # fmt: skip
    cat = lake()
    data = pa.Table.from_pylist([run])
    runs = cat.load_table(RUNS_TABLE) if cat.table_exists(RUNS_TABLE) else cat.create_table(RUNS_TABLE, data.schema)
    runs.append(data)
    print(f"logged to {RUNS_TABLE}: eval snapshot {run['eval_snapshot']}, offsets {offsets}")
    return run


def nearest(con: psycopg.Connection, setting: str, sql: str, params: dict) -> tuple[set, float]:
    """One statement in its own transaction with one planner setting; the (doc, chunk) pairs and the seconds."""
    with con.transaction():
        con.execute(setting)
        t0 = time.perf_counter()
        rows = con.execute(sql, params).fetchall()
        return {(r[0], r[1]) for r in rows}, time.perf_counter() - t0


def plan(con: psycopg.Connection, sql: str, params: dict, setting: str = "SELECT 1") -> str:
    with con.transaction():
        con.execute(setting)
        lines = [r[0] for r in con.execute("EXPLAIN (COSTS OFF) " + sql, params).fetchall()]
    return ", ".join(m.group(0) for line in lines if (m := re.search(r"(Seq Scan|Index Scan using \w+)", line)))


def filtered() -> dict:
    """A GB agent may only see GB tickets; SG holds a small share of the chunks. Ask for 10 SG chunks per query."""
    ev = eval_set().to_pylist()
    emb = Embedder()
    vectors = emb([query_text(e["query"]) for e in ev])
    out = {}
    sql = statements()["vector_in_country"]
    with psycopg.connect(RAG, autocommit=True) as con:
        share = con.execute("SELECT round(100.0 * avg((country = 'SG')::int), 1) FROM chunks").fetchone()[0]
        p0 = {"q": vec(vectors[0]), "country": "SG", "n": 10}
        print(f"SG share of chunks: {share}%")
        print(f"planner's choice, top 10 in SG: {plan(con, sql, p0)}; with enable_seqscan = off: "
              f"{plan(con, sql, p0, 'SET LOCAL enable_seqscan = off')}")  # fmt: skip
        print(f"planner's choice, top 10 unfiltered: {plan(con, statements()['vector'], p0)}")
        for mode in ("off", "relaxed_order", "strict_order"):
            counts = []
            for v in vectors:
                setting = f"SET LOCAL enable_seqscan = off; SET LOCAL hnsw.iterative_scan = {mode}"
                rows, _ = nearest(con, setting, sql, {"q": vec(v), "country": "SG", "n": 10})
                counts.append(len(rows))
            out[mode] = counts
            short = sum(c < 10 for c in counts)
            print(f"hnsw.iterative_scan = {mode:<14} rows returned (asked for 10): min {min(counts)}, "
                  f"mean {sum(counts) / len(counts):.1f}, queries short of 10: {short}")  # fmt: skip
    return out


def batching() -> float:
    """The same chunk embedded alone and in a batch with a longer neighbour: vectors are not bit-for-bit stable."""
    emb = Embedder()
    text = "Ticket TGB000001 | arrived warm | GB | Chilled items arrived warm\nCustomer: The yoghurt was warm."
    alone = emb([text])[0]
    batched = emb([APPENDED * 4, text])[1]
    diff = max(abs(a - b) for a, b in zip(alone, batched, strict=True))
    cos = sum(a * b for a, b in zip(alone, batched, strict=True))
    print(f"largest difference in one dimension: {diff:.6f}; cosine similarity {cos:.6f}")
    return diff


# ---------- the lakehouse copy: a versioned corpus, searched with DuckDB ----------


def export() -> dict:
    with psycopg.connect(RAG) as con:
        rows = con.execute("SELECT c.doc_id, c.chunk_no, d.source, c.country, c.text, c.embedding::text, c.model "
                           "FROM chunks c JOIN documents d USING (doc_id) ORDER BY 1, 2").fetchall()  # fmt: skip
    data = pa.table({
        "doc_id": [r[0] for r in rows], "chunk_no": pa.array([r[1] for r in rows], pa.int32()),
        "source": [r[2] for r in rows], "country": [r[3] for r in rows], "text": [r[4] for r in rows],
        "embedding": pa.array([json.loads(r[5]) for r in rows], pa.list_(pa.float32())), "model": [r[6] for r in rows],
    })  # fmt: skip
    snap = publish(CHUNKS_TABLE, data, CHUNKS_TAG)
    print(f"{CHUNKS_TABLE}: {data.num_rows:,} chunks; tag {CHUNKS_TAG} -> snapshot {snap}")
    # exact search in DuckDB over the tagged snapshot: no index, every vector compared
    t = lake().load_table(CHUNKS_TABLE)
    t0 = time.perf_counter()
    arrow = t.scan(snapshot_id=t.snapshot_by_name(CHUNKS_TAG).snapshot_id).to_arrow()
    load = time.perf_counter() - t0
    con = duckdb.connect()
    con.register("snapshot", arrow)
    con.execute("CREATE TABLE corpus AS SELECT doc_id, chunk_no, embedding::FLOAT[1024] AS embedding FROM snapshot")
    ev = eval_set().to_pylist()
    vectors = Embedder()([query_text(e["query"]) for e in ev])
    sql = (SQL / "nearest.sql").read_text()
    t0 = time.perf_counter()
    hits = [con.execute(sql, [v]).fetchall() for v in vectors]
    per = (time.perf_counter() - t0) / len(vectors)
    with psycopg.connect(RAG) as pg_, pg_.transaction():
        pg_.execute("SET LOCAL enable_indexscan = off")
        same = sum(
            {(r[0], r[1]) for r in h}
            == {(r[0], r[1]) for r in pg_.execute(statements()["vector"], {"q": vec(v), "n": 10}).fetchall()}
            for h, v in zip(hits, vectors, strict=True)
        )
    print(f"read the snapshot in {load:.1f} s; DuckDB exact top-10: {per * 1000:.1f} ms per query; "
          f"same 10 chunks as pgvector's exact scan for {same} of {len(vectors)} queries")  # fmt: skip
    return {"rows": data.num_rows, "ms_per_query": per * 1000, "same": same, "snapshot": snap}


# ---------- features for recommendations, as of the moment of prediction ----------


def features() -> dict:
    """Online orders (Part 16's CDC replica) -> per-customer category features with event and availability times,
    an as-of training set, and what a naive join to today's profile does to the offline metric."""
    cat = cat9.pyiceberg("root", vended=False)
    orders = cat.load_table("raw.orders").scan(
        selected_fields=("order_id", "customer_id", "status", "created_at")).to_arrow()  # fmt: skip
    orders = p23.suppress(orders, delete={"customer_id": "customer_id"})  # the erased never become features
    lines = cat.load_table("raw.order_lines").scan(selected_fields=("order_id", "sku", "qty")).to_arrow()
    products = cat.load_table("gold.dim_product").scan(row_filter="is_current = true",
                                                       selected_fields=("sku", "category")).to_arrow()  # fmt: skip
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for name, t in (("orders", orders), ("order_lines", lines), ("products", products)):
        con.register(name, t)
    text = (SQL / "features.sql").read_text()
    blocks = {b.split("\n", 1)[0].strip(): b.split("\n", 1)[1] for b in text.split("-- name: ")[1:]}
    for name in ("order_categories", "customer_features", "published", "examples", "popularity", "training"):
        con.execute(blocks[name])
    n_feat, n_cust = con.execute("SELECT count(*), count(DISTINCT customer_id) FROM customer_features").fetchone()
    print(f"customer_features: {n_feat:,} rows for {n_cust:,} customers (one per order, as it stood after it)")
    res = con.execute(blocks["metrics"]).fetchall()
    print(f"{'training set':<44} {'examples':>8} {'with a feature':>14} {'hit@1':>7}")
    for r in res:
        print(f"{r[0]:<44} {r[1]:>8,} {r[2]:>14,} {r[3]:>7.3f}")
    # serving: what the online path returns for (customer, time), compared with each training row
    serve = blocks["serve"]
    sample = con.execute("SELECT customer_id, event_ts, fav_event_time, fav_available FROM training "
                         "ORDER BY hash(order_id) LIMIT 2000").fetchall()  # fmt: skip
    skew_event = skew_avail = 0
    for cid, ts, fav_event, fav_avail in sample:
        served = (con.execute(serve, [cid, ts]).fetchall() or [[None]])[0][0]
        skew_event += served != fav_event
        skew_avail += served != fav_avail
    print(f"served value vs training value, 2,000 examples: event-time features differ in {skew_event}, "
          f"availability-time features differ in {skew_avail}")  # fmt: skip
    feat = con.sql("SELECT * FROM customer_features ORDER BY customer_id, feature_ts").to_arrow_table()
    train = con.sql(
        "SELECT customer_id, order_id, event_ts, bought, fav_available AS favourite_category, "
        "available_at AS feature_available_at FROM training ORDER BY event_ts, order_id"
    ).to_arrow_table()
    publish(FEATURES_TABLE, feat, None)
    snap = publish(TRAINING_TABLE, train, TRAINING_TAG)
    print(f"{FEATURES_TABLE} {feat.num_rows:,} rows; {TRAINING_TABLE} {train.num_rows:,} rows, "
          f"tag {TRAINING_TAG} -> snapshot {snap}")  # fmt: skip
    return {"metrics": {r[0]: (r[1], r[2], r[3]) for r in res}, "skew": (skew_event, skew_avail)}


# ---------- reset ----------


def reset() -> None:
    old = (p16.CONNECTOR, p16.SLOT)
    p16.CONNECTOR, p16.SLOT = CONNECTOR, SLOT  # Part 16's stop/delete-offsets/delete/drop-slot, for this connector
    try:
        p16.remove_connector()
        print(f"connector {CONNECTOR} and slot {SLOT}: removed")
    except (urllib.error.URLError, ConnectionError):
        with psycopg.connect(PG, autocommit=True) as con:
            con.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                        (SLOT,))  # fmt: skip
        print("Kafka Connect not reachable: slot dropped")
    finally:
        p16.CONNECTOR, p16.SLOT = old
    try:
        existing = p16.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:29092", "--list")
        for t in [x for x in existing.split() if x in (*TOPICS, "__debezium-heartbeat.larkspur.cx")]:
            p16.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:29092", "--delete",
                      "--topic", t)  # fmt: skip
        print("topics larkspur.cx.support.*: deleted")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Kafka not running: topics left alone")
    with psycopg.connect(PG, autocommit=True) as con:
        con.execute("DROP PUBLICATION IF EXISTS larkspur_support")
        con.execute("DROP SCHEMA IF EXISTS support CASCADE")
        con.execute("DROP DATABASE IF EXISTS rag WITH (FORCE)")
    print("PostgreSQL: schema support, publication larkspur_support, database rag: dropped")
    cat = lake()
    for name in (EVAL_TABLE, RUNS_TABLE, CHUNKS_TABLE, FEATURES_TABLE, TRAINING_TABLE):
        if cat.table_exists(name):
            cat.purge_table(name)
    print("Iceberg ml.*: purged")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("setup", "connect", "change", "erased", "filtered", "eval", "batching", "export", "features", "reset"):
        sub.add_parser(name)
    p = sub.add_parser("index")
    p.add_argument("--from-beginning", action="store_true")
    p.add_argument("--idle", type=float, default=5.0)
    p.add_argument("--label", default="run")
    p = sub.add_parser("search")
    p.add_argument("text")
    p.add_argument("--country")
    p.add_argument("--how", default="hybrid", choices=["vector", "keyword", "hybrid"])
    a = ap.parse_args()
    if a.cmd == "index":
        index(a.from_beginning, a.idle, a.label)
    elif a.cmd == "search":
        search(a.text, a.country, a.how)
    else:
        {"setup": setup, "connect": connect, "change": change, "erased": erased, "filtered": filtered,
         "eval": evaluate, "batching": batching, "export": export, "features": features,
         "reset": reset}[a.cmd]()  # fmt: skip


if __name__ == "__main__":
    main()
