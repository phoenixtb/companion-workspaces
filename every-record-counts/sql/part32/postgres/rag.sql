-- Part 32: the retrieval index, in its own database (rag) on the lab PostgreSQL, apart from the OLTP tables it is
-- built from. pgvector 0.8.7 ships in the image (pgvector/pgvector:0.8.7-pg18); the extension is created here only.
CREATE EXTENSION IF NOT EXISTS vector;
DROP TABLE IF EXISTS chunks, documents, consumer_offsets;

CREATE TABLE documents (                        -- one row per ticket or review the index has seen
    doc_id       text        PRIMARY KEY,       -- ticket_id or review_id
    source       text        NOT NULL,          -- ticket | review
    customer_id  text,                          -- NULL once deleted: a tombstone keeps no personal data
    country      char(2),
    sku          text,
    status       text,
    content_hash text,                          -- sha256 of model + chunker + the redacted text; NULL when deleted
    source_lsn   bigint      NOT NULL,          -- position in the source's WAL of the last change applied
    source_ts    timestamptz NOT NULL,          -- when that change was committed in the source (event time)
    indexed_at   timestamptz NOT NULL,          -- when the index applied it (ingestion time)
    deleted_at   timestamptz                    -- tombstone: a replayed snapshot row must not bring it back
);

CREATE TABLE chunks (
    doc_id      text        NOT NULL REFERENCES documents ON DELETE CASCADE,
    chunk_no    int         NOT NULL,
    chunk_hash  text        NOT NULL,           -- sha256 of model + chunk text: the unit of re-use
    country     char(2)     NOT NULL,           -- copied from the document for filtered search
    text        text        NOT NULL,           -- header line + one message, personal data redacted
    tsv         tsvector    GENERATED ALWAYS AS (to_tsvector('english', text)) STORED,
    embedding   vector(1024) NOT NULL,
    model       text        NOT NULL,
    embedded_at timestamptz NOT NULL,
    PRIMARY KEY (doc_id, chunk_no)
);
CREATE INDEX chunks_embedding ON chunks USING hnsw (embedding vector_cosine_ops);  -- m 16, ef_construction 64
CREATE INDEX chunks_tsv ON chunks USING gin (tsv);

CREATE TABLE consumer_offsets (                 -- written in the same transaction as the chunks
    topic       text NOT NULL,
    partition   int  NOT NULL,
    next_offset bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);
