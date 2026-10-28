-- Part 5: a Data Vault for refunds and the sales they refund (DuckDB).
-- Hubs hold business keys, links hold relationships between them, satellites hold descriptive attributes with
-- history. Every table is insert-only. Every row says when the vault learned it (load_ts, the transaction time)
-- and where from (record_source). Hash keys are md5 of the upper-cased, trimmed business key.

CREATE TABLE IF NOT EXISTS hub_sale (       -- a till basket or an online order, whichever source names it first
    sale_hk VARCHAR PRIMARY KEY, sale_id VARCHAR NOT NULL, load_ts TIMESTAMPTZ NOT NULL, record_source VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS hub_refund (
    refund_hk VARCHAR PRIMARY KEY, refund_id VARCHAR NOT NULL, load_ts TIMESTAMPTZ NOT NULL,
    record_source VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS hub_store (
    store_hk VARCHAR PRIMARY KEY, store_id VARCHAR NOT NULL, load_ts TIMESTAMPTZ NOT NULL, record_source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS lnk_refund_sale (  -- which sale a refund pays back
    refund_sale_hk VARCHAR PRIMARY KEY, refund_hk VARCHAR NOT NULL, sale_hk VARCHAR NOT NULL,
    load_ts TIMESTAMPTZ NOT NULL, record_source VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS lnk_sale_store (   -- where a sale happened (online orders: the country's online store)
    sale_store_hk VARCHAR PRIMARY KEY, sale_hk VARCHAR NOT NULL, store_hk VARCHAR NOT NULL,
    load_ts TIMESTAMPTZ NOT NULL, record_source VARCHAR NOT NULL
);

-- Satellites: a new row only when the attributes' hash (hash_diff) differs from the key's latest row.
CREATE TABLE IF NOT EXISTS sat_refund (
    refund_hk VARCHAR, load_ts TIMESTAMPTZ, hash_diff VARCHAR NOT NULL, record_source VARCHAR NOT NULL,
    refund_ts TIMESTAMPTZ, country VARCHAR, original_channel VARCHAR, currency VARCHAR, amount_minor BIGINT,
    reason VARCHAR, tender_type VARCHAR,
    PRIMARY KEY (refund_hk, load_ts)
);
CREATE TABLE IF NOT EXISTS sat_sale_pos (    -- from the till uploads
    sale_hk VARCHAR, load_ts TIMESTAMPTZ, hash_diff VARCHAR NOT NULL, record_source VARCHAR NOT NULL,
    event_ts TIMESTAMPTZ, till_id VARCHAR, currency VARCHAR, amount_minor BIGINT,
    PRIMARY KEY (sale_hk, load_ts)
);
CREATE TABLE IF NOT EXISTS sat_sale_order (  -- from the nightly order extracts: one row per status change
    sale_hk VARCHAR, load_ts TIMESTAMPTZ, hash_diff VARCHAR NOT NULL, record_source VARCHAR NOT NULL,
    status VARCHAR, created_at TIMESTAMPTZ, channel VARCHAR, currency VARCHAR, total_minor BIGINT,
    PRIMARY KEY (sale_hk, load_ts)
);
CREATE TABLE IF NOT EXISTS sat_store (
    store_hk VARCHAR, load_ts TIMESTAMPTZ, hash_diff VARCHAR NOT NULL, record_source VARCHAR NOT NULL,
    country VARCHAR, timezone VARCHAR, city VARCHAR, format VARCHAR,
    PRIMARY KEY (store_hk, load_ts)
);
