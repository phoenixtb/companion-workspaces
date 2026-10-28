-- Part 13: copy Part 10's lines out of the lake into ClickHouse's own storage, sorted for the dashboard's filter.
CREATE DATABASE IF NOT EXISTS larkspur;
DROP TABLE IF EXISTS larkspur.pos_lines SYNC;
CREATE TABLE larkspur.pos_lines (
    basket_id        String,
    line_no          Int32,
    store_id         LowCardinality(String),   -- 600 values: dictionary-encoded
    country          LowCardinality(String),
    currency         LowCardinality(String),
    event_ts         DateTime64(6, 'UTC'),
    business_date    Date32,
    sku              LowCardinality(String),
    qty              Int32,
    unit_price_minor Int64,
    discount_minor   Int64,
    amount_minor     Int64,
    uploaded_at      DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
-- The sorting key is also the sparse primary index: one entry (a mark) per 8,192 rows.
ORDER BY (store_id, business_date);
INSERT INTO larkspur.pos_lines SELECT * FROM lake.`part10.pos_monthly`;
-- Merge each month's parts into one (a lab shortcut for stable numbers, not a habit).
OPTIMIZE TABLE larkspur.pos_lines FINAL;
