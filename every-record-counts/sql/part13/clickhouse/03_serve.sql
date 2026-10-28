-- Part 13: a serving table kept by a refreshable materialized view. Every hour ClickHouse re-runs the query against
-- the lake and swaps the result in atomically: the lakehouse stays the source of truth, ClickHouse serves it.
CREATE DATABASE IF NOT EXISTS serve;
DROP VIEW IF EXISTS serve.daily_store_sales_mv SYNC;
DROP TABLE IF EXISTS serve.daily_store_sales SYNC;
CREATE TABLE serve.daily_store_sales (
    store_id       LowCardinality(String),
    business_date  Date32,
    country        LowCardinality(String),
    currency       LowCardinality(String),
    lines          UInt64,
    sales_minor    Int64,
    discount_minor Int64
)
ENGINE = MergeTree
ORDER BY (store_id, business_date);
CREATE MATERIALIZED VIEW serve.daily_store_sales_mv
REFRESH EVERY 1 HOUR TO serve.daily_store_sales AS
SELECT store_id, business_date, any(country) AS country, any(currency) AS currency,
       count() AS lines, sum(amount_minor) AS sales_minor, sum(discount_minor) AS discount_minor
FROM lake.`part10.pos_monthly`
GROUP BY store_id, business_date;
