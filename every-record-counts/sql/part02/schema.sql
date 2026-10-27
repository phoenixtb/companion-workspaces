-- Part 2: the tables the daily POS sales job writes (DuckDB).
CREATE SCHEMA IF NOT EXISTS naive;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- v1's output: sales per store and business date, appended by every run.
CREATE TABLE IF NOT EXISTS naive.daily_store_sales (
    business_date DATE,
    store_id      VARCHAR,
    currency      VARCHAR,
    baskets       BIGINT,
    revenue_minor BIGINT
);

-- silver: one row per basket, cleaned and deduplicated. The primary key is the dedup key.
CREATE TABLE IF NOT EXISTS silver.pos_baskets (
    basket_id     VARCHAR PRIMARY KEY,
    store_id      VARCHAR NOT NULL,
    country       VARCHAR NOT NULL,
    currency      VARCHAR NOT NULL,
    business_date DATE NOT NULL,         -- the store's local date: what finance reports on
    event_ts      TIMESTAMPTZ NOT NULL,  -- when the sale happened
    upload_date   DATE NOT NULL,         -- which raw partition delivered it (lineage)
    event_id      VARCHAR NOT NULL,
    amount_minor  BIGINT NOT NULL
);

-- gold: v2's output, the same shape as v1's, one row per store and business date.
CREATE TABLE IF NOT EXISTS gold.daily_store_sales (
    business_date DATE,
    store_id      VARCHAR,
    country       VARCHAR NOT NULL,
    currency      VARCHAR NOT NULL,
    baskets       BIGINT NOT NULL,
    revenue_minor BIGINT NOT NULL,
    PRIMARY KEY (business_date, store_id)
);
