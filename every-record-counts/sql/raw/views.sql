-- Views over the landed raw zone. Set raw_root first:
--   SET VARIABLE raw_root = 'data/raw';                -- local files
--   SET VARIABLE raw_root = 's3://larkspur-raw/raw';   -- the bucket (needs an S3 secret)
SET TimeZone = 'UTC';  -- otherwise ::DATE on a TIMESTAMPTZ uses your laptop's zone

CREATE OR REPLACE VIEW raw_stores AS
SELECT * FROM read_csv(getvariable('raw_root') || '/reference/stores/stores.csv');

CREATE OR REPLACE VIEW raw_pos_events AS
SELECT * FROM read_json(getvariable('raw_root') || '/pos/events/*/*.jsonl.gz', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_orders_poll AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/oltp/orders_poll/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_orders AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/oltp/orders/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_refunds AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/oltp/refunds/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_fx_rates AS
SELECT rate_date, base_currency, quote_currency, CAST(rate AS DECIMAL(18, 8)) AS rate
FROM read_csv(getvariable('raw_root') || '/fx/eod_rates.csv', types = {'rate': 'VARCHAR'});

CREATE OR REPLACE VIEW raw_stock_snapshots AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/inventory/snapshots/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_clickstream AS
SELECT * FROM read_json(getvariable('raw_root') || '/clickstream/events/*/*.jsonl.gz', hive_partitioning = true,
                        union_by_name = true);
