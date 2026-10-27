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

-- Added in Part 4: the rest of the sources the star schema reads.
CREATE OR REPLACE VIEW raw_order_lines AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/oltp/order_lines/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_catalog_products AS  -- one full extract per day, taken just after midnight
SELECT * FROM read_parquet(getvariable('raw_root') || '/catalog/products/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_price_changes AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/pricing/price_changes/*.parquet');

CREATE OR REPLACE VIEW raw_promotion_versions AS
SELECT * FROM read_csv(getvariable('raw_root') || '/promotions/promotion_versions.csv');

CREATE OR REPLACE VIEW raw_promotion_scope AS
SELECT * FROM read_csv(getvariable('raw_root') || '/promotions/promotion_scope.csv');

CREATE OR REPLACE VIEW raw_stock_adjustments AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/inventory/adjustments/*/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW raw_suppliers AS
SELECT * FROM read_csv(getvariable('raw_root') || '/reference/suppliers/suppliers.csv');

CREATE OR REPLACE VIEW raw_po_lines AS
SELECT * FROM read_parquet(getvariable('raw_root') || '/procurement/purchase_orders/*.parquet');

-- Suppliers' delivery files, every value as text (Part 15 ingests them properly). XLSX files need read_xlsx,
-- one file at a time; scripts/part04_star.py adds those rows.
CREATE OR REPLACE VIEW raw_supplier_deliveries_csv AS
SELECT * FROM read_csv(getvariable('raw_root') || '/suppliers/deliveries/*/*.csv', all_varchar = true,
                       union_by_name = true, filename = true);
