-- Part 28: real-time sales and stock movement by store, from the POS and price topics into Iceberg.
-- Run with: python -m scripts.part28_flink run (sql-client.sh -i tables.sql -f sales_to_lake.sql).
SET 'execution.runtime-mode' = 'streaming';
-- The Iceberg sink commits after each completed checkpoint (and once at the end of a bounded input):
-- the checkpoint interval is the lake's commit interval.
SET 'execution.checkpointing.interval' = '10 s';
SET 'pipeline.name' = 'part28-sales-to-lake';

-- Every deduplicated line with its as-of list price: append-only.
CREATE TABLE IF NOT EXISTS lake.`stream`.pos_sales_lines (
  event_id STRING, basket_id STRING, store_id STRING, till_id STRING, country STRING, currency STRING,
  business_date DATE, event_ts TIMESTAMP(3), uploaded_at TIMESTAMP(3), line_no INT, sku STRING, qty INT,
  unit_price_minor BIGINT, discount_minor BIGINT, net_minor BIGINT, list_price_minor BIGINT, price_from TIMESTAMP(3)
) PARTITIONED BY (business_date) WITH ('format-version' = '2');

-- Sales and units per store per UTC hour: a window, final when the watermark passes its end. Append-only.
CREATE TABLE IF NOT EXISTS lake.`stream`.store_sales_hourly (
  store_id STRING, currency STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  baskets BIGINT, lines BIGINT, units_out BIGINT, sales_minor BIGINT
) WITH ('format-version' = '2');

-- Sales per store and local business date: an updating aggregate, written as upserts on the primary key.
CREATE TABLE IF NOT EXISTS lake.`stream`.store_sales_daily (
  store_id STRING NOT NULL, business_date DATE NOT NULL, currency STRING,
  baskets BIGINT, units_out BIGINT, sales_minor BIGINT,
  PRIMARY KEY (store_id, business_date) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

EXECUTE STATEMENT SET
BEGIN
INSERT INTO lake.`stream`.pos_sales_lines
SELECT event_id, basket_id, store_id, till_id, country, currency, business_date, event_ts, uploaded_at, line_no, sku,
       qty, unit_price_minor, discount_minor, net_minor, list_price_minor, price_from
FROM sales_lines;
INSERT INTO lake.`stream`.store_sales_hourly
SELECT store_id, currency, window_start, window_end, COUNT(DISTINCT basket_id), COUNT(*), SUM(qty), SUM(net_minor)
FROM TABLE(TUMBLE(TABLE sales_lines, DESCRIPTOR(event_ts), INTERVAL '1' HOUR))
GROUP BY store_id, currency, window_start, window_end;
-- Grouped by exactly the primary key: grouped by more columns, Flink 2.2 adds a SinkMaterializer (more state).
INSERT INTO lake.`stream`.store_sales_daily
SELECT store_id, business_date, MAX(currency), COUNT(DISTINCT basket_id), SUM(qty), SUM(net_minor)
FROM sales_lines
GROUP BY store_id, business_date;
END;
