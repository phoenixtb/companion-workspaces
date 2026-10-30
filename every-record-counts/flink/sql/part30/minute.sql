-- Part 30, the minute tier: every sale line into Iceberg, one commit per completed checkpoint.
-- The checkpoint interval is the commit interval, and so the freshness of the lake (Part 28).
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60 s';
SET 'pipeline.name' = 'part30-stock-lines-1m';

CREATE TABLE IF NOT EXISTS lake.`stream`.stock_lines_1m (
  basket_id STRING, line_no INT, store_id STRING, sku STRING, qty INT, business_date DATE,
  event_ts TIMESTAMP(3), produced_at TIMESTAMP(3)
) PARTITIONED BY (business_date) WITH ('format-version' = '2');

INSERT INTO lake.`stream`.stock_lines_1m SELECT * FROM stock_lines;
