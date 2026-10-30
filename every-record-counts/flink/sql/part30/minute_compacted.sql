-- Part 30: the same minute tier, written by Iceberg's SinkV2 sink (experimental in Iceberg 1.12) with compaction
-- after commits. After 10 commits or 600 seconds (both default triggers) the job rewrites small files into larger
-- ones in a replace commit. Snapshot expiry (flink-maintenance.expire-snapshots.enabled) is left off: see Part 10.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60 s';
SET 'pipeline.name' = 'part30-stock-lines-1m-compacted';
SET 'table.exec.iceberg.use-v2-sink' = 'true';
SET 'flink-maintenance.rewrite.enabled' = 'true';
SET 'flink-maintenance.rewrite.schedule.commit-count' = '10';

CREATE TABLE IF NOT EXISTS lake.`stream`.stock_lines_1m_compacted (
  basket_id STRING, line_no INT, store_id STRING, sku STRING, qty INT, business_date DATE,
  event_ts TIMESTAMP(3), produced_at TIMESTAMP(3)
) PARTITIONED BY (business_date) WITH ('format-version' = '2');

INSERT INTO lake.`stream`.stock_lines_1m_compacted SELECT * FROM stock_lines;
