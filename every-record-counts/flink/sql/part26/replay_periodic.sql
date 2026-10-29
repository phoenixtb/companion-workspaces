-- Part 26: the 2-minute watermark again, emitted the default way. A fast replay sees no late data at all.
-- Run with: make flink-sql SQL=part26/replay_periodic.sql (after `python -m scripts.part26_time kafka`).
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';

CREATE TABLE readings (
  van_id     STRING,
  sensor_id  STRING,
  seq        INT,
  temp_c     DECIMAL(4, 1),
  event_ts   TIMESTAMP(3),   -- the sensor's clock, UTC
  ingest_ts  TIMESTAMP(3),   -- the MQTT bridge's clock, UTC
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'coldchain.readings',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',        -- replay what is there, then finish
  'scan.watermark.emit.strategy' = 'on-event',  -- a watermark after every reading, not every 200 ms
  'format' = 'json'
);

-- The same table with Flink's default: a watermark every 200 ms of wall-clock time, not one per reading.
CREATE TABLE readings_periodic WITH ('scan.watermark.emit.strategy' = 'on-periodic') LIKE readings;

SELECT van_id, window_start, COUNT(*) AS readings, CAST(AVG(temp_c) AS DECIMAL(4, 2)) AS avg_c
FROM TABLE(TUMBLE(TABLE readings_periodic, DESCRIPTOR(event_ts), INTERVAL '5' MINUTES))
WHERE van_id = 'VAN-014' AND window_start >= TIMESTAMP '2026-10-14 10:10:00' AND window_start < TIMESTAMP '2026-10-14 10:30:00'
GROUP BY van_id, window_start, window_end;
