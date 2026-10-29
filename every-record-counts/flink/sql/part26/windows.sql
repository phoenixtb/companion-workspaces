-- Part 26: VAN-014's 5-minute average temperature in event time, under two watermarks.
-- Run with: make flink-sql SQL=part26/windows.sql (after `python -m scripts.part26_time kafka`).
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

-- The same topic and columns; only the watermark differs.
CREATE TABLE readings_wm10 (
  WATERMARK FOR event_ts AS event_ts - INTERVAL '10' MINUTE
) LIKE readings (OVERWRITING WATERMARKS);

SELECT van_id, window_start, COUNT(*) AS readings, CAST(AVG(temp_c) AS DECIMAL(4, 2)) AS avg_c
FROM TABLE(TUMBLE(TABLE readings, DESCRIPTOR(event_ts), INTERVAL '5' MINUTES))
WHERE van_id = 'VAN-014' AND window_start >= TIMESTAMP '2026-10-14 10:10:00' AND window_start < TIMESTAMP '2026-10-14 10:30:00'
GROUP BY van_id, window_start, window_end;

SELECT van_id, window_start, COUNT(*) AS readings, CAST(AVG(temp_c) AS DECIMAL(4, 2)) AS avg_c
FROM TABLE(TUMBLE(TABLE readings_wm10, DESCRIPTOR(event_ts), INTERVAL '5' MINUTES))
WHERE van_id = 'VAN-014' AND window_start >= TIMESTAMP '2026-10-14 10:10:00' AND window_start < TIMESTAMP '2026-10-14 10:30:00'
GROUP BY van_id, window_start, window_end;
