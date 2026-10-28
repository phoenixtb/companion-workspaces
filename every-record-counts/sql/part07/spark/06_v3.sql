-- Part 7, step 7: one day of clickstream in a default (format v2) table with the payload as JSON text, and in a
-- format v3 table with the payload as VARIANT (Spark SQL). Bot events are then deleted from both, merge-on-read.
CREATE TEMPORARY VIEW clicks_staged USING parquet
OPTIONS (path '/opt/lakehouse/lab/part07/clicks_2026-09-15.parquet');

CREATE TABLE lakehouse.part07.clicks_v2 (
    event_id STRING, event_type STRING, event_ts TIMESTAMP, anonymous_id STRING, platform STRING, country STRING,
    payload STRING
) USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ('write.delete.mode' = 'merge-on-read');

CREATE TABLE lakehouse.part07.clicks_v3 (
    event_id STRING, event_type STRING, event_ts TIMESTAMP, anonymous_id STRING, platform STRING, country STRING,
    payload VARIANT
) USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read');

INSERT INTO lakehouse.part07.clicks_v2 SELECT * FROM clicks_staged;
INSERT INTO lakehouse.part07.clicks_v3
SELECT event_id, event_type, event_ts, anonymous_id, platform, country, parse_json(payload) FROM clicks_staged;

-- Top products viewed: text parsed on every read vs a typed path into the binary variant.
SELECT get_json_object(payload, '$.sku') AS sku, count(*) AS views
FROM lakehouse.part07.clicks_v2 WHERE event_type = 'product_view'
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 3;
SELECT variant_get(payload, '$.sku', 'string') AS sku, count(*) AS views
FROM lakehouse.part07.clicks_v3 WHERE event_type = 'product_view'
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 3;

DELETE FROM lakehouse.part07.clicks_v2
WHERE get_json_object(payload, '$.user_agent') RLIKE '(?i)bot|crawl|spider|headless';
DELETE FROM lakehouse.part07.clicks_v3
WHERE variant_get(payload, '$.user_agent', 'string') RLIKE '(?i)bot|crawl|spider|headless';

-- v2 writes a position-delete file (Parquet); v3 writes a deletion vector (a blob in a Puffin file).
SELECT 'v2' AS format, content, file_format, record_count AS deleted_rows, file_size_in_bytes,
       content_size_in_bytes AS blob_bytes
FROM lakehouse.part07.clicks_v2.delete_files
UNION ALL
SELECT 'v3', content, file_format, record_count, file_size_in_bytes, content_size_in_bytes
FROM lakehouse.part07.clicks_v3.delete_files;

-- Row lineage (v3): every row has a stable ID and the sequence number of the commit that last changed it.
SELECT _row_id, _last_updated_sequence_number, event_id FROM lakehouse.part07.clicks_v3 ORDER BY _row_id LIMIT 3;

-- Default values are a v3 feature, but Spark can't set one yet (fails on purpose).
ALTER TABLE lakehouse.part07.clicks_v3 ADD COLUMN consent_version INT DEFAULT 1;
