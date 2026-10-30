-- Part 30, the seconds tier: ClickHouse reads the live POS topic itself (Kafka table engine) and a materialized view
-- writes every basket line into MergeTree as it arrives. Run by scripts.part30_tiers setup, statement by statement.
DROP DATABASE IF EXISTS realtime;
CREATE DATABASE realtime;

-- Opening stock for business day 15 August (Part 29's WMS file), loaded once.
CREATE TABLE realtime.opening_stock (store_id String, sku String, opening_qty Int32)
ENGINE = MergeTree ORDER BY (store_id, sku);

-- Where the lines land. Re-sent baskets repeat (basket_id, line_no): ReplacingMergeTree removes the copies when it
-- merges parts, some time later; queries that must be exact now read it with FINAL.
CREATE TABLE realtime.sale_lines (
    basket_id String, line_no Int32, store_id String, sku String, qty Int32, business_date Date,
    event_ts DateTime64(3, 'UTC'),
    produced_at DateTime64(3, 'UTC'),   -- the Kafka record timestamp: when the producer sent the basket
    inserted_at DateTime64(3, 'UTC')    -- when this block was written: the line is queryable from here on
) ENGINE = ReplacingMergeTree ORDER BY (store_id, sku, basket_id, line_no);

-- The same lines from a second consumer that keeps the engine's default flush interval (stream_flush_interval_ms).
CREATE TABLE realtime.sale_lines_default AS realtime.sale_lines;

-- A Kafka table is a consumer, not storage: each SELECT (here, the materialized views) reads new messages.
-- AvroConfluent: magic byte, 4-byte schema ID, Avro body; the writer schema comes from Apicurio's ccompat API.
CREATE TABLE realtime.pos_queue (
    basket_id String, store_id String, event_ts DateTime64(3, 'UTC'),
    lines Array(Tuple(line_no Int32, sku String, qty Int32, unit_price_minor Int64, discount_minor Int64,
                      promo_id Nullable(String)))
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'larkspur.pos.live',
         kafka_group_name = 'clickhouse-stock', kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://apicurio:8080/apis/ccompat/v7',
         kafka_poll_timeout_ms = 100,      -- default stream_poll_timeout_ms, 500
         kafka_flush_interval_ms = 250;    -- default stream_flush_interval_ms, 7500: a block (and a part) per flush

CREATE TABLE realtime.pos_queue_default AS realtime.pos_queue
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'larkspur.pos.live',
         kafka_group_name = 'clickhouse-stock-default', kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://apicurio:8080/apis/ccompat/v7';

-- One row per line, the store-local business date (Part 5). now64(3) is evaluated per inserted block.
CREATE MATERIALIZED VIEW realtime.sale_lines_mv TO realtime.sale_lines AS
SELECT basket_id, l.line_no AS line_no, store_id, l.sku AS sku, l.qty AS qty,
       multiIf(startsWith(store_id, 'GB'), toDate(event_ts, 'Europe/London'),
               startsWith(store_id, 'IN'), toDate(event_ts, 'Asia/Kolkata'),
               toDate(event_ts, 'Asia/Singapore')) AS business_date,
       event_ts, _timestamp_ms AS produced_at, now64(3, 'UTC') AS inserted_at
FROM realtime.pos_queue ARRAY JOIN lines AS l;

CREATE MATERIALIZED VIEW realtime.sale_lines_default_mv TO realtime.sale_lines_default AS
SELECT basket_id, l.line_no AS line_no, store_id, l.sku AS sku, l.qty AS qty,
       multiIf(startsWith(store_id, 'GB'), toDate(event_ts, 'Europe/London'),
               startsWith(store_id, 'IN'), toDate(event_ts, 'Asia/Kolkata'),
               toDate(event_ts, 'Asia/Singapore')) AS business_date,
       event_ts, _timestamp_ms AS produced_at, now64(3, 'UTC') AS inserted_at
FROM realtime.pos_queue_default ARRAY JOIN lines AS l;
