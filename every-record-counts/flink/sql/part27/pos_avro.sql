-- Part 27: Flink SQL reads what the Java producers wrote: Avro framed by the Apicurio serializer, schemas fetched by ID
-- from the Confluent-compatible API of Apicurio. Run with: make flink-sql SQL=part27/pos_avro.sql (after the POS producer).
-- Comments stay on lines of their own, without apostrophes: in file mode, a comment after a semicolon or a quote
-- inside a comment merges two statements, and the SQL client answers only single statement supported.
SET 'sql-client.execution.result-mode' = 'tableau';
-- Batch mode: a bounded replay of the topic, one final answer.
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';

CREATE TABLE pos_basket_events (
  event_id        STRING NOT NULL,
  event_type      STRING NOT NULL,
  schema_version  INT NOT NULL,
  store_id        STRING NOT NULL,
  till_id         STRING NOT NULL,
  basket_id       STRING NOT NULL,
  event_ts        TIMESTAMP(3) NOT NULL,   -- the till clock, UTC
  uploaded_at     TIMESTAMP(3) NOT NULL,   -- the upload clock, UTC
  currency        STRING NOT NULL,
  loyalty_id      STRING,
  lines   ARRAY<ROW<line_no INT NOT NULL, sku STRING NOT NULL, qty INT NOT NULL, unit_price_minor BIGINT NOT NULL,
                    discount_minor BIGINT NOT NULL, promo_id STRING> NOT NULL> NOT NULL,
  tenders ARRAY<ROW<type STRING NOT NULL, amount_minor BIGINT NOT NULL> NOT NULL> NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = 'larkspur.pos.basket-events',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

-- The same table read the other way: only records of committed transactions.
CREATE TABLE pos_committed WITH ('properties.isolation.level' = 'read_committed') LIKE pos_basket_events;

SELECT 'as declared above' AS reader, COUNT(*) AS events, COUNT(DISTINCT event_id) AS event_ids,
       COUNT(DISTINCT basket_id) AS baskets
FROM pos_basket_events
UNION ALL
SELECT 'read_committed', COUNT(*), COUNT(DISTINCT event_id), COUNT(DISTINCT basket_id)
FROM pos_committed;
