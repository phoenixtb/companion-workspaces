-- Part 28: a mistake and how Flink answers it, before any data moves. Run with: python -m scripts.part28_flink errors
SET 'execution.runtime-mode' = 'streaming';

-- 2. An upsert sink without a primary key.
CREATE TABLE store_totals_nokey (
  store_id STRING,
  sales_minor BIGINT
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'larkspur.analytics.top-stores',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);
INSERT INTO store_totals_nokey SELECT store_id, SUM(net_minor) FROM sales_lines GROUP BY store_id;
