-- Part 28: a mistake and how Flink answers it, before any data moves. Run with: python -m scripts.part28_flink errors
SET 'execution.runtime-mode' = 'streaming';

-- 1. An updating query (a running total) into an insert-only sink: the plain kafka connector.
CREATE TABLE store_totals_append (
  store_id STRING,
  sales_minor BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'larkspur.analytics.top-stores',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);
INSERT INTO store_totals_append SELECT store_id, SUM(net_minor) FROM sales_lines GROUP BY store_id;
