-- Part 28: what updating queries emit. One store's running total, then the top three stores by sales in each
-- country on 15 August, printed as a changelog and written to an upsert topic.
-- Run with: python -m scripts.part28_flink changelog (sql-client.sh -i tables.sql -f changelog.sql).
SET 'execution.runtime-mode' = 'streaming';
-- Each INSERT waits for its job to finish before the next statement runs.
SET 'table.dml-sync' = 'true';

CREATE VIEW top_stores_query AS
SELECT country, store_id, sales_minor, baskets FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY sales_minor DESC) AS rn FROM (
    SELECT country, store_id, SUM(net_minor) AS sales_minor, COUNT(DISTINCT basket_id) AS baskets
    FROM sales_lines
    WHERE business_date = DATE '2026-08-15'
    GROUP BY country, store_id))
WHERE rn <= 3;

-- One store, one business date: every new line updates the running total (the op column).
SELECT store_id, COUNT(DISTINCT basket_id) AS baskets, SUM(net_minor) AS sales_minor
FROM sales_lines
WHERE store_id = 'SG-0042' AND business_date = DATE '2026-08-15'
GROUP BY store_id;

-- The top three, change by change.
SELECT * FROM top_stores_query;

-- The same changes into a compacted topic: an upsert per key, a tombstone (null value) for a delete.
CREATE TABLE top_stores (
  country STRING NOT NULL,
  store_id STRING NOT NULL,
  sales_minor BIGINT,
  baskets BIGINT,
  PRIMARY KEY (country, store_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'larkspur.analytics.top-stores',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

INSERT INTO top_stores SELECT * FROM top_stores_query;

