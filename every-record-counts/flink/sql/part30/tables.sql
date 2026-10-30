-- Part 30: the live POS topic and the deduplicated sale lines, shared by the minute-tier jobs (sql-client.sh -i).
-- Comments stay on lines of their own, without apostrophes (Part 27: the SQL client file mode).
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.local-time-zone' = 'UTC';

-- Polaris as the catalog, as the job principal flink, with storage keys vended per table (Part 28).
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://polaris:8181/api/catalog',
  'warehouse' = 'larkspur',
  'credential' = 'flink:flink-lab-secret',
  'scope' = 'PRINCIPAL_ROLE:ALL',
  'header.X-Iceberg-Access-Delegation' = 'vended-credentials',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true',
  'client.region' = 'us-east-1'
);

-- The live replay (scripts.part30_tiers live): till baskets as they are uploaded. Unbounded.
-- produced_at is the Kafka record timestamp the producer set when it sent the basket: every tier measures from it.
CREATE TABLE pos_live (
  event_id        STRING NOT NULL,
  store_id        STRING NOT NULL,
  till_id         STRING NOT NULL,
  basket_id       STRING NOT NULL,
  event_ts        TIMESTAMP(3) NOT NULL,
  uploaded_at     TIMESTAMP(3) NOT NULL,
  lines   ARRAY<ROW<line_no INT NOT NULL, sku STRING NOT NULL, qty INT NOT NULL, unit_price_minor BIGINT NOT NULL,
                    discount_minor BIGINT NOT NULL, promo_id STRING> NOT NULL> NOT NULL,
  produced_at TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  arrived_at AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'larkspur.pos.live',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-stock-minute',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

-- The first copy of each basket to arrive (Part 28), one row per line, the store-local business date (Part 5).
CREATE VIEW stock_lines AS
SELECT b.basket_id, l.line_no, b.store_id, l.sku, l.qty,
       CAST(SUBSTRING(CONVERT_TZ(DATE_FORMAT(b.event_ts, 'yyyy-MM-dd HH:mm:ss'), 'UTC',
            CASE SUBSTRING(b.store_id, 1, 2) WHEN 'GB' THEN 'Europe/London' WHEN 'IN' THEN 'Asia/Kolkata'
                 ELSE 'Asia/Singapore' END), 1, 10) AS DATE) AS business_date,
       b.event_ts, CAST(b.produced_at AS TIMESTAMP(3)) AS produced_at
FROM (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY basket_id ORDER BY arrived_at ASC) AS rn FROM pos_live)
  WHERE rn = 1) AS b
CROSS JOIN UNNEST(b.lines) AS l (line_no, sku, qty, unit_price_minor, discount_minor, promo_id);
