-- Part 28: the sources and views every Part 28 script starts from (passed with sql-client.sh -i).
-- Comments stay on lines of their own, without apostrophes (Part 27: the SQL client file mode).
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.local-time-zone' = 'UTC';

-- Polaris as the catalog, as the job principal flink, with storage keys vended per table (Part 9).
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
-- (`stream` is a reserved word in Flink SQL: scripts write lake.`stream`.table_name.)

-- Till baskets from Part 27, committed transactions only (the Kafka source sets no isolation level itself).
-- Event time is the till clock; the watermark trails the newest basket seen by 5 minutes.
CREATE TABLE pos_basket_events (
  event_id        STRING NOT NULL,
  event_type      STRING NOT NULL,
  schema_version  INT NOT NULL,
  store_id        STRING NOT NULL,
  till_id         STRING NOT NULL,
  basket_id       STRING NOT NULL,
  event_ts        TIMESTAMP(3) NOT NULL,
  uploaded_at     TIMESTAMP(3) NOT NULL,
  currency        STRING NOT NULL,
  loyalty_id      STRING,
  lines   ARRAY<ROW<line_no INT NOT NULL, sku STRING NOT NULL, qty INT NOT NULL, unit_price_minor BIGINT NOT NULL,
                    discount_minor BIGINT NOT NULL, promo_id STRING> NOT NULL> NOT NULL,
  tenders ARRAY<ROW<type STRING NOT NULL, amount_minor BIGINT NOT NULL> NOT NULL> NOT NULL,
  arrived_at AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'larkspur.pos.basket-events',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'scan.watermark.emit.strategy' = 'on-event',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

-- Every shelf-price version, oldest first per SKU. Event time is when the price takes effect.
CREATE TABLE price_changes (
  sku             STRING NOT NULL,
  country         STRING NOT NULL,
  currency        STRING NOT NULL,
  price_minor     BIGINT NOT NULL,
  effective_from  TIMESTAMP(3) NOT NULL,
  WATERMARK FOR effective_from AS effective_from
) WITH (
  'connector' = 'kafka',
  'topic' = 'larkspur.catalog.price-changes',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

-- Dedup: the first copy of each basket to arrive, as Part 21 keeps the first upload. Ordered by arrival (processing
-- time), not event time: keep-first on event time emits only once the watermark passes and drops late baskets.
CREATE VIEW baskets AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY basket_id ORDER BY arrived_at ASC) AS rn FROM pos_basket_events)
WHERE rn = 1;

-- A versioned table from an append-only topic: latest row per key on event time, the pattern Flink documents.
-- Key (sku, country) becomes its primary key, effective_from its version time.
CREATE VIEW price_versions AS
SELECT sku, country, currency, price_minor, effective_from FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY sku, country ORDER BY effective_from DESC) AS rn FROM price_changes)
WHERE rn = 1;

-- One row per basket line, with the shelf price in force when the till sold it (event-time temporal join).
-- business_date is the store-local date (Part 5); the three countries and their zones.
CREATE VIEW sales_lines AS
SELECT b.event_id, b.basket_id, b.store_id, b.till_id, SUBSTRING(b.store_id, 1, 2) AS country, b.currency,
       CAST(SUBSTRING(CONVERT_TZ(DATE_FORMAT(b.event_ts, 'yyyy-MM-dd HH:mm:ss'), 'UTC',
            CASE SUBSTRING(b.store_id, 1, 2) WHEN 'GB' THEN 'Europe/London' WHEN 'IN' THEN 'Asia/Kolkata'
                 ELSE 'Asia/Singapore' END), 1, 10) AS DATE) AS business_date,
       b.event_ts, b.uploaded_at, l.line_no, l.sku, l.qty, l.unit_price_minor, l.discount_minor,
       l.qty * l.unit_price_minor - l.discount_minor AS net_minor,
       p.price_minor AS list_price_minor, p.effective_from AS price_from
FROM baskets AS b
CROSS JOIN UNNEST(b.lines) AS l (line_no, sku, qty, unit_price_minor, discount_minor, promo_id)
LEFT JOIN price_versions FOR SYSTEM_TIME AS OF b.event_ts AS p
  ON p.sku = l.sku AND p.country = SUBSTRING(b.store_id, 1, 2);
