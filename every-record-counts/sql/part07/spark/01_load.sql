-- Part 7, step 1: create the sales-line table through Polaris and load every till upload up to 30 September
-- (Spark SQL). The staged Parquet files hold every event, re-sends included; one row per (basket, line) is kept.
CREATE TEMPORARY VIEW pos_lines USING parquet OPTIONS (path '/opt/lakehouse/lab/part07/pos_lines');

CREATE NAMESPACE IF NOT EXISTS lakehouse.part07;

CREATE TABLE lakehouse.part07.sales_line (
    basket_id        STRING    NOT NULL,
    line_no          INT       NOT NULL,
    store_id         STRING    NOT NULL,
    country          STRING    NOT NULL,
    currency         STRING    NOT NULL,
    event_ts         TIMESTAMP NOT NULL COMMENT 'when the till rang it up (UTC)',
    business_date    DATE      NOT NULL COMMENT 'the store''s local date',
    sku              STRING    NOT NULL,
    qty              INT       NOT NULL,
    unit_price_minor BIGINT    NOT NULL,
    discount_minor   BIGINT    NOT NULL,
    amount_minor     BIGINT    NOT NULL,
    uploaded_at      TIMESTAMP NOT NULL COMMENT 'when the upload landed (ingestion time)'
)
USING iceberg
PARTITIONED BY (days(event_ts));

INSERT INTO lakehouse.part07.sales_line
SELECT basket_id, CAST(line_no AS INT), store_id, country, currency, event_ts, business_date, sku, CAST(qty AS INT),
       unit_price_minor, discount_minor, amount_minor, uploaded_at
FROM (SELECT *, row_number() OVER (PARTITION BY basket_id, line_no ORDER BY uploaded_at) AS copy_no
      FROM pos_lines
      WHERE upload_date <= DATE '2026-09-30')
WHERE copy_no = 1;

-- Finance closes the quarter on this snapshot: name it. A tag without RETAIN is kept until someone drops it,
-- whatever snapshot expiry does; "as filed" numbers may be asked for years later (Part 5).
ALTER TABLE lakehouse.part07.sales_line CREATE TAG q3_close;

SELECT count(*) AS rows, count(DISTINCT basket_id) AS baskets, count(DISTINCT days(event_ts)) AS utc_days
FROM lakehouse.part07.sales_line;
