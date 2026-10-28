-- Part 10, step 1: four years of POS lines, written as three layouts of the same table (Spark SQL).
-- The lab has 126 business days of tills (1 June to 4 October 2026). Twelve copies, each shifted back by
-- another 126 days (18 weeks, so weekdays stay put), with 4 October held back (step 5 appends it in
-- micro-batches), make 1,511 days of history: ~72.6 million lines.
CREATE TEMPORARY VIEW pos_lines USING parquet OPTIONS (path '/opt/lakehouse/lab/part07/pos_lines');

CREATE NAMESPACE IF NOT EXISTS lakehouse.part10;

CREATE TEMPORARY VIEW lines AS
SELECT basket_id, CAST(line_no AS INT) AS line_no, store_id, country, currency, event_ts, business_date, sku,
       CAST(qty AS INT) AS qty, unit_price_minor, discount_minor, amount_minor, uploaded_at
FROM (SELECT *, row_number() OVER (PARTITION BY basket_id, line_no ORDER BY uploaded_at) AS copy_no FROM pos_lines)
WHERE copy_no = 1;

CREATE TEMPORARY VIEW history AS
SELECT concat(substr(basket_id, 1, 8),
              date_format(date_sub(to_date(substr(basket_id, 9, 8), 'yyyyMMdd'), 126 * k), 'yyyyMMdd'),
              substr(basket_id, 17))                AS basket_id,   -- GB-0042-20260815-00009, re-dated
       line_no, store_id, country, currency,
       event_ts - make_dt_interval(126 * k)       AS event_ts,
       date_sub(business_date, 126 * k)            AS business_date,
       sku, qty, unit_price_minor, discount_minor, amount_minor,
       uploaded_at - make_dt_interval(126 * k)    AS uploaded_at
FROM lines LATERAL VIEW explode(sequence(0, 11)) copies AS k
WHERE NOT (k = 0 AND business_date = DATE '2026-10-04');

-- The lab holds ~1/50 of Larkspur's real volume, so file and row-group targets are scaled down too (1/64:
-- 512 MiB -> 8 MiB files, 128 MiB -> 2 MiB row groups). File counts then look like the full-size table's.

-- Layout A: no partitioning, no sort order. Rows land in whatever order the job produces them.
CREATE TABLE lakehouse.part10.pos_flat
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '8388608', 'write.parquet.row-group-size-bytes' = '2097152')
AS SELECT * FROM history;

-- Layout B: one partition per business day (the store's local date), 1,511 of them.
CREATE TABLE lakehouse.part10.pos_daily
USING iceberg
PARTITIONED BY (business_date)
TBLPROPERTIES ('write.target-file-size-bytes' = '8388608', 'write.parquet.row-group-size-bytes' = '2097152',
               'write.spark.fanout.enabled' = 'false')   -- sort within each task instead of one open file per day
AS SELECT * FROM lakehouse.part10.pos_flat;

-- Layout C: one partition per month, rows sorted by store, then date, within it.
CREATE TABLE lakehouse.part10.pos_monthly
USING iceberg
PARTITIONED BY (months(business_date))
TBLPROPERTIES ('write.target-file-size-bytes' = '8388608', 'write.parquet.row-group-size-bytes' = '2097152')
AS SELECT * FROM lakehouse.part10.pos_flat WHERE false;

ALTER TABLE lakehouse.part10.pos_monthly WRITE ORDERED BY store_id, business_date;

INSERT INTO lakehouse.part10.pos_monthly SELECT * FROM lakehouse.part10.pos_flat;

SELECT count(*) AS rows, min(business_date) AS first_day, max(business_date) AS last_day,
       count(DISTINCT business_date) AS days
FROM lakehouse.part10.pos_flat;
