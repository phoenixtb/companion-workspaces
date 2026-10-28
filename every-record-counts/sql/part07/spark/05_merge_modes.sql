-- Part 7, step 6: the same MERGE into two copies of a daily store-sales table, one copy-on-write and one
-- merge-on-read (Spark SQL). The gold table is built from Q3 as closed, then the store-days the 1 October upload
-- touched are recomputed and upserted, as in Part 6's step 8.
CREATE TABLE lakehouse.part07.daily_store_sales_cow (
    store_id STRING, business_date DATE, currency STRING, baskets BIGINT, sales_minor BIGINT
) USING iceberg
PARTITIONED BY (months(business_date))
TBLPROPERTIES ('write.merge.mode' = 'copy-on-write');

CREATE TABLE lakehouse.part07.daily_store_sales_mor (
    store_id STRING, business_date DATE, currency STRING, baskets BIGINT, sales_minor BIGINT
) USING iceberg
PARTITIONED BY (months(business_date))
TBLPROPERTIES ('write.merge.mode' = 'merge-on-read');

CREATE TEMPORARY VIEW closed_q3 AS
SELECT store_id, business_date, currency, count(DISTINCT basket_id) AS baskets, sum(amount_minor) AS sales_minor
FROM lakehouse.part07.sales_line VERSION AS OF 'q3_close'
GROUP BY store_id, business_date, currency;

INSERT INTO lakehouse.part07.daily_store_sales_cow SELECT * FROM closed_q3;
INSERT INTO lakehouse.part07.daily_store_sales_mor SELECT * FROM closed_q3;

CREATE TEMPORARY VIEW touched AS
SELECT store_id, business_date, currency, count(DISTINCT basket_id) AS baskets, sum(net_amount_minor) AS sales_minor
FROM lakehouse.part07.sales_line
WHERE (store_id, business_date) IN (SELECT store_id, business_date FROM lakehouse.part07.sales_line
                                     WHERE uploaded_at >= TIMESTAMP '2026-10-01 00:00:00'
                                       AND uploaded_at <  TIMESTAMP '2026-10-02 00:00:00')
GROUP BY store_id, business_date, currency;

MERGE INTO lakehouse.part07.daily_store_sales_cow t USING touched s
ON t.store_id = s.store_id AND t.business_date = s.business_date
WHEN MATCHED AND (t.baskets <> s.baskets OR t.sales_minor <> s.sales_minor) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO lakehouse.part07.daily_store_sales_mor t USING touched s
ON t.store_id = s.store_id AND t.business_date = s.business_date
WHEN MATCHED AND (t.baskets <> s.baskets OR t.sales_minor <> s.sales_minor) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- What each MERGE commit did, from the snapshot summaries (Spark adds its MERGE row counts to them).
SELECT 'copy-on-write' AS mode, s.summary['spark.merge-into.num-target-rows-updated'] AS updated,
       s.summary['spark.merge-into.num-target-rows-inserted'] AS inserted,
       s.summary['spark.merge-into.num-target-rows-copied'] AS copied,
       s.summary['deleted-data-files'] AS removed_files, s.summary['added-data-files'] AS added_files,
       s.summary['added-delete-files'] AS delete_files, s.summary['added-files-size'] AS bytes_written
FROM lakehouse.part07.daily_store_sales_cow.snapshots s WHERE s.operation = 'overwrite'
UNION ALL
SELECT 'merge-on-read', s.summary['spark.merge-into.num-target-rows-updated'],
       s.summary['spark.merge-into.num-target-rows-inserted'], s.summary['spark.merge-into.num-target-rows-copied'],
       s.summary['deleted-data-files'], s.summary['added-data-files'], s.summary['added-delete-files'],
       s.summary['added-files-size']
FROM lakehouse.part07.daily_store_sales_mor.snapshots s WHERE s.operation = 'overwrite';

-- The manifest entries of the MERGE snapshot: status 1 = added, 2 = deleted; content 0 = data, 1 = position deletes.
SELECT 'copy-on-write' AS mode, e.status, e.data_file.content, e.data_file.partition.business_date_month AS month,
       e.data_file.record_count, e.data_file.file_size_in_bytes AS bytes
FROM lakehouse.part07.daily_store_sales_cow.entries e
WHERE e.snapshot_id = (SELECT max_by(snapshot_id, committed_at) FROM lakehouse.part07.daily_store_sales_cow.snapshots)
UNION ALL
SELECT 'merge-on-read', e.status, e.data_file.content, e.data_file.partition.business_date_month,
       e.data_file.record_count, e.data_file.file_size_in_bytes
FROM lakehouse.part07.daily_store_sales_mor.entries e
WHERE e.snapshot_id = (SELECT max_by(snapshot_id, committed_at) FROM lakehouse.part07.daily_store_sales_mor.snapshots)
ORDER BY 1, 2, 3, 4;

-- Same answer either way.
SELECT 'copy-on-write' AS mode, count(*) AS store_days, sum(baskets) AS baskets, sum(sales_minor) AS sales_minor
FROM lakehouse.part07.daily_store_sales_cow
UNION ALL
SELECT 'merge-on-read', count(*), sum(baskets), sum(sales_minor) FROM lakehouse.part07.daily_store_sales_mor;
