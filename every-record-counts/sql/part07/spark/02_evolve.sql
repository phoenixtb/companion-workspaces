-- Part 7, step 3: evolve the schema and the partition spec (Spark SQL). Each statement writes a new metadata
-- file and rewrites no data: columns are tracked by ID, and the new spec applies to files written from now on.
ALTER TABLE lakehouse.part07.sales_line RENAME COLUMN amount_minor TO net_amount_minor;
ALTER TABLE lakehouse.part07.sales_line ADD COLUMN till_id STRING COMMENT 'sent by the tills from October';
ALTER TABLE lakehouse.part07.sales_line ADD PARTITION FIELD country;

SELECT (SELECT count(*) FROM lakehouse.part07.sales_line.metadata_log_entries) AS metadata_files,
       (SELECT count(*) FROM lakehouse.part07.sales_line.snapshots)            AS snapshots,
       (SELECT count(*) FROM lakehouse.part07.sales_line.files)                AS data_files,
       (SELECT count(*) FROM lakehouse.part07.sales_line WHERE till_id IS NULL) AS rows_without_till_id;
