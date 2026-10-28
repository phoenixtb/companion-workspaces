-- Part 8: the same gold table as two Delta Lake tables (Spark SQL, Delta 4.4.1), on SeaweedFS (S3A).
-- delta_dv: deletion vectors on. delta_uniform: UniForm, so Delta also writes Iceberg metadata (no deletion vectors).
CREATE DATABASE IF NOT EXISTS part08 LOCATION 's3a://larkspur-lake/delta/part08.db';

CREATE TEMPORARY VIEW staged USING parquet OPTIONS (path '/opt/lakehouse/lab/part08/daily_store_sales.parquet');

CREATE TABLE part08.delta_dv (
    store_id STRING, business_date DATE, currency STRING, baskets BIGINT, sales_minor BIGINT
) USING delta
LOCATION 's3a://larkspur-lake/delta/part08/delta_dv'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE TABLE part08.delta_uniform (
    store_id STRING, business_date DATE, currency STRING, baskets BIGINT, sales_minor BIGINT
) USING delta
LOCATION 's3a://larkspur-lake/delta/part08/delta_uniform'
TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

INSERT INTO part08.delta_dv SELECT * FROM staged;
INSERT INTO part08.delta_uniform SELECT * FROM staged;

SHOW TBLPROPERTIES part08.delta_uniform;
