-- The same correction against a folder of Parquet files (DuckDB).
UPDATE read_parquet('s3://larkspur-lake/parquet/sales_line/*.parquet')
SET amount_minor = amount_minor - 100, discount_minor = discount_minor + 100
WHERE basket_id = 'GB-0042-20260815-00009' AND line_no = 1;
