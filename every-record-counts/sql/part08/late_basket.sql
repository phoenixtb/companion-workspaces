-- Part 8: one late £19.99 basket restates one store-day (DuckDB and Spark SQL; {table} and {store_id} are filled in
-- by scripts/part08_formats.py and spark/jobs/part08_late_baskets.py).
UPDATE {table}
SET baskets = baskets + 1, sales_minor = sales_minor + 1999
WHERE store_id = '{store_id}' AND business_date = DATE '2026-09-30'
