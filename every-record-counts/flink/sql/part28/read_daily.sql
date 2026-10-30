-- Part 28: the upsert table as a reader sees it: equality deletes applied, one row per store and business date.
-- Run with: python -m scripts.part28_flink reconcile (PyIceberg 0.12 cannot read equality deletes yet).
SET 'execution.runtime-mode' = 'batch';
SELECT store_id, business_date, currency, baskets, units_out, sales_minor FROM lake.`stream`.store_sales_daily;
