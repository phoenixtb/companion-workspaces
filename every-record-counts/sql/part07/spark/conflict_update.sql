-- Part 7: the other writer in the conflict demo. Finance corrects one store-day in the gold table (Spark SQL).
UPDATE lakehouse.part07.daily_store_sales_mor
SET sales_minor = sales_minor - 100
WHERE store_id = 'GB-0042' AND business_date = DATE '2026-09-30';
