-- Part 10's promotions question: one store's discounted lines over the last year, by month.
-- CAST(... AS DATE): Spark's date_trunc returns a TIMESTAMP, DuckDB's a DATE; the cast makes the answers comparable.
SELECT CAST(date_trunc('MONTH', business_date) AS DATE) AS month, count(*) AS promo_lines,
       sum(discount_minor) AS discount_minor
FROM {table}
WHERE store_id = 'GB-0042' AND discount_minor > 0 AND business_date >= DATE '2025-10-01'
GROUP BY 1
