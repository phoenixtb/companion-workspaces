-- Part 13: the same tile from the serving table (one row per store and day, kept by a materialized view).
SELECT business_date, sum(lines) AS lines, sum(sales_minor) AS sales_minor
FROM {table}
WHERE store_id = '{store}' AND business_date BETWEEN DATE '2026-09-03' AND DATE '2026-09-30'
GROUP BY business_date
ORDER BY business_date
