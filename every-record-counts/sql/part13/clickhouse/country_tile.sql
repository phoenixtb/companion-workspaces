-- Part 13: the tile on every regional dashboard: yesterday's sales by country, from the serving table.
SELECT country, currency, sum(lines) AS lines, sum(sales_minor) AS sales_minor
FROM serve.daily_store_sales
WHERE business_date = DATE '2026-10-03'
GROUP BY country, currency
ORDER BY country;
