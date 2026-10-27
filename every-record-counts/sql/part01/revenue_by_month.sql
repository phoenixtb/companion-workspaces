-- Q3 sales by country and month. The same SQL runs on PostgreSQL, DuckDB and Trino;
-- only the table name changes (scripts/part01_compare.py fills in {sales_line}).
SELECT country,
       currency,
       date_trunc('month', business_date) AS month,
       count(*)                           AS lines,
       sum(amount_minor)                  AS sales_minor
FROM {sales_line}
WHERE business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
GROUP BY country, currency, date_trunc('month', business_date)
ORDER BY country, month
