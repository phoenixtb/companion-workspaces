-- Part 13: a store manager's dashboard tile, the last four weeks of daily sales for one store, from the lines.
-- Runs unchanged in Trino and ClickHouse; {table} and {store} are filled in by scripts/part13_engines.py.
SELECT business_date, count(*) AS lines, sum(amount_minor) AS sales_minor
FROM {table}
WHERE store_id = '{store}' AND business_date BETWEEN DATE '2026-09-03' AND DATE '2026-09-30'
GROUP BY business_date
ORDER BY business_date
