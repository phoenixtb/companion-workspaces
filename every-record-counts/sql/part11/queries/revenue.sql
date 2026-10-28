-- Part 10's finance question: one month of revenue by country. {table} is filled in by the runner.
-- Runs unchanged in DuckDB and Spark SQL; scripts/part11_single_node.py has the Polars version.
SELECT country, currency, count(*) AS lines, sum(amount_minor) AS sales_minor
FROM {table}
WHERE business_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
GROUP BY country, currency
