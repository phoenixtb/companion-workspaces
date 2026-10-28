-- Revenue for one month, by country: the finance question. {table} is filled in by spark/jobs/part10_measure.py.
SELECT country, currency, count(*) AS lines, sum(amount_minor) AS sales_minor FROM {table}
WHERE business_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30' GROUP BY country, currency
