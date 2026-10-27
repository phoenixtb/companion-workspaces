-- One business day's sales by country: the filter most finance questions start with.
SELECT country,
       currency,
       count(*)          AS lines,
       sum(amount_minor) AS sales_minor
FROM {sales_line}
WHERE business_date = DATE '2026-08-15'
GROUP BY country, currency
ORDER BY country
