-- The revenue view over all four years: every row, by month, country and currency (a full scan).
SELECT CAST(date_trunc('MONTH', business_date) AS DATE) AS month, country, currency, count(*) AS lines,
       sum(amount_minor) AS sales_minor, sum(discount_minor) AS discount_minor
FROM {table}
GROUP BY 1, 2, 3
