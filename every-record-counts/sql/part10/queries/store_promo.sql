-- One store's discounted lines over the last year, by month: the promotions team's question.
SELECT date_trunc('MONTH', business_date) AS month, count(*) AS promo_lines, sum(discount_minor) AS discount_minor
FROM {table} WHERE store_id = 'GB-0042' AND discount_minor > 0 AND business_date >= DATE '2025-10-01' GROUP BY 1
