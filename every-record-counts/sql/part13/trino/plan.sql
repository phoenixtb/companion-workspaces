-- Part 13: how Trino cuts Part 11's history query into fragments (stages) connected by exchanges.
EXPLAIN (TYPE DISTRIBUTED)
SELECT CAST(date_trunc('MONTH', business_date) AS DATE) AS month, country, currency, count(*) AS lines,
       sum(amount_minor) AS sales_minor, sum(discount_minor) AS discount_minor
FROM lakehouse.part10.pos_monthly
GROUP BY 1, 2, 3;
