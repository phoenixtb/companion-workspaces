-- Stock on hand is semi-additive: one store-SKU-week from the periodic snapshot fact (DuckDB).
SELECT d.date, d.day_name, f.on_hand_qty
FROM fct_stock_daily f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_store s ON s.store_key = f.store_key
JOIN dim_product p ON p.product_key = f.product_key
WHERE s.store_id = 'GB-0126' AND p.sku = 'SKU-01684' AND d.week_start = DATE '2026-08-10'
ORDER BY d.date;

-- The same week, aggregated four ways. Only the first answers no question.
SELECT sum(f.on_hand_qty)                                AS sum_over_week,
       round(avg(f.on_hand_qty), 1)                      AS avg_daily_on_hand,
       arg_max(f.on_hand_qty, d.date)                    AS closing_on_hand,
       count(*) FILTER (WHERE f.on_hand_qty > 0) || '/' || count(*) AS days_in_stock
FROM fct_stock_daily f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_store s ON s.store_key = f.store_key
JOIN dim_product p ON p.product_key = f.product_key
WHERE s.store_id = 'GB-0126' AND p.sku = 'SKU-01684' AND d.week_start = DATE '2026-08-10';

-- Q3 availability: the dashboard's weekly sum vs counting store-SKU-days. Group by sku, not product_key:
-- a product that changed category has two product_keys, and a week must not split in two.
WITH q3 AS (
    SELECT s.store_id, p.sku, d.iso_week, f.on_hand_qty
    FROM fct_stock_daily f
    JOIN dim_date d ON d.date_key = f.date_key
    JOIN dim_store s ON s.store_key = f.store_key
    JOIN dim_product p ON p.product_key = f.product_key
    WHERE d.quarter = '2026-Q3'
),
weekly AS (
    SELECT store_id, sku, iso_week, sum(on_hand_qty) AS summed_on_hand
    FROM q3 GROUP BY ALL
)
SELECT round(100 * (SELECT avg((summed_on_hand > 0)::INT) FROM weekly), 1) AS dashboard_weekly_sum_pct,
       round(100 * (SELECT avg((on_hand_qty > 0)::INT) FROM q3), 1)        AS store_sku_days_in_stock_pct;
