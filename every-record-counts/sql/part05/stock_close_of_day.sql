-- Part 5: when was each store's stock counted? (DuckDB, Q3 2026)
SELECT s.wms_snapshot_schedule,
       strftime(f.snapshot_ts AT TIME ZONE s.timezone, '%H:%M')       AS local_time,
       (f.snapshot_ts AT TIME ZONE s.timezone)::DATE - d.date         AS days_after_business_date,
       count(DISTINCT s.store_id)                                     AS stores
FROM fct_stock_daily f
JOIN dim_store s USING (store_key)
JOIN dim_date d USING (date_key)
WHERE d.quarter = '2026-Q3'
GROUP BY ALL
ORDER BY ALL;

-- One store-SKU where the morning count hid an empty shelf.
WITH days AS (
    SELECT s.store_id, p.sku, d.date, f.snapshot_ts AT TIME ZONE s.timezone AS counted_at_local,
           f.on_hand_qty, f.received_qty, f.closing_on_hand_qty
    FROM fct_stock_daily f
    JOIN dim_store s USING (store_key)
    JOIN dim_product p USING (product_key)
    JOIN dim_date d USING (date_key)
    WHERE s.country = 'GB' AND d.quarter = '2026-Q3'
),
first_case AS (
    SELECT store_id, sku, date FROM days WHERE on_hand_qty > 0 AND closing_on_hand_qty = 0
    ORDER BY store_id, sku, date LIMIT 1
)
SELECT days.*
FROM days JOIN first_case c USING (store_id, sku)
WHERE days.date BETWEEN c.date - 1 AND c.date + 1
ORDER BY days.date;

-- Q3 availability per store-SKU-day: the snapshot as reported vs stock at the close of the business day.
SELECT round(100 * avg((on_hand_qty > 0)::INT), 1)         AS per_day_as_reported_pct,
       round(100 * avg((closing_on_hand_qty > 0)::INT), 1) AS per_day_at_close_pct
FROM fct_stock_daily f
JOIN dim_date d USING (date_key)
WHERE d.quarter = '2026-Q3';
