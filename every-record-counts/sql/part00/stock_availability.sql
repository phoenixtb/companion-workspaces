-- Stock availability, Q3 2026: the stores dashboard's number vs empty shelves at closing (DuckDB).
WITH q3 AS (
    SELECT s.*, st.wms_snapshot_schedule
    FROM raw_stock_snapshots s
    JOIN raw_stores st USING (store_id)
    WHERE s.business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
),
dashboard AS (              -- the dashboard: was there any stock that week?
    SELECT avg((weekly_on_hand > 0)::INT) AS availability
    FROM (SELECT store_id, sku, week(business_date) AS wk, sum(on_hand_qty) AS weekly_on_hand
          FROM q3 GROUP BY ALL)
),
per_day AS (                -- per day, trusting each store's snapshot as reported
    SELECT avg((on_hand_qty > 0)::INT) AS availability FROM q3
),
shelf AS (                  -- the shelf: was there stock at closing, each day?
    SELECT avg((CASE WHEN wms_snapshot_schedule = 'after_delivery_06_00'
                     THEN on_hand_qty - received_qty   -- these stores count after the overnight delivery
                     ELSE on_hand_qty END > 0)::INT) AS availability
    FROM q3
)
SELECT round(dashboard.availability * 100, 1) AS dashboard_pct,
       round(per_day.availability * 100, 1)   AS per_day_snapshot_pct,
       round(shelf.availability * 100, 1)     AS shelf_at_close_pct
FROM dashboard, per_day, shelf;
