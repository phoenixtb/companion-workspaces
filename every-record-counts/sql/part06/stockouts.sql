-- Part 6: gaps and islands. Runs of consecutive stock-out days per store and SKU (DuckDB, Q3 2026).
-- An island is a run of days with nothing on the shelf at close. Within a run, date minus row_number is constant.
WITH days AS (
    SELECT s.store_id, p.sku, d.date,
           f.closing_on_hand_qty = 0 AS empty_at_close,   -- Part 5's close-of-day measure
           f.on_hand_qty = 0         AS empty_as_reported  -- the snapshot as the WMS sent it
    FROM star.fct_stock_daily f
    JOIN star.dim_store s USING (store_key)
    JOIN star.dim_product p USING (product_key)
    JOIN star.dim_date d USING (date_key)
    WHERE d.quarter = '2026-Q3'
),
islands AS (
    SELECT store_id, sku, min(date) AS first_day, max(date) AS last_day, count(*) AS run_days
    FROM (
        SELECT *, date - CAST(row_number() OVER (PARTITION BY store_id, sku ORDER BY date) AS INTEGER) AS island
        FROM days WHERE empty_at_close
    )
    GROUP BY store_id, sku, island
)
SELECT CASE WHEN run_days = 1 THEN '1 day' WHEN run_days <= 6 THEN '2-6 days'
            WHEN run_days <= 13 THEN '7-13 days' ELSE '14+ days' END AS run_length,
       count(*)              AS runs,
       sum(run_days)         AS stockout_days,
       max(run_days)         AS longest
FROM islands
GROUP BY run_length
ORDER BY min(run_days);

-- The same islands on the snapshot as reported. 216 stores count after the overnight delivery, so a shelf that
-- sold out and was refilled overnight never shows as empty.
WITH days AS (
    SELECT s.store_id, p.sku, d.date, f.closing_on_hand_qty = 0 AS empty_at_close, f.on_hand_qty = 0 AS empty_as_reported
    FROM star.fct_stock_daily f
    JOIN star.dim_store s USING (store_key)
    JOIN star.dim_product p USING (product_key)
    JOIN star.dim_date d USING (date_key)
    WHERE d.quarter = '2026-Q3'
),
runs AS (
    SELECT measure, store_id, sku, island, count(*) AS run_days
    FROM (
        SELECT 'at close' AS measure, store_id, sku,
               date - CAST(row_number() OVER (PARTITION BY store_id, sku ORDER BY date) AS INTEGER) AS island
        FROM days WHERE empty_at_close
        UNION ALL
        SELECT 'as reported', store_id, sku,
               date - CAST(row_number() OVER (PARTITION BY store_id, sku ORDER BY date) AS INTEGER)
        FROM days WHERE empty_as_reported
    )
    GROUP BY ALL
)
SELECT measure, sum(run_days) AS stockout_days, count(*) AS runs,
       count(*) FILTER (WHERE run_days = 1) AS one_day_runs, count(*) FILTER (WHERE run_days >= 14) AS runs_14_plus
FROM runs
GROUP BY measure
ORDER BY measure DESC;

-- Partition by the natural key. dim_product is Type 2: SKUs moved in the 1 August reorganisation got a new
-- product_key, and partitioning by the surrogate key cuts every run that spans the change.
WITH empty AS (
    SELECT s.store_id, p.sku, f.product_key, d.date
    FROM star.fct_stock_daily f
    JOIN star.dim_store s USING (store_key)
    JOIN star.dim_product p USING (product_key)
    JOIN star.dim_date d USING (date_key)
    WHERE d.quarter = '2026-Q3' AND f.closing_on_hand_qty = 0
),
numbered AS (
    SELECT *,
           date - CAST(row_number() OVER (PARTITION BY store_id, sku ORDER BY date) AS INTEGER)         AS by_sku,
           date - CAST(row_number() OVER (PARTITION BY store_id, product_key ORDER BY date) AS INTEGER) AS by_key
    FROM empty
)
SELECT count(DISTINCT (store_id, sku, by_sku))         AS runs_by_sku,
       count(DISTINCT (store_id, product_key, by_key)) AS runs_by_product_key
FROM numbered;
