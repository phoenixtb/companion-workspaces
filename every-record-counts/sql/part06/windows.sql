-- Part 6: window functions over Part 5's star (DuckDB, Q3 2026). `star` is Part 5's database, attached read-only.
-- Statement 1: ranking. Top three SKUs per store by units: how often does a tie straddle the cut-off?
WITH units AS (
    SELECT s.store_id, p.sku, sum(f.qty) AS units
    FROM star.fct_sales_line f
    JOIN star.dim_store s USING (store_key)
    JOIN star.dim_product p USING (product_key)
    JOIN star.dim_date d USING (date_key)
    WHERE d.quarter = '2026-Q3' AND s.store_type = 'store'
    GROUP BY ALL
),
ranked AS (
    SELECT *,
           row_number() OVER w AS row_no,      -- 1, 2, 3, 4: ties broken arbitrarily
           rank()       OVER w AS rnk,         -- 1, 2, 3, 3, 5: ties share, then a gap
           dense_rank() OVER w AS dense_rnk    -- 1, 2, 3, 3, 4: ties share, no gap
    FROM units
    WINDOW w AS (PARTITION BY store_id ORDER BY units DESC)
)
SELECT count(DISTINCT store_id)                                        AS stores,
       count(DISTINCT store_id) FILTER (WHERE row_no = 4 AND rnk <= 3) AS stores_where_row_number_drops_a_tie,
       count(*) FILTER (WHERE row_no <= 3)                             AS rows_top3_row_number,
       count(*) FILTER (WHERE rnk <= 3)                                AS rows_top3_rank
FROM ranked;

-- Statement 2: running totals and the default frame. With ORDER BY and no frame, the frame is RANGE ... CURRENT ROW,
-- which includes every row with the same ORDER BY value (its peers).
WITH lines AS (
    SELECT d.date, f.transaction_id, f.line_no, f.amount_minor
    FROM star.fct_sales_line f
    JOIN star.dim_store s USING (store_key)
    JOIN star.dim_date d USING (date_key)
    WHERE s.store_id = 'GB-0001' AND d.date IN (DATE '2026-07-01', DATE '2026-07-02')
)
SELECT date, transaction_id, line_no, amount_minor,
       sum(amount_minor) OVER (ORDER BY date)                          AS default_frame,
       sum(amount_minor) OVER (ORDER BY date, transaction_id, line_no
                               ROWS UNBOUNDED PRECEDING)               AS rows_frame
FROM lines
ORDER BY date, transaction_id, line_no
LIMIT 4;

-- Statement 3: a 7-day moving sum on a series with missing days. ROWS counts rows; RANGE counts days.
WITH daily AS (
    SELECT d.date, sum(f.qty) AS units
    FROM star.fct_sales_line f
    JOIN star.dim_store s USING (store_key)
    JOIN star.dim_product p USING (product_key)
    JOIN star.dim_date d USING (date_key)
    WHERE s.store_id = 'GB-0001' AND p.sku = 'SKU-00273' AND d.quarter = '2026-Q3'
    GROUP BY d.date
),
moving AS (
    SELECT date,
           date - lag(date) OVER (ORDER BY date)                                       AS days_since_last_sale,
           units,
           sum(units) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)     AS last_7_rows,
           sum(units) OVER (ORDER BY date RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW) AS last_7_days
    FROM daily
)
SELECT * FROM moving WHERE date BETWEEN DATE '2026-08-10' AND DATE '2026-08-20' ORDER BY date;

SELECT count(*) AS days_with_sales, count(*) FILTER (WHERE last_7_rows <> last_7_days) AS days_where_they_differ,
       max(days_in_7_rows) AS most_days_in_7_rows
FROM (
    SELECT date - first_value(date) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) + 1 AS days_in_7_rows,
           sum(units) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7_rows,
           sum(units) OVER (ORDER BY date RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW) AS last_7_days
    FROM (SELECT d.date, sum(f.qty) AS units
          FROM star.fct_sales_line f
          JOIN star.dim_store s USING (store_key)
          JOIN star.dim_product p USING (product_key)
          JOIN star.dim_date d USING (date_key)
          WHERE s.store_id = 'GB-0001' AND p.sku = 'SKU-00273' AND d.quarter = '2026-Q3'
          GROUP BY d.date)
);
