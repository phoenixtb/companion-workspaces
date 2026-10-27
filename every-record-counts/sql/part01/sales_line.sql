-- One row per POS basket line: the table every engine in Part 1 is asked about (DuckDB).
-- Re-sent events are dropped (one row per basket_id); the business date is the store's local date.
CREATE OR REPLACE TABLE sales_line AS
WITH baskets AS (
    SELECT DISTINCT ON (basket_id) * FROM raw_pos_events ORDER BY basket_id, uploaded_at
)
SELECT b.basket_id,
       l.line_no,
       b.store_id,
       s.country,
       s.format                                     AS store_format,
       b.currency,
       b.event_ts,
       (b.event_ts AT TIME ZONE s.timezone)::DATE   AS business_date,
       l.sku,
       l.qty,
       l.unit_price_minor,
       l.discount_minor,
       l.qty * l.unit_price_minor - l.discount_minor AS amount_minor
FROM baskets b,
     unnest(b.lines) AS t(l)
JOIN raw_stores s ON s.store_id = b.store_id
ORDER BY b.basket_id, l.line_no;
