-- Part 29: the batch twin of the stream's input, from the raw zone (DuckDB). The POS topic holds upload day
-- getvariable('upload_day') (Part 27). Same rules as the job: the first copy of each basket by arrival (re-sends come
-- 1-30 minutes later), one row per line, the store's local business date, and "late" = more than the job's 5-minute
-- watermark bound behind the sale when it arrived. In this replay uploads come within 90 seconds or after 2 hours.
CREATE OR REPLACE VIEW sale_lines AS
WITH events AS (
    SELECT * FROM raw_pos_events WHERE upload_date = getvariable('upload_day')::DATE
), first_copy AS (
    SELECT * FROM events QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) = 1
)
SELECT f.store_id, l.sku, l.qty, f.basket_id, f.event_ts::TIMESTAMPTZ AS event_ts,
       f.uploaded_at::TIMESTAMPTZ AS uploaded_at,
       (f.event_ts::TIMESTAMPTZ AT TIME ZONE s.timezone)::DATE AS business_date,
       f.uploaded_at::TIMESTAMPTZ - f.event_ts::TIMESTAMPTZ > INTERVAL 5 MINUTE AS late
FROM first_copy f
JOIN raw_stores s USING (store_id)
CROSS JOIN UNNEST(f.lines) AS u(l);

SELECT business_date, late, count(DISTINCT basket_id) AS baskets, count(*) AS lines, sum(qty) AS units
FROM sale_lines GROUP BY ALL ORDER BY ALL;
