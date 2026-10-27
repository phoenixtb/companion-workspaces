-- How far apart are the upload partition (UTC arrival date) and the business date (store's local date)?
-- Run with scripts/duck.py over the whole raw zone (DuckDB).
SELECT upload_date - (e.event_ts AT TIME ZONE s.timezone)::DATE AS upload_minus_business_date,
       count(*)                                                AS events,
       count(DISTINCT e.basket_id)                             AS baskets
FROM raw_pos_events e
JOIN raw_stores s USING (store_id)
GROUP BY ALL
ORDER BY 1;
