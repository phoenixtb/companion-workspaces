-- Why v1's number for one business date is wrong (DuckDB). Exactly:
--   v1 = recompute - (baskets with no copy in that date's upload) + (extra copies inside that upload)
WITH events AS (
    SELECT s.country, e.basket_id, e.upload_date, e.tenders[1].amount_minor AS amount,
           (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date
    FROM landed_pos_events e JOIN landed_stores s USING (store_id)
),
per_basket AS (
    SELECT country, basket_id, any_value(amount) AS amount,
           count(*) FILTER (WHERE upload_date = business_date) AS copies_in_own_upload
    FROM events
    WHERE business_date = getvariable('check_date')
    GROUP BY country, basket_id
)
SELECT country,
       count(*) FILTER (WHERE copies_in_own_upload = 0)            AS baskets_in_other_uploads,
       sum(amount) FILTER (WHERE copies_in_own_upload = 0)          AS missed_minor,
       sum(copies_in_own_upload - 1) FILTER (WHERE copies_in_own_upload > 1)          AS extra_copies,
       sum(amount * (copies_in_own_upload - 1)) FILTER (WHERE copies_in_own_upload > 1) AS double_counted_minor
FROM per_basket
GROUP BY country
ORDER BY country;
