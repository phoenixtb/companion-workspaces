-- How late do till uploads arrive, against the UTC day they were rung up? A daily microbatch run must look back far
-- enough to reach every event uploaded after its UTC day closed (Part 18). DuckDB over the raw zone.
SELECT count(*)                                                                   AS events,
       count(*) FILTER (WHERE upload_date > CAST(timezone('UTC', event_ts) AS DATE))  AS events_on_a_later_utc_day,
       count(DISTINCT basket_id) FILTER (WHERE upload_date > CAST(timezone('UTC', event_ts) AS DATE))
                                                                                  AS baskets_on_a_later_utc_day,
       max(uploaded_at - event_ts)                                                AS worst_lateness
FROM raw_pos_events;
