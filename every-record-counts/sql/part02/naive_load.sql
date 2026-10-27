-- v1 of the daily sales job (DuckDB): "load yesterday's upload, keep yesterday's sales, append".
-- Three bugs, one per line marked (!).
INSERT INTO naive.daily_store_sales                                                   -- (!) append
SELECT (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date,
       e.store_id,
       e.currency,
       count(*)                       AS baskets,                                     -- (!) per event
       sum(e.tenders[1].amount_minor) AS revenue_minor
FROM read_json(getvariable('landed_root') || '/pos/events/upload_date=' || getvariable('run_date') || '/*.jsonl.gz') e
JOIN landed_stores s USING (store_id)
WHERE (e.event_ts AT TIME ZONE s.timezone)::DATE = getvariable('run_date')            -- (!) one file = one day
GROUP BY ALL;
