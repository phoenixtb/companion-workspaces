-- v2, step 1 (DuckDB): which business dates does the newly landed upload contain?
SELECT DISTINCT (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date
FROM read_json(getvariable('landed_root') || '/pos/events/upload_date=' || getvariable('run_date') || '/*.jsonl.gz') e
JOIN landed_stores s USING (store_id)
ORDER BY business_date;
