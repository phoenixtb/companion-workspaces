-- Part 6: one clean, typed clickstream table (DuckDB). Nothing is dropped here; problems become columns.
-- raw_clickstream reads the JSON with union_by_name, so columns that only exist in later files survive.
CREATE OR REPLACE TABLE clicks AS
SELECT event_id,
       anonymous_id,
       event_ts,
       event_type,
       coalesce(page_path, page)                                   AS page,    -- renamed on 2026-08-15
       platform,
       country,
       consent.analytics                                           AS consent_analytics,  -- from 2026-08-01
       regexp_matches(user_agent, '(?i)bot|crawl|spider|headless') AS is_bot,
       -- an SDK retry re-sends the identical event: same event_id, same timestamp
       row_number() OVER (PARTITION BY event_id ORDER BY event_ts) > 1 AS is_retry
FROM raw_clickstream;
