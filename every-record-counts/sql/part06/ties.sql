-- Part 6: why sessions.sql removes retries first. A retry has the same event_id and timestamp as its original, so
-- ORDER BY event_ts, event_id can't tell the pair apart. lag() and sum() each sort the pair their own way; when they
-- disagree, an original and its retry land in different "sessions". Run it a few times: the count can move.
WITH flagged AS (
    SELECT *, coalesce(event_ts - lag(event_ts) OVER w > INTERVAL 30 MINUTE, true) AS starts_session
    FROM clicks
    WHERE NOT is_bot
    WINDOW w AS (PARTITION BY anonymous_id ORDER BY event_ts, event_id)
),
numbered AS (
    SELECT *, sum(starts_session::INT) OVER (PARTITION BY anonymous_id ORDER BY event_ts, event_id
                                             ROWS UNBOUNDED PRECEDING) AS session_no
    FROM flagged
)
SELECT count(DISTINCT (anonymous_id, session_no)) AS sessions_with_retries_kept,
       count(*) FILTER (WHERE session_no = 0)     AS rows_in_session_zero
FROM numbered;
