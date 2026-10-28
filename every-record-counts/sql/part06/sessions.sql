-- Part 6: sessionize the clickstream two ways, one row per session (DuckDB).
-- Retries are removed first: a retried event has the same event_id AND the same timestamp as its original, so
-- the window ORDER BY below would not be unique, and two windows could sort the pair differently.
CREATE OR REPLACE TABLE sessions AS
WITH events AS (
    SELECT * FROM clicks WHERE NOT is_retry
),
flagged AS (                -- product: a new session after 30 minutes without an event
    SELECT *,
           coalesce(event_ts - lag(event_ts) OVER w > INTERVAL 30 MINUTE, true) AS starts_session
    FROM events
    WINDOW w AS (PARTITION BY anonymous_id ORDER BY event_ts, event_id)
),
numbered AS (               -- a running count of session starts numbers the sessions
    SELECT *,
           sum(starts_session::INT) OVER (PARTITION BY anonymous_id ORDER BY event_ts, event_id
                                          ROWS UNBOUNDED PRECEDING) AS session_no
    FROM flagged
),
both_definitions AS (
    SELECT 'product' AS definition, anonymous_id || '#' || session_no AS session_id, * FROM numbered
    UNION ALL               -- marketing: one session per visitor per UTC calendar day
    SELECT 'marketing', anonymous_id || '#' || event_ts::DATE, *, NULL, NULL FROM events
)
SELECT definition,
       session_id,
       any_value(anonymous_id)                 AS anonymous_id,
       bool_or(is_bot)                         AS is_bot,
       min(event_ts)                           AS started_at,
       max(event_ts)                           AS ended_at,
       count(*)                                AS events,
       bool_or(event_type = 'page_view')       AS page_view,
       bool_or(event_type = 'product_view')    AS product_view,
       bool_or(event_type = 'add_to_cart')     AS add_to_cart,
       bool_or(event_type = 'checkout_start')  AS checkout_start,
       bool_or(event_type = 'purchase')        AS purchase
FROM both_definitions
GROUP BY definition, session_id;
