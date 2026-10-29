-- Part 20: sessions as contracts/clickstream_sessions.odcs.yaml defines them (DuckDB), one row per session.
-- `events` is raw.clickstream_events, the SDK's events as landed. The contract's rules, in order:
--   1. events that count: all five event types, each event_id once (an SDK retry is the same event again);
--   2. bots excluded: a user agent matching (?i)bot|crawl|spider|headless starts no session;
--   3. a session is one anonymous_id's events with no gap over 30 minutes, in event time (UTC instants, no days);
--   4. a session converted if it holds a purchase.
CREATE OR REPLACE TABLE web_sessions AS
WITH counted AS (
    SELECT * FROM events
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_ts) = 1                       -- rule 1
),
human AS (
    SELECT * FROM counted WHERE NOT regexp_matches(user_agent, '(?i)bot|crawl|spider|headless')  -- rule 2
),
gaps AS (
    SELECT *, event_ts - lag(event_ts) OVER w AS gap
    FROM human
    WINDOW w AS (PARTITION BY anonymous_id ORDER BY event_ts, event_id)
),
numbered AS (                                                                                     -- rule 3
    SELECT *, sum((gap IS NULL OR gap > INTERVAL 30 MINUTE)::INT)
                  OVER (PARTITION BY anonymous_id ORDER BY event_ts, event_id ROWS UNBOUNDED PRECEDING) AS session_no
    FROM gaps
)
SELECT anonymous_id || '#' || session_no                                       AS session_id,
       anonymous_id,
       any_value(platform)                                                     AS platform,
       any_value(country)                                                      AS country,
       any_value(user_agent)                                                   AS user_agent,
       min(event_ts)                                                           AS started_at,
       max(event_ts)                                                           AS ended_at,
       count(*)                                                                AS events,
       coalesce(max(epoch(gap)) FILTER (WHERE gap <= INTERVAL 30 MINUTE), 0)::BIGINT AS max_gap_seconds,
       bool_or(event_type = 'product_view')                                    AS product_view,
       bool_or(event_type = 'add_to_cart')                                     AS add_to_cart,
       bool_or(event_type = 'checkout_start')                                  AS checkout_start,
       bool_or(event_type = 'purchase')                                        AS converted   -- rule 4
FROM numbered
GROUP BY anonymous_id, session_no;
