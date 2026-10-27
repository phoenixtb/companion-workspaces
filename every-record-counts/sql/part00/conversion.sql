-- Online conversion, Q3 2026, the product team's way and marketing's way (DuckDB).
WITH events AS (SELECT DISTINCT ON (event_id) * FROM raw_clickstream),
product_hits AS (           -- product: a new session after 30 minutes of inactivity; bots removed
    SELECT anonymous_id, event_ts, event_type,
           CASE WHEN lag(event_ts) OVER w IS NULL
                  OR event_ts - lag(event_ts) OVER w > INTERVAL 30 MINUTE THEN 1 ELSE 0 END AS starts_session
    FROM events
    WHERE NOT regexp_matches(user_agent, '(?i)bot|crawl|spider|headless')
    WINDOW w AS (PARTITION BY anonymous_id ORDER BY event_ts)
),
product AS (
    SELECT bool_or(event_type = 'purchase') AS converted
    FROM (SELECT *, sum(starts_session) OVER (PARTITION BY anonymous_id ORDER BY event_ts) AS session_no
          FROM product_hits)
    GROUP BY anonymous_id, session_no
),
marketing AS (              -- marketing: one session per visitor per (UTC) day; every hit counts
    SELECT bool_or(event_type = 'purchase') AS converted
    FROM raw_clickstream
    GROUP BY anonymous_id, event_ts::DATE
)
SELECT 'product' AS team, count(*) AS sessions, round(avg(converted::INT) * 100, 2) AS conversion_pct FROM product
UNION ALL
SELECT 'marketing', count(*), round(avg(converted::INT) * 100, 2) FROM marketing;
