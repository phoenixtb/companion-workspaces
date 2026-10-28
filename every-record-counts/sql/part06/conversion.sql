-- Part 6: one clickstream, two session definitions, two conversion rates (DuckDB, Q3 2026).
-- Statement 1: each definition with and without bots. Product = 30 minutes, bots out; marketing = UTC day, bots in.
SELECT definition,
       CASE WHEN bots THEN 'included' ELSE 'excluded' END AS bots,
       sessions, converted,
       round(100 * converted / sessions, 2)               AS conversion_pct
FROM (
    SELECT definition, true AS bots, count(*) AS sessions, count(*) FILTER (WHERE purchase) AS converted
    FROM sessions GROUP BY definition
    UNION ALL
    SELECT definition, false, count(*), count(*) FILTER (WHERE purchase)
    FROM sessions WHERE NOT is_bot GROUP BY definition
)
ORDER BY definition DESC, bots;

-- Statement 2: why the day rule finds fewer human sessions. Product sessions, bots excluded, grouped by UTC day.
WITH product AS (
    SELECT *, started_at::DATE AS day FROM sessions WHERE definition = 'product' AND NOT is_bot
),
ordered AS (
    SELECT *,
           started_at - lag(ended_at) OVER (PARTITION BY anonymous_id, day ORDER BY started_at) AS gap,
           bool_or(purchase) OVER (PARTITION BY anonymous_id, day)                         AS day_has_purchase
    FROM product
),
crossing AS (               -- sessions that run past midnight: the day rule cuts them in two
    SELECT p.anonymous_id, p.ended_at::DATE AS day FROM product p WHERE p.ended_at::DATE > p.day
)
SELECT step, sessions FROM (
    SELECT 1 AS n, 'product sessions (30 minutes, no bots)' AS step, count(*) AS sessions FROM product
    UNION ALL
    SELECT 2, CASE WHEN gap <= INTERVAL 90 MINUTE THEN '- later visit, back within 90 minutes'
                   WHEN day_has_purchase THEN '- later visit, same day as a purchase'
                   ELSE '- later visit, no purchase that day' END,
           -count(*)
    FROM ordered WHERE gap IS NOT NULL GROUP BY 2
    UNION ALL
    SELECT 3, '+ session crossing UTC midnight', count(*)
    FROM crossing c ANTI JOIN product p ON p.anonymous_id = c.anonymous_id AND p.day = c.day
    UNION ALL
    SELECT 4, 'marketing sessions (UTC day, no bots)', count(*)
    FROM sessions WHERE definition = 'marketing' AND NOT is_bot
)
ORDER BY n, sessions;

-- Statement 3: SDK retries change event counts, not sessions. Part 0's marketing query kept them.
SELECT count(*)                                            AS events,
       count(*) FILTER (WHERE NOT is_retry)                AS events_without_retries,
       count(DISTINCT (anonymous_id, event_ts::DATE))      AS marketing_sessions_with_retries,
       count(DISTINCT (anonymous_id, event_ts::DATE)) FILTER (WHERE NOT is_retry) AS without_retries
FROM clicks;
