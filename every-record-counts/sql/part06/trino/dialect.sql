-- Part 6: the same dedup in Trino 484. Trino has no QUALIFY; filter the window result in an outer query.
SELECT count(*) AS events
FROM lakehouse.part06.clicks
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_ts) = 1;

SELECT count(*) AS events_without_retries
FROM (SELECT row_number() OVER (PARTITION BY event_id ORDER BY event_ts) AS copy_no
      FROM lakehouse.part06.clicks)
WHERE copy_no = 1;

-- PIVOT arrived in Trino 483 (UNPIVOT has not). Events per type, one column per platform. Unlike DuckDB, Trino
-- doesn't group by the other input columns: without GROUP BY inside PIVOT you get one row.
SELECT *
FROM (SELECT event_type, platform FROM lakehouse.part06.clicks WHERE NOT is_retry)
PIVOT (count(*) FOR platform IN ('app' AS app, 'web' AS web));

SELECT *
FROM (SELECT event_type, platform FROM lakehouse.part06.clicks WHERE NOT is_retry)
PIVOT (count(*) FOR platform IN ('app' AS app, 'web' AS web) GROUP BY event_type)
ORDER BY web DESC;

-- The 30-minute session rule, Trino syntax. Same rule, same answer as DuckDB.
WITH flagged AS (
    SELECT anonymous_id, event_ts, event_id, event_type,
           coalesce(event_ts - lag(event_ts) OVER w > INTERVAL '30' MINUTE, true) AS starts_session
    FROM lakehouse.part06.clicks
    WHERE NOT is_retry AND NOT is_bot
    WINDOW w AS (PARTITION BY anonymous_id ORDER BY event_ts, event_id)
),
numbered AS (
    SELECT *, sum(IF(starts_session, 1, 0)) OVER (PARTITION BY anonymous_id ORDER BY event_ts, event_id
                                                ROWS UNBOUNDED PRECEDING) AS session_no
    FROM flagged
),
per_session AS (
    SELECT bool_or(event_type = 'purchase') AS converted FROM numbered GROUP BY anonymous_id, session_no
)
SELECT count(*) AS sessions, count_if(converted) AS converted,
       round(100e0 * count_if(converted) / count(*), 2) AS conversion_pct
FROM per_session;
