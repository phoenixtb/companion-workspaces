-- Part 6: exact vs approximate distinct visitors (DuckDB; approx_count_distinct is HyperLogLog).
-- Statement 1: Q3 (UTC days), humans and bots, each counted both ways.
SELECT CASE WHEN is_bot THEN 'bots' ELSE 'humans' END                    AS visitors,
       count(DISTINCT anonymous_id)                                      AS exact,
       approx_count_distinct(anonymous_id)                               AS approx,
       round(100 * (approx_count_distinct(anonymous_id) - count(DISTINCT anonymous_id))
                 / count(DISTINCT anonymous_id), 2)                      AS error_pct
FROM clicks
WHERE event_ts::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
GROUP BY ALL
ORDER BY visitors DESC;

-- Statement 2: per UTC day in Q3. Distinct counts don't add up: the sum of 92 daily counts is not the quarter's count.
WITH daily AS (
    SELECT event_ts::DATE AS day,
           count(DISTINCT anonymous_id)        AS exact,
           approx_count_distinct(anonymous_id) AS approx
    FROM clicks
    WHERE NOT is_bot AND event_ts::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
    GROUP BY day
)
SELECT count(*)                                           AS days,
       round(avg(abs(approx - exact) / exact) * 100, 2)   AS mean_abs_error_pct,
       round(max(abs(approx - exact) / exact) * 100, 2)   AS max_abs_error_pct,
       sum(exact)                                         AS sum_of_daily_visitors,
       (SELECT count(DISTINCT anonymous_id) FROM clicks
        WHERE NOT is_bot AND event_ts::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30') AS quarter_visitors
FROM daily;
