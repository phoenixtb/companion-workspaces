-- Part 6: exact vs approximate distinct visitors in Q3 (UTC days) in Trino 484 (approx_distinct: HyperLogLog, 2.3% standard error).
SELECT CASE WHEN is_bot THEN 'bots' ELSE 'humans' END AS visitors,
       count(DISTINCT anonymous_id)                    AS exact,
       approx_distinct(anonymous_id)                   AS approx,
       round(100e0 * (approx_distinct(anonymous_id) - count(DISTINCT anonymous_id))
             / count(DISTINCT anonymous_id), 2)        AS error_pct
FROM lakehouse.part06.clicks
WHERE CAST(event_ts AT TIME ZONE 'UTC' AS DATE) BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
GROUP BY 1
ORDER BY 1 DESC;

-- Sketches merge: keep one HyperLogLog per day, and any range of days is a merge away. Sums of daily counts are not.
-- Q3 by UTC day, the same range as sql/part06/approx.sql in DuckDB.
WITH q3 AS (
    SELECT anonymous_id, CAST(event_ts AT TIME ZONE 'UTC' AS DATE) AS day
    FROM lakehouse.part06.clicks
    WHERE NOT is_bot
),
daily AS (
    SELECT day, count(DISTINCT anonymous_id) AS exact, approx_set(anonymous_id) AS sketch
    FROM q3
    WHERE day BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
    GROUP BY day
)
SELECT count(*)                     AS days,
       sum(exact)                   AS sum_of_daily_visitors,
       cardinality(merge(sketch))   AS merged_daily_sketches,
       (SELECT count(DISTINCT anonymous_id) FROM q3 WHERE day BETWEEN DATE '2026-07-01' AND DATE '2026-09-30')
                                    AS exact_visitors
FROM daily;
