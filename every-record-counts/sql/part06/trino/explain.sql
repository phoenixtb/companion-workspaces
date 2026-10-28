-- Part 6: a Trino plan. EXPLAIN ANALYZE runs the query and reports, per fragment (stage) and operator, rows, CPU,
-- wall time and how data moved between stages.
EXPLAIN ANALYZE
SELECT country, count(*) AS retries
FROM (SELECT country, row_number() OVER (PARTITION BY event_id ORDER BY event_ts) AS copy_no
      FROM lakehouse.part06.clicks)
WHERE copy_no > 1
GROUP BY country;
