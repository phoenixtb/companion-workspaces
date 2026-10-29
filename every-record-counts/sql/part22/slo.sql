-- Part 22: the daily revenue report's service level indicators for one morning check (DuckDB).
-- Variables: expected_day (E: the upload day whose numbers the 06:00 Europe/London check expects published) and
-- first_day (E-27: a 28-day window). scripts.part22_observe registers the inputs:
--   published_through (variable) the upload day the `published` snapshot covers: a property the publish commit sets
--   publishes  one row per upload day in the window: the state of its run's `publish` task (Airflow's REST API),
--              the history for days no 06:00 check recorded
--   controls   gold.daily_controls for the window's run days (Part 21)
--   contract   the published report's columns next to the dbt contract's (Iceberg schema at the `published` tag)
--   recorded   earlier 06:00 checks' results from gold.slo_results: a day they measured counts as they measured it;
--              days before the checks existed are judged from the run history
-- Objectives: freshness and completeness good on 27 of 28 days (an error budget of one day); till events uploaded
-- within 20% of the same weekday a week earlier; schema identical to the contract, every day.
WITH days AS (
    SELECT d::DATE AS d FROM generate_series(getvariable('first_day'), getvariable('expected_day'), INTERVAL 1 DAY) t(d)
),
fresh AS (              -- freshness: today, the published snapshot covers the expected upload day (the table's own
                        -- record); earlier days without a recorded check: their run's publish task succeeded
    SELECT d,
           CASE WHEN d = getvariable('expected_day') THEN coalesce(getvariable('published_through') >= d, false)
                ELSE coalesce(p.state = 'success', false) END AS now_good,
           CASE WHEN d = getvariable('expected_day')
                THEN 'published through ' || coalesce(getvariable('published_through')::VARCHAR, 'nothing')
                ELSE coalesce('publish ' || p.state, 'no run') END AS detail
    FROM days LEFT JOIN publishes p ON p.upload_date = d
),
complete AS (           -- completeness: every control of the day's run passed; a day never checked is not good
    SELECT d, coalesce(c.fails = 0, false) AS now_good,
           coalesce(c.fails || ' failed of ' || c.checks || coalesce(': ' || c.failing, ''), 'no controls') AS detail
    FROM days LEFT JOIN (
        SELECT run_day, count(*) FILTER (WHERE rule <> 'info') AS checks,
               count(*) FILTER (WHERE status = 'FAIL') AS fails,
               string_agg(DISTINCT check_name || ' ' || scope_date || ' ' || coalesce(country, ''), ', ')
                   FILTER (WHERE status = 'FAIL') AS failing
        FROM controls GROUP BY run_day
    ) c ON c.run_day = d
),
judged AS (             -- a day its 06:00 check recorded counts as recorded (this check excluded); else as it is now
    SELECT 'freshness' AS sli, f.d, f.now_good, f.detail, r.status AS recorded FROM fresh f
    LEFT JOIN recorded r ON r.sli = 'freshness' AND r.expected_day = f.d
    UNION ALL
    SELECT 'completeness', c.d, c.now_good, c.detail, r.status FROM complete c
    LEFT JOIN recorded r ON r.sli = 'completeness' AND r.expected_day = c.d
),
budget AS (
    SELECT sli, CASE sli WHEN 'freshness' THEN 'report' ELSE 'controls' END AS scope,
           bool_or(now_good) FILTER (WHERE d = getvariable('expected_day')) AS today,
           count(*) FILTER (WHERE coalesce(recorded = 'pass', now_good)) AS good_days, count(*) AS window_days,
           count(recorded) AS measured, max(detail) FILTER (WHERE d = getvariable('expected_day')) AS detail
    FROM judged GROUP BY sli
),
uploads AS (            -- till events received per country and upload day (Part 21's landing control), newest run
    SELECT country, scope_date, source_count AS n
    FROM controls WHERE check_name = 'raw_vs_dispositions'
    QUALIFY row_number() OVER (PARTITION BY country, scope_date ORDER BY run_day DESC) = 1
),
volume AS (             -- the expected upload day against the same weekday one week earlier
    SELECT 'volume' AS sli, n.country AS scope, abs(n.n - w.n) <= 0.2 * w.n AS today, NULL::BIGINT AS good_days,
           NULL::BIGINT AS window_days, NULL::BIGINT AS measured,
           format('{} till events uploaded on {} vs {} on {} ({:+.1f}%)', n.n, n.scope_date, w.n, w.scope_date,
                  100.0 * (n.n - w.n) / w.n) AS detail
    FROM uploads n JOIN uploads w ON w.country = n.country AND w.scope_date = n.scope_date - 7
    WHERE n.scope_date = getvariable('expected_day')
),
schema AS (
    SELECT 'schema' AS sli, 'report' AS scope, count(*) FILTER (WHERE expected IS DISTINCT FROM actual) = 0 AS today,
           NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
           coalesce(string_agg(column_name || ': ' || coalesce(expected, 'none') || ' vs ' || coalesce(actual, 'none'), ', ')
                        FILTER (WHERE expected IS DISTINCT FROM actual),
                    count(*) || ' columns as contracted') AS detail
    FROM contract
)
SELECT sli, scope, CASE WHEN today THEN 'pass' ELSE 'FAIL' END AS status, good_days, window_days,
       CASE WHEN sli IN ('freshness', 'completeness') THEN 1 - (window_days - good_days) END AS budget_left, measured,
       detail
FROM (SELECT * FROM budget UNION ALL SELECT * FROM volume UNION ALL SELECT * FROM schema)
ORDER BY CASE sli WHEN 'freshness' THEN 1 WHEN 'completeness' THEN 2 WHEN 'volume' THEN 3 ELSE 4 END, scope
