-- Part 19: checks the daily pipeline runs after dbt build and before publishing. dbt's own tests (grain, no sale line
-- lost) already ran inside the build; these look across runs and across tables, which dbt's tests don't.
-- Each query returns the rows that break the rule: no rows = pass. {day} is the run's upload day (Airflow's ds).

-- closed_days_moved: a run for upload day D rebuilds UTC days D-1 and D, which are local business days D-1 to D+1 in
-- London, Mumbai and Singapore (all east of UTC). Anything older must equal what was last published.
WITH now_ AS (
    SELECT country, business_date, net_minor FROM lakehouse_pipeline.gold.rpt_daily_net_revenue_v2
    WHERE business_date <= DATE '{day}' - INTERVAL '2' DAY
),
published AS (
    SELECT country, business_date, net_minor
    FROM lakehouse_pipeline.gold.rpt_daily_net_revenue_v2 FOR VERSION AS OF 'published'
    WHERE business_date <= DATE '{day}' - INTERVAL '2' DAY
)
SELECT coalesce(n.country, p.country) AS country, coalesce(n.business_date, p.business_date) AS business_date,
       p.net_minor AS published_minor, n.net_minor AS now_minor
FROM now_ n FULL JOIN published p ON p.country = n.country AND p.business_date = n.business_date
WHERE n.net_minor IS DISTINCT FROM p.net_minor;

-- missing_countries: every country with stores has a revenue row for the upload day.
SELECT DISTINCT s.country
FROM lakehouse_pipeline.gold.dim_store s
WHERE s.country NOT IN (
    SELECT country FROM lakehouse_pipeline.gold.rpt_daily_net_revenue_v2 WHERE business_date = DATE '{day}'
);

-- day_not_landed: the upload day's raw events reached the lake (ingestion wrote something for that day).
SELECT DATE '{day}' AS upload_date
WHERE NOT EXISTS (SELECT 1 FROM lakehouse_pipeline.raw.pos_uploads WHERE upload_date = DATE '{day}');
