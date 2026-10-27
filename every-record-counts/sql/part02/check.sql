-- One business date three ways (DuckDB): v1's table, v2's table, and a full recompute over every
-- landed upload. check_date is set by scripts/part02_pipeline.py.
WITH recompute AS (
    SELECT country, count(*) AS baskets, sum(amount) AS revenue_minor
    FROM (
        SELECT DISTINCT ON (e.basket_id) s.country, e.tenders[1].amount_minor AS amount,
               (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date
        FROM landed_pos_events e JOIN landed_stores s USING (store_id)
        ORDER BY e.basket_id, e.uploaded_at, e.event_id
    )
    WHERE business_date = getvariable('check_date')
    GROUP BY country
),
v1 AS (
    SELECT s.country, sum(baskets) AS baskets, sum(revenue_minor) AS revenue_minor
    FROM naive.daily_store_sales n JOIN landed_stores s USING (store_id)
    WHERE business_date = getvariable('check_date')
    GROUP BY s.country
),
v2 AS (
    SELECT country, sum(baskets) AS baskets, sum(revenue_minor) AS revenue_minor
    FROM gold.daily_store_sales
    WHERE business_date = getvariable('check_date')
    GROUP BY country
)
SELECT r.country,
       v1.revenue_minor AS v1_naive,
       v2.revenue_minor AS v2_gold,
       r.revenue_minor  AS recompute,
       v2.baskets       AS v2_baskets,
       r.baskets        AS recompute_baskets
FROM recompute r
LEFT JOIN v1 USING (country)
LEFT JOIN v2 USING (country)
ORDER BY r.country;
