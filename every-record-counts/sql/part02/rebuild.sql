-- v2, step 2 (DuckDB): rebuild the affected business dates, silver then gold, in one transaction.
-- scripts/part02_pipeline.py fills the temp table `affected` and the upload window variables first.

-- silver: every basket of those dates, from every landed upload that can hold them, once each.
DELETE FROM silver.pos_baskets WHERE business_date IN (SELECT business_date FROM affected);

INSERT INTO silver.pos_baskets
SELECT DISTINCT ON (basket_id)                       -- the dedup key: a re-sent basket keeps its basket_id
       basket_id, store_id, country, currency, business_date, event_ts, upload_date, event_id, amount_minor
FROM (
    SELECT e.*, s.country,
           (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date,
           e.tenders[1].amount_minor                  AS amount_minor
    FROM landed_pos_events e
    JOIN landed_stores s USING (store_id)
    WHERE e.upload_date BETWEEN getvariable('first_upload') AND getvariable('last_upload')  -- prune partitions
)
WHERE business_date IN (SELECT business_date FROM affected)
  AND upload_date BETWEEN business_date - 1 AND business_date + getvariable('late_days')  -- the lateness policy
ORDER BY basket_id, uploaded_at, event_id;           -- first upload wins; ties broken, so reruns agree

-- gold: overwrite the same dates from silver.
DELETE FROM gold.daily_store_sales WHERE business_date IN (SELECT business_date FROM affected);
INSERT INTO gold.daily_store_sales
SELECT business_date, store_id, country, currency, count(*) AS baskets, sum(amount_minor) AS revenue_minor
FROM silver.pos_baskets WHERE business_date IN (SELECT business_date FROM affected) GROUP BY ALL;
