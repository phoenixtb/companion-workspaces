-- The first days of October by store: the partition the micro-batches and the correction wrote to.
SELECT store_id, count(*) AS lines, sum(amount_minor) AS sales_minor
FROM {table}
WHERE business_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-04'
GROUP BY store_id
