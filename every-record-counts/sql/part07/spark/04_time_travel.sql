-- Part 7, step 5: Q3 as finance closed it vs Q3 now, and a branch for experiments (Spark SQL).
-- The tagged snapshot is read with its own schema: there the column is still called amount_minor.
SELECT 'tag q3_close' AS version, count(*) AS lines, sum(amount_minor) AS uk_sales_minor
FROM lakehouse.part07.sales_line VERSION AS OF 'q3_close'
WHERE country = 'GB' AND business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
UNION ALL
SELECT 'main', count(*), sum(net_amount_minor)
FROM lakehouse.part07.sales_line
WHERE country = 'GB' AND business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';

-- Fails on purpose: the new name doesn't exist in that snapshot's schema.
SELECT sum(net_amount_minor) FROM lakehouse.part07.sales_line VERSION AS OF 'q3_close';

-- A branch is a second line of snapshots. Writing to it leaves main alone.
ALTER TABLE lakehouse.part07.sales_line CREATE BRANCH rohan_dev RETAIN 7 DAYS;
DELETE FROM lakehouse.part07.sales_line.branch_rohan_dev WHERE country = 'SG';

SELECT 'main' AS ref, count(*) AS rows FROM lakehouse.part07.sales_line
UNION ALL SELECT 'branch rohan_dev', count(*) FROM lakehouse.part07.sales_line VERSION AS OF 'rohan_dev'
UNION ALL SELECT 'tag q3_close', count(*) FROM lakehouse.part07.sales_line VERSION AS OF 'q3_close';
