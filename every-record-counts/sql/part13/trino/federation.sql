-- Part 13: one query over two systems: the lake's sales lines (Iceberg via Polaris) and the store-ops
-- service's targets (PostgreSQL). Trino pushes what it can into each source and does the join itself.

-- 1. PostgreSQL alone: the filter and the aggregation run inside PostgreSQL (TableScan shows Query[...]).
EXPLAIN
SELECT month, count(*) AS stores, sum(target_minor) AS target_minor
FROM oltp.store_ops.sales_targets
WHERE currency = 'GBP'
GROUP BY month;

-- 2. Grouping by a text column is not pushed: PostgreSQL may sort and compare strings differently from Trino.
EXPLAIN
SELECT currency, count(*) AS stores, sum(target_minor) AS target_minor
FROM oltp.store_ops.sales_targets
WHERE month = DATE '2026-09-01'
GROUP BY currency;

-- 3. Nor is a function the connector can't translate: every row of the month crosses the wire first.
EXPLAIN
SELECT store_id, target_minor
FROM oltp.store_ops.sales_targets
WHERE month = DATE '2026-09-01' AND regexp_like(set_by, 'gb$');

-- 4. The federated question: September's UK stores furthest behind target. First the plan as it ran,
-- with what each operator read, then the answer.
EXPLAIN ANALYZE
SELECT t.store_id, t.target_minor, s.sales_minor,
       round(100.0 * s.sales_minor / t.target_minor, 1) AS pct_of_target
FROM oltp.store_ops.sales_targets t
JOIN (
    SELECT store_id, sum(amount_minor) AS sales_minor
    FROM lakehouse.part10.pos_monthly
    WHERE business_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30' AND country = 'GB'
    GROUP BY store_id
) s ON s.store_id = t.store_id
WHERE t.month = DATE '2026-09-01' AND t.currency = 'GBP'
ORDER BY pct_of_target
LIMIT 5;

SELECT t.store_id, t.target_minor, s.sales_minor,
       round(100.0 * s.sales_minor / t.target_minor, 1) AS pct_of_target
FROM oltp.store_ops.sales_targets t
JOIN (
    SELECT store_id, sum(amount_minor) AS sales_minor
    FROM lakehouse.part10.pos_monthly
    WHERE business_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30' AND country = 'GB'
    GROUP BY store_id
) s ON s.store_id = t.store_id
WHERE t.month = DATE '2026-09-01' AND t.currency = 'GBP'
ORDER BY pct_of_target
LIMIT 5;

-- 5. Materialized views: Trino 484's Iceberg connector supports them, but not on a REST catalog such as Polaris.
CREATE SCHEMA IF NOT EXISTS lakehouse.part13;
CREATE MATERIALIZED VIEW lakehouse.part13.daily_store_sales_mv AS
SELECT store_id, business_date, count(*) AS lines, sum(amount_minor) AS sales_minor
FROM lakehouse.part10.pos_monthly
GROUP BY store_id, business_date
