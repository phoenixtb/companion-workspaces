-- Part 16: does the lake hold exactly what PostgreSQL holds? (DuckDB)
-- `pg` is the larkspur database attached read-only; orders, payments and refunds are the Iceberg current-state
-- tables, registered by scripts.part16_cdc. Counts and sums first, then every column of every row compared both ways.
WITH lake_orders AS (
    SELECT order_id, customer_id, country, currency, channel, status, total_minor, tender_type,
           created_at, updated_at, fulfilled_at FROM orders
), lake_payments AS (
    SELECT payment_id, source, reference, country, currency, tender_type, amount_minor, status,
           authorised_at, captured_at, voided_at FROM payments
), lake_refunds AS (
    SELECT refund_id, original_channel, original_reference, country, store_id, currency, tender_type,
           refund_ts, amount_minor, reason FROM refunds
)
SELECT 'orders' AS "table",
       (SELECT count(*) FROM pg.shop.orders) AS pg_rows,
       (SELECT count(*) FROM lake_orders) AS lake_rows,
       (SELECT sum(total_minor) FROM pg.shop.orders)::BIGINT AS pg_minor,
       (SELECT sum(total_minor) FROM lake_orders)::BIGINT AS lake_minor,
       (SELECT count(*) FROM (FROM pg.shop.orders EXCEPT ALL FROM lake_orders))
     + (SELECT count(*) FROM (FROM lake_orders EXCEPT ALL FROM pg.shop.orders)) AS rows_differing
UNION ALL
SELECT 'payments',
       (SELECT count(*) FROM pg.shop.payments),
       (SELECT count(*) FROM lake_payments),
       (SELECT sum(amount_minor) FROM pg.shop.payments)::BIGINT,
       (SELECT sum(amount_minor) FROM lake_payments)::BIGINT,
       (SELECT count(*) FROM (FROM pg.shop.payments EXCEPT ALL FROM lake_payments))
     + (SELECT count(*) FROM (FROM lake_payments EXCEPT ALL FROM pg.shop.payments))
UNION ALL
SELECT 'refunds',
       (SELECT count(*) FROM pg.shop.refunds),
       (SELECT count(*) FROM lake_refunds),
       (SELECT sum(amount_minor) FROM pg.shop.refunds)::BIGINT,
       (SELECT sum(amount_minor) FROM lake_refunds)::BIGINT,
       (SELECT count(*) FROM (FROM pg.shop.refunds EXCEPT ALL FROM lake_refunds))
     + (SELECT count(*) FROM (FROM lake_refunds EXCEPT ALL FROM pg.shop.refunds));

-- The payment ledger by status: voided payments stay (a soft delete), cancelled orders don't (a hard delete).
SELECT status,
       (SELECT count(*) FROM pg.shop.payments p WHERE p.status = s.status) AS pg_rows,
       count(*) AS lake_rows,
       sum(amount_minor)::BIGINT AS lake_minor
FROM payments s
GROUP BY status
ORDER BY status;
