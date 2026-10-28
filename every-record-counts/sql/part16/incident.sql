-- Part 16: revenue step 2, the hard-deleted orders (DuckDB).
-- UK online orders in Q3 2026, summed the way the board deck did it (UTC dates, euros at the report-day rate, as in
-- sql/incident/uk_revenue_legacy.sql), from the legacy poller's extracts and from the CDC replica raw.orders.
-- orders and orders_changes are the Iceberg tables, registered by scripts.part16_cdc.
WITH report_day_eur AS (
    SELECT rate FROM raw_fx_rates WHERE base_currency = 'EUR' ORDER BY rate_date DESC LIMIT 1
),
polled AS (                                -- the poller's last version of each order it ever saw
    SELECT order_id, country, currency, total_minor, created_at FROM raw_orders_poll
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
),
deleted AS (                               -- orders whose last change in the history is a delete
    SELECT order_id, country, currency, total_minor, created_at FROM orders_changes
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _lsn DESC) = 1 AND _op = 'd'
),
sources AS (
    SELECT '1 poller extracts (board deck)' AS source, * FROM polled
    UNION ALL
    SELECT '2 CDC replica raw.orders', order_id, country, currency, total_minor, created_at FROM orders
    UNION ALL
    SELECT '3 deleted, per raw.orders_changes', * FROM deleted
    UNION ALL
    SELECT '4   of which the poller extracted', * FROM deleted WHERE order_id IN (SELECT order_id FROM polled)
)
SELECT source,
       count(*) AS orders,
       count(*) FILTER (WHERE currency = 'EUR') AS eur_orders,
       sum(CASE WHEN currency = 'EUR' THEN round(total_minor * (SELECT rate FROM report_day_eur))
                ELSE total_minor END)::BIGINT AS revenue_minor
FROM sources
WHERE country = 'GB'
  AND created_at::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'   -- UTC dates, as the board deck cut them
GROUP BY source
ORDER BY source;
