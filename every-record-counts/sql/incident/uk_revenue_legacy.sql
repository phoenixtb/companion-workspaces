-- UK net revenue for Q3 2026, the way the board deck computed it (DuckDB).
-- Every line below is a defect; docs/injected-defects.md lists what each one costs.
WITH latest_eur AS (                       -- "today's" rate: the newest row in the table
    SELECT rate FROM raw_fx_rates WHERE base_currency = 'EUR' ORDER BY rate_date DESC LIMIT 1
),
pos AS (                                   -- every event counts, re-sends included
    SELECT sum(tenders[1].amount_minor) AS amount
    FROM raw_pos_events
    WHERE store_id LIKE 'GB-%'
      AND event_ts::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'      -- UTC dates
),
orders AS (                                -- the nightly updated_at poller's view of orders
    SELECT * FROM raw_orders_poll
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
),
online AS (
    SELECT sum(CASE WHEN currency = 'EUR'
                    THEN round(total_minor * (SELECT rate FROM latest_eur))
                    ELSE total_minor END) AS amount
    FROM orders
    WHERE country = 'GB'
      AND created_at::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
),
purchases AS (
    SELECT DISTINCT basket_id AS reference, event_ts AS purchased_at FROM raw_pos_events
    UNION ALL
    SELECT order_id, created_at FROM orders
),
refunds AS (                               -- refunds land in the quarter of the original purchase
    SELECT sum(CASE WHEN r.currency = 'EUR'
                    THEN round(r.amount_minor * (SELECT rate FROM latest_eur))
                    ELSE r.amount_minor END) AS amount
    FROM raw_refunds r
    JOIN purchases p ON p.reference = r.original_reference
    WHERE r.country = 'GB'
      AND p.purchased_at::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
)
SELECT pos.amount + online.amount - refunds.amount AS uk_net_revenue_minor
FROM pos, online, refunds;
