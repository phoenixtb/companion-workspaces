-- UK net revenue for Q3 2026, correctly (DuckDB).
WITH pos AS (                              -- one basket, one sale, however many times the till sent it
    SELECT DISTINCT ON (basket_id) basket_id, event_ts, tenders[1].amount_minor AS amount
    FROM raw_pos_events
    WHERE store_id LIKE 'GB-%'
),
sales AS (
    SELECT event_ts AS ts, 'GBP' AS currency, amount FROM pos
    UNION ALL                              -- the current orders table: deleted orders are gone
    SELECT created_at, currency, total_minor FROM raw_orders WHERE country = 'GB'
    UNION ALL                              -- refunds count on the day they happen
    SELECT refund_ts, currency, -amount_minor FROM raw_refunds WHERE country = 'GB'
),
local_sales AS (                           -- the business date is the date in London
    SELECT (ts AT TIME ZONE 'Europe/London')::DATE AS business_date, currency, amount FROM sales
),
eur AS (
    SELECT rate_date, rate FROM raw_fx_rates WHERE base_currency = 'EUR'
)
SELECT sum(CASE WHEN s.currency = 'EUR'    -- the rate in force that day (Friday's rate on a weekend)
                THEN sign(s.amount) * round(abs(s.amount) * e.rate)
                ELSE s.amount END)::BIGINT AS uk_net_revenue_minor
FROM local_sales s
ASOF LEFT JOIN eur e ON s.business_date >= e.rate_date
WHERE s.business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';
