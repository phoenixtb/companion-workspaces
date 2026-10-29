-- Part 21: UK net revenue for Q3 2026, from the board deck's method to the correct one, one fix at a time (DuckDB).
-- Every record carries the range of fix levels in which it counts; level k applies fixes 1..k. Step k removes
-- total(k-1) - total(k), so the steps sum to the gap by construction, and each step is computed from records:
--   pos        silver.pos_event_dispositions: every event a UK till sent, with Part 21's verdict
--   polled     the legacy updated_at poller's extracts (last version per order), what the board deck read
--   orders     raw.orders, the CDC replica (Part 16): hard-deleted orders are gone
--   refunds    raw.refunds (CDC), with the purchase time of the sale they refund
--   fx         raw.fx_rates
WITH eur AS (
    SELECT rate_date, rate FROM fx WHERE base_currency = 'EUR'
),
records AS (
    SELECT 'pos' AS kind, event_ts AS ts, NULL::TIMESTAMPTZ AS purchased_at, 'GBP' AS currency, amount_minor AS amount,
           0 AS from_level,
           CASE WHEN disposition = 'accepted' THEN 5 ELSE 0 END AS to_level   -- fix 1: re-sends stop counting
    FROM pos WHERE country = 'GB'
    UNION ALL
    SELECT 'online', created_at, NULL, currency, total_minor, 0, 1 FROM polled WHERE country = 'GB'   -- until fix 2
    UNION ALL
    SELECT 'online', created_at, NULL, currency, total_minor, 2, 5 FROM orders WHERE country = 'GB'   -- from fix 2
    UNION ALL
    SELECT 'refund', r.refund_ts, p.purchased_at, r.currency, -r.amount_minor, 0, 5
    FROM refunds r
    JOIN (SELECT basket_id AS reference, min(event_ts) AS purchased_at FROM pos GROUP BY basket_id
          UNION ALL
          SELECT order_id, created_at FROM orders
          UNION ALL
          SELECT order_id, created_at FROM polled WHERE order_id NOT IN (SELECT order_id FROM orders)
         ) p ON p.reference = r.original_reference
    WHERE r.country = 'GB'
),
dated AS (
    SELECT *, ts::DATE AS utc_date, (ts AT TIME ZONE 'Europe/London')::DATE AS london_date FROM records
),
rated AS (                                 -- the euro rate in force on each date (Friday's on a weekend)
    SELECT d.*, u.rate AS rate_utc, l.rate AS rate_london
    FROM dated d
    ASOF LEFT JOIN eur u ON d.utc_date >= u.rate_date
    ASOF LEFT JOIN eur l ON d.london_date >= l.rate_date
),
levels AS (
    SELECT r.*, k.level,
           CASE WHEN r.kind = 'refund' AND k.level < 3 THEN r.purchased_at::DATE   -- fix 3: the refund's own date
                WHEN k.level >= 5 THEN r.london_date                               -- fix 5: London business date
                ELSE r.utc_date END AS report_date,
           CASE WHEN k.level < 4 THEN (SELECT rate FROM eur ORDER BY rate_date DESC LIMIT 1)   -- fix 4: the day's rate
                WHEN k.level = 4 THEN r.rate_utc
                ELSE r.rate_london END AS rate
    FROM rated r
    JOIN range(6) k(level) ON k.level BETWEEN r.from_level AND r.to_level
),
totals AS (
    SELECT level,
           sum(CASE WHEN currency = 'EUR' THEN sign(amount) * round(abs(amount) * rate) ELSE amount END)::BIGINT AS minor
    FROM levels
    WHERE report_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
    GROUP BY level
)
SELECT level, minor, lag(minor) OVER (ORDER BY level) - minor AS step_minor
FROM totals
ORDER BY level
