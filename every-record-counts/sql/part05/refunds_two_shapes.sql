-- Part 5: one question asked of both shapes (DuckDB). UK refunds in Q3 by the month they happened and the month
-- of the purchase they refund, in GBP at the rate in force on the refund's London business date.

-- The vault: current state means the latest satellite row per key; every relationship is a link.
WITH refund AS (
    SELECT * FROM sat_refund QUALIFY row_number() OVER (PARTITION BY refund_hk ORDER BY load_ts DESC) = 1
),
sale AS (
    SELECT sale_hk, event_ts AS purchased_at FROM sat_sale_pos
    QUALIFY row_number() OVER (PARTITION BY sale_hk ORDER BY load_ts DESC) = 1
    UNION ALL
    SELECT sale_hk, created_at FROM sat_sale_order
    QUALIFY row_number() OVER (PARTITION BY sale_hk ORDER BY load_ts DESC) = 1
),
store AS (
    SELECT * FROM sat_store QUALIFY row_number() OVER (PARTITION BY store_hk ORDER BY load_ts DESC) = 1
),
gb AS (
    SELECT (r.refund_ts AT TIME ZONE st.timezone)::DATE    AS refund_date,
           (s.purchased_at AT TIME ZONE st.timezone)::DATE AS purchase_date,
           r.currency, r.amount_minor
    FROM refund r
    JOIN lnk_refund_sale rs ON rs.refund_hk = r.refund_hk
    JOIN sale s ON s.sale_hk = rs.sale_hk
    JOIN lnk_sale_store ss ON ss.sale_hk = rs.sale_hk
    JOIN store st ON st.store_hk = ss.store_hk
    WHERE st.country = 'GB'
)
SELECT strftime(refund_date, '%Y-%m') AS refund_month, strftime(purchase_date, '%Y-%m') AS purchase_month,
       count(*) AS refunds, major(sum(convert_minor(amount_minor, fx.rate))) AS refunds_gbp
FROM gb
JOIN dim_fx_rate fx ON fx.base_currency = gb.currency AND fx.quote_currency = 'GBP' AND fx.date = gb.refund_date
WHERE refund_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
GROUP BY ALL
ORDER BY ALL;

-- The wide table: the same answer, one table, no joins.
SELECT strftime(refund_date, '%Y-%m') AS refund_month, strftime(purchase_date, '%Y-%m') AS purchase_month,
       count(*) AS refunds, major(sum(amount_gbp_minor)) AS refunds_gbp
FROM wide_refund
WHERE country = 'GB' AND refund_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
GROUP BY ALL
ORDER BY ALL;

-- What only the vault can say: everything it was told about one refunded UK euro order, and when.
WITH one_order AS (
    SELECT min(sale_id) AS sale_id FROM wide_refund WHERE country = 'GB' AND currency = 'EUR'
)
SELECT h.sale_id, s.load_ts, s.record_source, s.status, s.total_minor, s.currency
FROM hub_sale h JOIN sat_sale_order s USING (sale_hk)
WHERE h.sale_id = (SELECT sale_id FROM one_order)
UNION ALL
SELECT h.sale_id, r.load_ts, r.record_source, 'refunded: ' || r.reason, r.amount_minor, r.currency
FROM hub_sale h JOIN lnk_refund_sale l USING (sale_hk) JOIN sat_refund r USING (refund_hk)
WHERE h.sale_id = (SELECT sale_id FROM one_order)
ORDER BY load_ts;
