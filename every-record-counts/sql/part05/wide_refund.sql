-- Part 5: the same refunds as one wide table, built from the vault (DuckDB).
-- One row per refund, every attribute a report filters or groups by already on the row: local business dates,
-- the store, the purchase it refunds, and the amount in GBP at the rate in force on the refund's business date.
-- Rebuilt in full on every load; it keeps no history of its own.
CREATE OR REPLACE TABLE wide_refund AS
WITH refund AS (                               -- the latest row of each satellite is the current state
    SELECT * FROM sat_refund QUALIFY row_number() OVER (PARTITION BY refund_hk ORDER BY load_ts DESC) = 1
),
sale AS (                                      -- a sale is a till basket or an online order: two satellites
    SELECT sale_hk, event_ts AS purchased_at, 'pos' AS sale_source FROM sat_sale_pos
    QUALIFY row_number() OVER (PARTITION BY sale_hk ORDER BY load_ts DESC) = 1
    UNION ALL
    SELECT sale_hk, created_at, 'online' FROM sat_sale_order
    QUALIFY row_number() OVER (PARTITION BY sale_hk ORDER BY load_ts DESC) = 1
),
store AS (
    SELECT * FROM sat_store QUALIFY row_number() OVER (PARTITION BY store_hk ORDER BY load_ts DESC) = 1
)
SELECT hr.refund_id,
       hs.sale_id,
       r.country,
       hst.store_id,
       st.city                                       AS store_city,
       st.format                                     AS store_format,
       st.timezone,
       r.original_channel,
       r.reason,
       r.tender_type,
       r.refund_ts,
       (r.refund_ts AT TIME ZONE st.timezone)::DATE   AS refund_date,      -- local business date
       s.purchased_at,
       (s.purchased_at AT TIME ZONE st.timezone)::DATE AS purchase_date,
       r.currency,
       r.amount_minor,
       fx.rate_date                                  AS fx_rate_date,     -- Friday's, for a weekend refund
       fx.rate                                       AS fx_rate_to_gbp,
       convert_minor(r.amount_minor, fx.rate)        AS amount_gbp_minor
FROM refund r
JOIN hub_refund hr ON hr.refund_hk = r.refund_hk
JOIN lnk_refund_sale rs ON rs.refund_hk = r.refund_hk
JOIN hub_sale hs ON hs.sale_hk = rs.sale_hk
JOIN sale s ON s.sale_hk = rs.sale_hk
JOIN lnk_sale_store ss ON ss.sale_hk = rs.sale_hk
JOIN hub_store hst ON hst.store_hk = ss.store_hk
JOIN store st ON st.store_hk = ss.store_hk
JOIN dim_fx_rate fx
    ON fx.base_currency = r.currency AND fx.quote_currency = 'GBP'
   AND fx.date = (r.refund_ts AT TIME ZONE st.timezone)::DATE;
