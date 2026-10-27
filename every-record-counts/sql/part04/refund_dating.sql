-- UK refunds in Q3 2026, in GBP: dated by the purchase they refund (the board deck) vs by the refund (DuckDB).
-- Euros at the report-day rate, as the board deck converted them (Part 5 fixes that).
-- First row: the quarter cut at UTC midnight, as step 3 of docs/injected-defects.md measures it.
-- Second row: the star's own dates, the local business date in London.
WITH report_day_eur AS (
    SELECT rate FROM raw_fx_rates WHERE base_currency = 'EUR' ORDER BY rate_date DESC LIMIT 1
),
gb AS (
    SELECT CASE WHEN r.currency = 'EUR' THEN round(r.refund_minor * (SELECT rate FROM report_day_eur))
                ELSE r.refund_minor END AS gbp_minor,
           r.refund_ts::DATE   AS refund_utc_date,      -- the session time zone is UTC
           r.purchased_at::DATE AS purchase_utc_date,
           rd.quarter AS refund_quarter,
           pd.quarter AS purchase_quarter
    FROM fct_refund r
    JOIN dim_date rd ON rd.date_key = r.refund_date_key    -- dim_date playing "refund date"
    JOIN dim_date pd ON pd.date_key = r.purchase_date_key  -- and "purchase date"
    JOIN dim_store s ON s.store_key = r.store_key
    WHERE s.country = 'GB'
)
SELECT 'UTC dates (waterfall step 3)' AS dates,
       major(sum(gbp_minor) FILTER (WHERE purchase_utc_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'))
           AS by_purchase_date,
       major(sum(gbp_minor) FILTER (WHERE refund_utc_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'))
           AS by_refund_date,
       by_refund_date - by_purchase_date AS board_deck_overstated_by
FROM gb
UNION ALL
SELECT 'London business dates',
       major(sum(gbp_minor) FILTER (WHERE purchase_quarter = '2026-Q3')),
       major(sum(gbp_minor) FILTER (WHERE refund_quarter = '2026-Q3')),
       major(sum(gbp_minor) FILTER (WHERE refund_quarter = '2026-Q3'))
           - major(sum(gbp_minor) FILTER (WHERE purchase_quarter = '2026-Q3'))
FROM gb;
