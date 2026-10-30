-- Part 31: three plausible ad-hoc queries per number, each making one mistake the incident taught (Trino).
-- Run as analyst_gb, through the governed catalog: the same tables and rights Cube has. Only the last one, which
-- needs raw events, runs as a platform identity (an analyst can't read the raw zone at all).

-- revenue, the definition: baskets once, refunds on their own date, the business date's rate, local dates
WITH m AS (
    SELECT date_key, store_key, currency, amount_minor FROM governed.gold.fct_basket
    UNION ALL
    SELECT refund_date_key, store_key, currency, -refund_minor FROM governed.gold.fct_refund
)
SELECT CAST(CAST(sum(CAST(sign(m.amount_minor) * round(abs(m.amount_minor) * fx.rate) AS bigint)) AS DECIMAL(18, 0))
            * 0.01 AS varchar) AS gbp
FROM m
JOIN governed.gold.dim_store s ON s.store_key = m.store_key
JOIN governed.gold.dim_date d ON d.date_key = m.date_key
JOIN governed.gold.dim_fx_rate fx ON fx."date" = d."date" AND fx.base_currency = m.currency AND fx.quote_currency = 'GBP'
WHERE s.country = 'GB' AND d."date" BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';

-- revenue, refunds dated by the purchase (Part 4's mistake)
WITH m AS (
    SELECT date_key, store_key, currency, amount_minor FROM governed.gold.fct_basket
    UNION ALL
    SELECT purchase_date_key, store_key, currency, -refund_minor FROM governed.gold.fct_refund
)
SELECT CAST(CAST(sum(CAST(sign(m.amount_minor) * round(abs(m.amount_minor) * fx.rate) AS bigint)) AS DECIMAL(18, 0))
            * 0.01 AS varchar) AS gbp
FROM m
JOIN governed.gold.dim_store s ON s.store_key = m.store_key
JOIN governed.gold.dim_date d ON d.date_key = m.date_key
JOIN governed.gold.dim_fx_rate fx ON fx."date" = d."date" AND fx.base_currency = m.currency AND fx.quote_currency = 'GBP'
WHERE s.country = 'GB' AND d."date" BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';

-- revenue, every euro at today's rate (Part 5's mistake): the latest rate in the table
WITH m AS (
    SELECT date_key, store_key, currency, amount_minor FROM governed.gold.fct_basket
    UNION ALL
    SELECT refund_date_key, store_key, currency, -refund_minor FROM governed.gold.fct_refund
),
today AS (
    SELECT base_currency, rate FROM governed.gold.dim_fx_rate
    WHERE quote_currency = 'GBP' AND "date" = (SELECT max("date") FROM governed.gold.dim_fx_rate)
)
SELECT CAST(CAST(sum(CAST(sign(m.amount_minor) * round(abs(m.amount_minor) * t.rate) AS bigint)) AS DECIMAL(18, 0))
            * 0.01 AS varchar) AS gbp
FROM m
JOIN governed.gold.dim_store s ON s.store_key = m.store_key
JOIN governed.gold.dim_date d ON d.date_key = m.date_key
JOIN today t ON t.base_currency = m.currency
WHERE s.country = 'GB' AND d."date" BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';

-- revenue, the UTC date instead of the store's business date (Part 5's other mistake)
WITH m AS (
    SELECT CAST(event_ts AS date) AS utc_date, store_key, currency, amount_minor FROM governed.gold.fct_basket
    UNION ALL
    SELECT CAST(refund_ts AS date), store_key, currency, -refund_minor FROM governed.gold.fct_refund
)
SELECT CAST(CAST(sum(CAST(sign(m.amount_minor) * round(abs(m.amount_minor) * fx.rate) AS bigint)) AS DECIMAL(18, 0))
            * 0.01 AS varchar) AS gbp
FROM m
JOIN governed.gold.dim_store s ON s.store_key = m.store_key
JOIN governed.gold.dim_fx_rate fx ON fx."date" = m.utc_date AND fx.base_currency = m.currency AND fx.quote_currency = 'GBP'
WHERE s.country = 'GB' AND m.utc_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';

-- stock, the definition: store-SKU-days in stock at the close of the business day
SELECT round(100 * avg(CASE WHEN f.closing_on_hand_qty > 0 THEN 1e0 ELSE 0e0 END), 1) AS pct
FROM governed.gold.fct_stock_daily f
JOIN governed.gold.dim_date d ON d.date_key = f.date_key
WHERE d.quarter = '2026-Q3';

-- stock, the snapshot as reported, some stores counting after the morning delivery (Part 5's mistake)
SELECT round(100 * avg(CASE WHEN f.on_hand_qty > 0 THEN 1e0 ELSE 0e0 END), 1) AS pct
FROM governed.gold.fct_stock_daily f
JOIN governed.gold.dim_date d ON d.date_key = f.date_key
WHERE d.quarter = '2026-Q3';

-- stock, units summed over each ISO week, any stock that week counts (the old dashboard, Part 4's mistake)
SELECT round(100 * avg(CASE WHEN weekly_on_hand > 0 THEN 1e0 ELSE 0e0 END), 1) AS pct
FROM (
    SELECT f.store_key, p.sku, d.iso_week, sum(f.on_hand_qty) AS weekly_on_hand
    FROM governed.gold.fct_stock_daily f
    JOIN governed.gold.dim_date d ON d.date_key = f.date_key
    JOIN governed.gold.dim_product p ON p.product_key = f.product_key
    WHERE d.quarter = '2026-Q3'
    GROUP BY 1, 2, 3
);

-- conversion, the definition: the sessions contract (Part 20)
SELECT count(*) AS sessions, count_if(converted) AS converted,
       round(100e0 * count_if(converted) / count(*), 2) AS pct
FROM governed.gold.web_sessions;

-- conversion, marketing's old rule: one session per visitor per UTC day, bots in (as lab)
SELECT count(*) AS sessions, count_if(purchased) AS converted,
       round(100e0 * count_if(purchased) / count(*), 2) AS pct
FROM (
    SELECT anonymous_id, CAST(event_ts AS date) AS utc_day, bool_or(event_type = 'purchase') AS purchased
    FROM lakehouse.raw.clickstream_events
    WHERE event_ts >= TIMESTAMP '2026-07-01 00:00:00 UTC' AND event_ts < TIMESTAMP '2026-10-01 00:00:00 UTC'
    GROUP BY 1, 2
)
