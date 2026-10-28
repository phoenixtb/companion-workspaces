-- Part 5: the last two fixes of the UK revenue waterfall, on the star (DuckDB).
-- The star already has steps 1-3 fixed: one row per basket, the current orders table, refunds dated by refund.
-- From there: step 4 converts euros as of the transaction date, step 5 dates everything in London.

-- Weekends carry Friday's rate (a few rows of the densified rate table).
SELECT date, dayname(date) AS day, rate_date, rate, carried_forward
FROM dim_fx_rate
WHERE base_currency = 'EUR' AND date BETWEEN DATE '2026-09-25' AND DATE '2026-09-28'
ORDER BY date;

-- The waterfall's last two steps.
WITH gb AS (
    SELECT 'sale' AS kind, any_value(l.event_ts) AS ts, any_value(l.currency) AS currency,
           sum(l.amount_minor) AS amount                     -- convert the basket's total, as the PSP does
    FROM fct_sales_line l JOIN dim_store s USING (store_key)
    WHERE s.country = 'GB'
    GROUP BY l.transaction_id
    UNION ALL
    SELECT 'refund', r.refund_ts, r.currency, -r.refund_minor
    FROM fct_refund r JOIN dim_store s USING (store_key)
    WHERE s.country = 'GB'
),
dated AS (
    SELECT currency, amount,
           ts::DATE                                 AS utc_date,     -- the session time zone is UTC
           (ts AT TIME ZONE 'Europe/London')::DATE  AS london_date
    FROM gb
),
eur AS (SELECT date, rate FROM dim_fx_rate WHERE base_currency = 'EUR' AND quote_currency = 'GBP'),
converted AS (
    SELECT d.utc_date, d.london_date,
           CASE WHEN d.currency = 'EUR' THEN convert_minor(d.amount, report_day.rate) ELSE d.amount END
               AS at_report_day_rate,                                -- the board deck: one rate, 5 October's
           CASE WHEN d.currency = 'EUR' THEN convert_minor(d.amount, on_utc.rate) ELSE d.amount END
               AS at_utc_date_rate,
           CASE WHEN d.currency = 'EUR' THEN convert_minor(d.amount, on_london.rate) ELSE d.amount END
               AS at_london_date_rate
    FROM dated d
    JOIN eur report_day ON report_day.date = DATE '2026-10-05'
    JOIN eur on_utc ON on_utc.date = d.utc_date
    JOIN eur on_london ON on_london.date = d.london_date
),
steps AS (
    SELECT 3 AS step, 'steps 1-3 fixed: UTC dates, report-day rate' AS uk_net_revenue,
           sum(at_report_day_rate) FILTER (WHERE utc_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30') AS minor
    FROM converted
    UNION ALL
    SELECT 4, '+ step 4: rate of the transaction date',
           sum(at_utc_date_rate) FILTER (WHERE utc_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30')
    FROM converted
    UNION ALL
    SELECT 5, '+ step 5: London business date',
           sum(at_london_date_rate) FILTER (WHERE london_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30')
    FROM converted
)
SELECT uk_net_revenue, major(minor) AS gbp, major(minor - lag(minor) OVER (ORDER BY step)) AS change_gbp
FROM steps
ORDER BY step;

-- Step 5, taken apart: sales and refunds, quarter cut in UTC vs in London (rates as of each date).
WITH gb AS (
    SELECT 'sales' AS side, any_value(l.event_ts) AS ts, any_value(l.currency) AS currency, sum(l.amount_minor) AS amount
    FROM fct_sales_line l JOIN dim_store s USING (store_key)
    WHERE s.country = 'GB'
    GROUP BY l.transaction_id
    UNION ALL
    SELECT 'refunds', r.refund_ts, r.currency, -r.refund_minor
    FROM fct_refund r JOIN dim_store s USING (store_key)
    WHERE s.country = 'GB'
),
dated AS (
    SELECT side, currency, amount, ts::DATE AS utc_date, (ts AT TIME ZONE 'Europe/London')::DATE AS london_date
    FROM gb
),
eur AS (SELECT date, rate FROM dim_fx_rate WHERE base_currency = 'EUR' AND quote_currency = 'GBP'),
converted AS (
    SELECT d.side, d.utc_date, d.london_date,
           CASE WHEN d.currency = 'EUR' THEN convert_minor(d.amount, u.rate) ELSE d.amount END AS by_utc,
           CASE WHEN d.currency = 'EUR' THEN convert_minor(d.amount, l.rate) ELSE d.amount END AS by_london
    FROM dated d JOIN eur u ON u.date = d.utc_date JOIN eur l ON l.date = d.london_date
)
SELECT coalesce(side, 'both') AS side,
       count(*) FILTER (WHERE utc_date <> london_date)                                       AS rows_dates_differ,
       major(sum(by_utc) FILTER (WHERE utc_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'))       AS q3_utc,
       major(sum(by_london) FILTER (WHERE london_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30')) AS q3_london,
       q3_london - q3_utc                                                                    AS change_gbp
FROM converted
GROUP BY ROLLUP (side)
ORDER BY GROUPING(side), side;

-- Why the quarter barely moved: only its two edges can move it. Daily totals move much more.
WITH gb AS (
    SELECT any_value(l.event_ts) AS ts, sum(l.amount_minor) AS amount
    FROM fct_sales_line l JOIN dim_store s USING (store_key)
    WHERE s.country = 'GB' AND l.currency = 'GBP'
    GROUP BY l.transaction_id
),
by_utc AS (SELECT ts::DATE AS date, sum(amount) AS minor FROM gb GROUP BY ALL),
by_london AS (SELECT (ts AT TIME ZONE 'Europe/London')::DATE AS date, sum(amount) AS minor FROM gb GROUP BY ALL)
SELECT count(*)                                             AS q3_days,
       count(*) FILTER (WHERE u.minor <> l.minor)            AS days_that_differ,
       major(max(abs(l.minor - u.minor)))                    AS largest_daily_change_gbp,
       major(sum(abs(l.minor - u.minor)))                    AS sum_of_daily_changes_gbp
FROM by_utc u JOIN by_london l USING (date)
WHERE date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';
