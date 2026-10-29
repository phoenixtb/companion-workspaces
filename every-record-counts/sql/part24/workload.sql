-- Part 24: what people read every day, one query per dashboard tile. `scripts.part24_cost reads` runs each one three
-- times as user `lab` and keeps the median; cost/assumptions.json says how often each runs per day (an assumption).
-- Dates are fixed so the numbers repeat: the week of 24-30 September 2026.

-- finance_revenue: Farah's revenue dashboard reads the published report (Part 19's tag).
SELECT country, business_date, net_minor, report_currency
FROM lakehouse.gold.rpt_daily_net_revenue_v2 FOR VERSION AS OF 'published'
WHERE business_date BETWEEN DATE '2026-09-24' AND DATE '2026-09-30'
ORDER BY country, business_date;

-- uk_store_week: the UK store managers' tile: sales per store for the last seven London business days.
-- Written the way a backend engineer writes it: the local date computed from the event's timestamp.
SELECT s.store_id, count(*) AS lines, sum(f.amount_minor) AS sales_minor
FROM lakehouse.gold.fct_sales_line f
JOIN lakehouse.gold.dim_store s ON s.store_key = f.store_key
WHERE s.country = 'GB'
  AND date(f.event_ts AT TIME ZONE 'Europe/London') BETWEEN DATE '2026-09-24' AND DATE '2026-09-30'
GROUP BY s.store_id;

-- stock_yesterday: availability per country on the last closed day (Part 5's close-of-day stock).
SELECT s.country, count(*) AS store_skus, avg(CASE WHEN f.closing_on_hand_qty > 0 THEN 1.0 ELSE 0 END) AS availability
FROM lakehouse.gold.fct_stock_daily f
JOIN lakehouse.gold.dim_store s ON s.store_key = f.store_key
WHERE f.date_key = 20260930
GROUP BY s.country;

-- conversion_week: the contract's sessions (Part 20), converted share per platform, last seven UTC days.
SELECT platform, count(*) AS sessions, avg(CASE WHEN converted THEN 1.0 ELSE 0 END) AS conversion
FROM lakehouse.gold.web_sessions
WHERE started_at >= TIMESTAMP '2026-09-24 00:00:00 UTC' AND started_at < TIMESTAMP '2026-10-01 00:00:00 UTC'
GROUP BY platform;
