-- Part 17: the dbt-built star, read as Farah's team reads it (Trino, the analyst principal, gold only).
-- Part 5's corrected numbers: UK net revenue for Q3 by London business date, stock as reported vs at close.

-- UK net revenue, Q3 2026, GBP (the rate of each business date, refunds on their own date)
SELECT country, sum(net_minor) AS net_minor
FROM lakehouse_analyst.gold.rpt_daily_net_revenue
WHERE business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
GROUP BY country
ORDER BY country;

-- Stock availability, Q3 2026, per store-SKU-day: the snapshot as reported vs at the close of the business day
SELECT avg(CASE WHEN f.on_hand_qty > 0 THEN 1e0 ELSE 0e0 END)         AS as_reported,
       avg(CASE WHEN f.closing_on_hand_qty > 0 THEN 1e0 ELSE 0e0 END) AS at_close
FROM lakehouse_analyst.gold.fct_stock_daily f
JOIN lakehouse_analyst.gold.dim_date d ON d.date_key = f.date_key
WHERE d.quarter = '2026-Q3';

-- Supplier fill rate and on time in full, Q3 2026 by expected date (Part 4's query, on Part 15's checked deliveries)
SELECT sum(coalesce(f.delivered_qty, 0)) * 1e0 / sum(f.ordered_qty)                              AS fill_rate,
       avg(CASE WHEN f.delivered_qty = f.ordered_qty AND f.days_late <= 0 THEN 1e0 ELSE 0e0 END) AS on_time_in_full
FROM lakehouse_analyst.gold.fct_po_line f
JOIN lakehouse_analyst.gold.dim_date e ON e.date_key = f.expected_date_key
WHERE e.quarter = '2026-Q3';
