-- Part 6: subtotals in one pass with GROUPING SETS / ROLLUP / CUBE (DuckDB, Q3 2026).
-- CUBE (currency, channel) = GROUPING SETS ((currency, channel), (currency), (channel), ()).
-- Baskets add up across everything; money only within one currency, so the query refuses to add pounds to rupees.
SELECT CASE WHEN grouping(b.currency) = 1 THEN '(all)' ELSE b.currency END AS currency,
       CASE WHEN grouping(p.channel) = 1 THEN '(all)' ELSE p.channel END   AS channel,
       count(*)                                                             AS baskets,
       CASE WHEN grouping(b.currency) = 0 THEN major(sum(b.amount_minor)) END AS sales
FROM star.fct_basket b
JOIN star.dim_sale_profile p USING (profile_key)
JOIN star.dim_date d USING (date_key)
WHERE d.quarter = '2026-Q3'
GROUP BY CUBE (b.currency, p.channel)
ORDER BY grouping(b.currency), b.currency, grouping(p.channel), p.channel;
