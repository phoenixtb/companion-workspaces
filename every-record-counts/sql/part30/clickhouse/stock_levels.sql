-- Part 30: book stock per store and SKU for business day 15 August, now (the rows stock_now.sql counts).
SELECT o.store_id, o.sku, toInt64(o.opening_qty) - toInt64(s.sold) AS level
FROM realtime.opening_stock AS o
LEFT JOIN (
    SELECT store_id, sku, sum(qty) AS sold
    FROM realtime.sale_lines FINAL
    WHERE business_date = '2026-08-15'
    GROUP BY store_id, sku
) AS s USING (store_id, sku);
