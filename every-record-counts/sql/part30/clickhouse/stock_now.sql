-- Part 30: book stock for business day 15 August, now. Opening stock minus every unit sold (each line once).
SELECT count() AS store_skus,
       countIf(level <= 0) AS at_or_below_zero,
       round(100 * countIf(level > 0) / count(), 2) AS availability_pct,
       sum(sold) AS units_sold
FROM (
    SELECT o.store_id, o.sku, o.opening_qty - s.sold AS level, s.sold
    FROM realtime.opening_stock AS o
    LEFT JOIN (
        SELECT store_id, sku, sum(qty) AS sold
        FROM realtime.sale_lines FINAL
        WHERE business_date = '2026-08-15'
        GROUP BY store_id, sku
    ) AS s USING (store_id, sku)
);
