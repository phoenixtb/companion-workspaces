-- Part 30: book stock per store and SKU for business day getvariable('day'), from a relation `stock_lines` (DuckDB):
-- the rows stock_now.sql counts.
SELECT o.store_id, o.sku, (o.opening_qty - coalesce(s.sold, 0))::BIGINT AS level
FROM read_csv(getvariable('opening_file')) AS o
LEFT JOIN (
    SELECT store_id, sku, sum(qty) AS sold
    FROM stock_lines
    WHERE business_date = getvariable('day')::DATE
    GROUP BY ALL
) AS s USING (store_id, sku);
