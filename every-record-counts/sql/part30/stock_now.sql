-- Part 30: book stock for business day getvariable('day'), from a relation `stock_lines` of sale lines (DuckDB).
-- Opening stock (Part 29's WMS file) minus every unit sold, each line once. The same question as
-- sql/part30/clickhouse/stock_now.sql asks ClickHouse.
SELECT count(*) AS store_skus,
       count(*) FILTER (WHERE level <= 0) AS at_or_below_zero,
       round(100 * count(*) FILTER (WHERE level > 0) / count(*), 2) AS availability_pct,
       sum(sold)::BIGINT AS units_sold
FROM (
    SELECT o.store_id, o.sku, o.opening_qty - coalesce(s.sold, 0) AS level, coalesce(s.sold, 0) AS sold
    FROM read_csv(getvariable('opening_file')) AS o
    LEFT JOIN (
        SELECT store_id, sku, sum(qty) AS sold
        FROM stock_lines
        WHERE business_date = getvariable('day')::DATE
        GROUP BY ALL
    ) AS s USING (store_id, sku)
);
