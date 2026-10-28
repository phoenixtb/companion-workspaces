-- Part 7, step 9: Trino 484 reads what Spark wrote (Trino SQL). Spark is stopped; only core + Trino run.
-- Branches and tags travel with the table: they live in its metadata, not in the engine that made them.
SELECT name, type, snapshot_id FROM lakehouse.part07."sales_line$refs" ORDER BY name;

-- Time travel by tag; Trino also reads the snapshot with its own schema (amount_minor, before the rename).
SELECT count(*) AS lines, sum(amount_minor) AS uk_sales_minor
FROM lakehouse.part07.sales_line FOR VERSION AS OF 'q3_close'
WHERE country = 'GB' AND business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30';

-- The v3 table: Trino calls format v3 experimental, but reads the VARIANT column and applies the deletion vector.
SELECT typeof(payload) AS payload_type, count(*) AS events FROM lakehouse.part07.clicks_v3 GROUP BY 1;

SELECT json_extract_scalar(CAST(payload AS JSON), '$.sku') AS sku, count(*) AS views
FROM lakehouse.part07.clicks_v3 WHERE event_type = 'product_view'
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 3;
