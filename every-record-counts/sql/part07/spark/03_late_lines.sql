-- Part 7, step 4: the 1 October upload arrives, with late baskets from Q3 and re-sends of baskets already
-- loaded (Spark SQL). Insert each (basket, line) that isn't in the table yet; re-sends match and are skipped.
CREATE TEMPORARY VIEW pos_lines USING parquet OPTIONS (path '/opt/lakehouse/lab/part07/pos_lines');

MERGE INTO lakehouse.part07.sales_line t
USING (SELECT basket_id, CAST(line_no AS INT) AS line_no, store_id, country, currency, event_ts, business_date, sku,
              CAST(qty AS INT) AS qty, unit_price_minor, discount_minor, amount_minor AS net_amount_minor,
              uploaded_at, till_id
       FROM (SELECT *, row_number() OVER (PARTITION BY basket_id, line_no ORDER BY uploaded_at) AS copy_no
             FROM pos_lines WHERE upload_date = DATE '2026-10-01')
       WHERE copy_no = 1) s
ON t.basket_id = s.basket_id AND t.line_no = s.line_no
WHEN NOT MATCHED THEN INSERT *;

-- Old files keep spec 0 (by day); the new upload's files use spec 1 (by day and country).
SELECT spec_id, count(*) AS data_files, sum(record_count) AS rows,
       min(partition.event_ts_day) AS first_day, max(partition.event_ts_day) AS last_day
FROM lakehouse.part07.sales_line.files
GROUP BY spec_id ORDER BY spec_id;

-- The late baskets: lines rung up on 30 September (UTC) that arrived with the 1 October upload.
SELECT partition.country AS country, record_count AS late_lines_30_sep
FROM lakehouse.part07.sales_line.files
WHERE spec_id = 1 AND partition.event_ts_day = DATE '2026-09-30'
ORDER BY country;
