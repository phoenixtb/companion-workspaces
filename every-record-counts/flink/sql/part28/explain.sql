-- Part 28: what each query emits. EXPLAIN CHANGELOG_MODE prints every operator with the row kinds it produces:
-- I insert, UB update-before (-U), UA update-after (+U), D delete. Run with: python -m scripts.part28_flink explain
SET 'execution.runtime-mode' = 'streaming';

-- 1. dedup, first copy by arrival
EXPLAIN CHANGELOG_MODE SELECT * FROM baskets;

-- 2. dedup, first copy by event time
EXPLAIN CHANGELOG_MODE
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY basket_id ORDER BY event_ts ASC) AS rn FROM pos_basket_events)
WHERE rn = 1;

-- 3. versioned view, latest price per key
EXPLAIN CHANGELOG_MODE SELECT * FROM price_versions;

-- 4. temporal join, lines priced as of the sale
EXPLAIN CHANGELOG_MODE SELECT * FROM sales_lines;

-- 5. hourly tumbling window
EXPLAIN CHANGELOG_MODE
SELECT store_id, window_start, SUM(net_minor) FROM TABLE(TUMBLE(TABLE sales_lines, DESCRIPTOR(event_ts), INTERVAL '1' HOUR))
GROUP BY store_id, window_start, window_end;

-- 6. running total per store and day
EXPLAIN CHANGELOG_MODE
SELECT store_id, business_date, SUM(net_minor) FROM sales_lines GROUP BY store_id, business_date;

-- 7. top three stores per country
EXPLAIN CHANGELOG_MODE
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY sales_minor DESC) AS rn FROM (
    SELECT country, store_id, SUM(net_minor) AS sales_minor FROM sales_lines GROUP BY country, store_id))
WHERE rn <= 3;

-- 8. the same running total into the Iceberg upsert table (needs the tables: run after the job)
EXPLAIN CHANGELOG_MODE
INSERT INTO lake.`stream`.store_sales_daily
SELECT store_id, business_date, MAX(currency), COUNT(DISTINCT basket_id), SUM(qty), SUM(net_minor)
FROM sales_lines
GROUP BY store_id, business_date;

