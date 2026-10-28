-- Part 6: read a plan. EXPLAIN shows what DuckDB will do; EXPLAIN ANALYZE runs it and adds rows and time per operator.
-- The query: re-sent POS baskets per country, straight from the raw JSON files.
EXPLAIN ANALYZE
SELECT country, count(*) AS resends
FROM (SELECT left(store_id, 2) AS country
      FROM raw_pos_events
      QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) > 1)
GROUP BY ALL;
