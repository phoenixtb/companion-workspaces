-- Part 8: one gold table for every format (DuckDB). Daily store sales recomputed from Part 7's staged POS lines:
-- uploads up to 30 September, one row per (basket_id, line_no), the first copy uploaded. Business dates run from
-- 1 June to 30 September, plus 46 store-days already on 1 October in local time.
COPY (
    SELECT store_id, business_date, currency,
           count(DISTINCT basket_id) AS baskets,
           sum(amount_minor)::BIGINT AS sales_minor   -- sum() of BIGINT is HUGEINT; Parquet would store DOUBLE
    FROM (
        FROM read_parquet('data/lab/part07/pos_lines/*/*.parquet', hive_partitioning = true)
        WHERE upload_date <= DATE '2026-09-30'
        QUALIFY row_number() OVER (PARTITION BY basket_id, line_no ORDER BY uploaded_at) = 1
    )
    GROUP BY ALL
    ORDER BY business_date, store_id
) TO 'data/lab/part08/daily_store_sales.parquet' (FORMAT parquet);
