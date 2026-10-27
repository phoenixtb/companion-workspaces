-- Part 3: read a Parquet footer with DuckDB, before reading any data. Run after `part03_formats.py write`:
--   uv run python scripts/duck.py --local sql/part03/footer.sql

-- 1. The schema the file carries: physical type plus logical annotation.
SELECT name, type, logical_type, duckdb_type
FROM parquet_schema('data/lab/part03/by_date.parquet')
WHERE name <> 'duckdb_schema';

-- 2. The file: who wrote it, rows, row groups, and how big the footer is.
SELECT created_by, num_rows, num_row_groups, file_size_bytes, footer_size
FROM parquet_file_metadata('data/lab/part03/by_date.parquet');

-- 3. One row group, column by column: encodings, bytes before and after the codec, min/max, bloom filter.
SELECT path_in_schema          AS "column",
       encodings,
       total_uncompressed_size AS encoded_bytes,
       total_compressed_size   AS stored_bytes,
       stats_min,
       stats_max,
       bloom_filter_length     AS bloom_bytes
FROM parquet_metadata('data/lab/part03/by_date.parquet')
WHERE row_group_id = 20
ORDER BY column_id;

-- 4. What a reader learns from the footers alone: which row groups could hold 15 August, in each sort order.
SELECT if(file_name LIKE '%by_date%', 'by business_date', 'by basket_id') AS sorted,
       count(*)                                                       AS row_groups,
       count(*) FILTER (WHERE stats_min::DATE <= DATE '2026-08-15'
                          AND stats_max::DATE >= DATE '2026-08-15')   AS may_hold_15_aug,
       max(stats_max::DATE - stats_min::DATE) + 1                     AS widest_range_days
FROM parquet_metadata(['data/lab/part03/sales_line.parquet', 'data/lab/part03/by_date.parquet'])
WHERE path_in_schema = 'business_date'
GROUP BY ALL
ORDER BY sorted;
