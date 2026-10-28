-- Part 10, step 2: what each layout wrote (Spark SQL over Iceberg's `files` metadata table).
-- split_offsets lists where each Parquet row group starts, so its size is the file's row-group count.
SELECT layout, partitions, data_files, round(total_bytes / 1048576, 1) AS total_mib,
       round(avg_bytes / 1048576, 2) AS avg_file_mib, round(min_bytes / 1048576, 2) AS min_file_mib,
       row_groups
FROM (SELECT 'A flat' AS layout, 1 AS partitions, count(*) AS data_files, sum(file_size_in_bytes) AS total_bytes,
             avg(file_size_in_bytes) AS avg_bytes, min(file_size_in_bytes) AS min_bytes,
             sum(size(split_offsets)) AS row_groups
      FROM lakehouse.part10.pos_flat.files
      UNION ALL
      SELECT 'B daily', count(DISTINCT partition), count(*), sum(file_size_in_bytes), avg(file_size_in_bytes),
             min(file_size_in_bytes), sum(size(split_offsets))
      FROM lakehouse.part10.pos_daily.files
      UNION ALL
      SELECT 'C monthly, sorted', count(DISTINCT partition), count(*), sum(file_size_in_bytes),
             avg(file_size_in_bytes), min(file_size_in_bytes), sum(size(split_offsets))
      FROM lakehouse.part10.pos_monthly.files)
ORDER BY layout;

-- How well does each layout cluster by date? The business-date range one file covers, on average (from the bounds
-- Iceberg keeps per file; a query for one month can skip a file only if its range misses the month).
SELECT 'A flat' AS layout, round(avg(datediff(CAST(readable_metrics.business_date.upper_bound AS DATE),
                                              CAST(readable_metrics.business_date.lower_bound AS DATE)) + 1), 1)
           AS days_per_file
FROM lakehouse.part10.pos_flat.files
UNION ALL
SELECT 'B daily', round(avg(datediff(CAST(readable_metrics.business_date.upper_bound AS DATE),
                                     CAST(readable_metrics.business_date.lower_bound AS DATE)) + 1), 1)
FROM lakehouse.part10.pos_daily.files
UNION ALL
SELECT 'C monthly, sorted', round(avg(datediff(CAST(readable_metrics.business_date.upper_bound AS DATE),
                                               CAST(readable_metrics.business_date.lower_bound AS DATE)) + 1), 1)
FROM lakehouse.part10.pos_monthly.files;
