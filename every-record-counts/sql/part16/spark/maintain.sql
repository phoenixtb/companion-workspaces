-- Part 16: what eleven streaming MERGEs left in the current-state tables, then compaction (Spark SQL, run as the
-- pipeline principal). content 0 = data file, 1 = position delete file, 2 = equality delete file.
SELECT 'raw.orders' AS table_name, count_if(content = 0) AS data_files, count_if(content = 1) AS position_delete_files,
       coalesce(sum(CASE WHEN content = 1 THEN record_count END), 0) AS position_deletes,
       count_if(content = 2) AS equality_delete_files
FROM pipeline.raw.orders.files
UNION ALL
SELECT 'raw.payments', count_if(content = 0), count_if(content = 1),
       coalesce(sum(CASE WHEN content = 1 THEN record_count END), 0), count_if(content = 2)
FROM pipeline.raw.payments.files;

-- Rewrite every data file that has a delete file against it, applying the deletes; then drop delete files that no
-- longer apply to any live data file.
CALL pipeline.system.rewrite_data_files(table => 'raw.orders',
    options => map('delete-file-threshold', '1', 'remove-dangling-deletes', 'true'));
CALL pipeline.system.rewrite_data_files(table => 'raw.payments',
    options => map('delete-file-threshold', '1', 'remove-dangling-deletes', 'true'));

SELECT 'raw.orders' AS table_name, count_if(content = 0) AS data_files, count_if(content = 1) AS position_delete_files,
       coalesce(sum(CASE WHEN content = 1 THEN record_count END), 0) AS position_deletes,
       count_if(content = 2) AS equality_delete_files
FROM pipeline.raw.orders.files
UNION ALL
SELECT 'raw.payments', count_if(content = 0), count_if(content = 1),
       coalesce(sum(CASE WHEN content = 1 THEN record_count END), 0), count_if(content = 2)
FROM pipeline.raw.payments.files;

-- Where the delete files went: the rewrite commit and the dangling-deletes commit, as the snapshot log records them.
SELECT operation, summary['deleted-data-files'] AS data_files_rewritten,
       summary['removed-delete-files'] AS delete_files_removed, summary['removed-position-deletes'] AS deletes_removed
FROM pipeline.raw.orders.snapshots
WHERE operation = 'replace'
ORDER BY committed_at;
