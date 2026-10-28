-- Part 10, step 4: fix layout A without partitioning it. Rewrite every file, clustered on a Z-order curve over the
-- two columns the queries filter on (Spark SQL; Iceberg's rewrite_data_files with the sort strategy).
CALL lakehouse.system.rewrite_data_files(
    table => 'part10.pos_flat', strategy => 'sort',
    sort_order => 'zorder(business_date, store_id)', options => map('rewrite-all', 'true'));
