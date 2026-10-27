-- The same correction against the Iceberg table (Trino), then what the commit wrote.
UPDATE lakehouse.part01.sales_line
SET amount_minor = amount_minor - 100, discount_minor = discount_minor + 100
WHERE basket_id = 'GB-0042-20260815-00009' AND line_no = 1;

SELECT snapshot_id, operation,
       element_at(summary, 'added-data-files')       AS added_data_files,
       element_at(summary, 'added-position-deletes') AS added_position_deletes,
       element_at(summary, 'total-records')          AS total_records
FROM lakehouse.part01."sales_line$snapshots"
ORDER BY committed_at;

-- content 0 = data file, 1 = position-delete file
SELECT content, record_count, file_size_in_bytes
FROM lakehouse.part01."sales_line$files";
