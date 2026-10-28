-- One dlt load per run: its id is on every row it wrote, and each load committed one Iceberg snapshot.
WITH per_load AS (
    SELECT _dlt_load_id AS load_id, min(_ingested_at) AS ingested_at, count(DISTINCT _source_file) AS files,
           count(*) AS rows, min(file_date) AS first_file_date, max(file_date) AS last_file_date,
           row_number() OVER (ORDER BY _dlt_load_id) AS n
    FROM raw_supplier_deliveries
    GROUP BY _dlt_load_id),
snaps AS (SELECT *, row_number() OVER (ORDER BY committed_at) AS n FROM snapshots)
SELECT load_id, ingested_at, files, rows, first_file_date, last_file_date, snapshot_id, added_rows
FROM per_load JOIN snaps USING (n)
ORDER BY load_id;
