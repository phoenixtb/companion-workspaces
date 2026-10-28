-- Headers as sent, per file: drift shows up here even when an alias absorbed it.
SELECT header, count(*) AS files, count(DISTINCT supplier_id) AS suppliers,
       min(file_date) AS first_file_date, max(file_date) AS last_file_date
FROM raw_supplier_files
GROUP BY header
ORDER BY files DESC;
