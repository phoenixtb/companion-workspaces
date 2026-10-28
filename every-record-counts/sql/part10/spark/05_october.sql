-- Part 10: the state of pos_monthly's October partition (month 681 = October 2026, counted from January 1970).
-- content 0 = data files, 1 = position-delete files.
SELECT content, count(*) AS files, round(avg(file_size_in_bytes) / 1024, 1) AS avg_kib, sum(record_count) AS records
FROM lakehouse.part10.pos_monthly.files
WHERE partition.business_date_month = 681
GROUP BY content
ORDER BY content;

SELECT (SELECT count(*) FROM lakehouse.part10.pos_monthly.snapshots) AS snapshots,
       (SELECT count(*) FROM lakehouse.part10.pos_monthly.manifests) AS manifests_in_current_snapshot;
