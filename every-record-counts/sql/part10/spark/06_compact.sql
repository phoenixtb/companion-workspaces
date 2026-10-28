-- Part 10, step 6: compaction. Rewrite October's files in the table's sort order; deletes are applied as the files
-- are rewritten, and delete files that no longer apply to any live file are dropped. Then regroup the manifests by
-- partition, so planning a query opens fewer of them.
CALL lakehouse.system.rewrite_data_files(table => 'part10.pos_monthly', strategy => 'sort',
    where => "business_date >= '2026-10-01'", options => map('remove-dangling-deletes', 'true'));

SELECT count(*) AS manifests FROM lakehouse.part10.pos_monthly.manifests;

CALL lakehouse.system.rewrite_manifests('part10.pos_monthly');

SELECT count(*) AS manifests FROM lakehouse.part10.pos_monthly.manifests;
