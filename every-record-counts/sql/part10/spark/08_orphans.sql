-- Part 10, step 7: orphan files, which no metadata references (a writer died before committing).
-- Listing needs prefix_listing on this stack: Spark has no Hadoop file system for s3://, Iceberg's S3FileIO lists.
-- With the default older_than (3 days ago), a fresh orphan is left alone: it could be an in-flight write.
CALL lakehouse.system.remove_orphan_files(table => 'part10.pos_monthly', prefix_listing => true, dry_run => true);

-- The procedure refuses anything under 24 hours, even for a dry run.
CALL lakehouse.system.remove_orphan_files(
    table          => 'part10.pos_monthly',
    older_than     => current_timestamp(),
    prefix_listing => true,
    dry_run        => true
);
