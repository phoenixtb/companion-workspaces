-- Part 10, step 6: snapshot expiry. Forget every snapshot but the current one, and delete the files only they used.
-- The lab expires everything up to now; production keeps days of history (see the post).
-- older_than takes a literal or current_timestamp(); current_timestamp() - INTERVAL 1 MINUTE failed to bind
CALL lakehouse.system.expire_snapshots(table => 'part10.pos_monthly', older_than => current_timestamp(), retain_last => 1);
