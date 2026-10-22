-- Part 1, step 2: what the log remembers about the OLD row.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/02-replica-identity.sql

-- REPLICA IDENTITY DEFAULT: the old row is identified by its primary key only.
UPDATE shipments SET hub_id = 'BLR-03', version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1004';

-- REPLICA IDENTITY FULL: the whole old row is written to the WAL.
ALTER TABLE shipments REPLICA IDENTITY FULL;
UPDATE shipments SET hub_id = 'BLR-07', version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1004';
DELETE FROM shipments WHERE tracking_code = 'DSP-1005';
ALTER TABLE shipments REPLICA IDENTITY DEFAULT;

SELECT data FROM pg_logical_slot_get_changes('part1_demo', NULL, NULL);

-- A table with no primary key, added to a publication (the thing pgoutput and Debezium read through).
CREATE TABLE hub_scans (tracking_code text, hub_id text, scanned_at timestamptz DEFAULT now());
INSERT INTO hub_scans (tracking_code, hub_id) VALUES ('DSP-1003', 'BLR-12');
CREATE PUBLICATION part1_pub FOR TABLE hub_scans;

-- Inserts still work. Updates and deletes are now rejected, on the application's connection.
INSERT INTO hub_scans (tracking_code, hub_id) VALUES ('DSP-1004', 'BLR-07');
UPDATE hub_scans SET hub_id = 'BLR-03' WHERE tracking_code = 'DSP-1003';

-- FULL gives the table an identity: every column of the old row.
ALTER TABLE hub_scans REPLICA IDENTITY FULL;
UPDATE hub_scans SET hub_id = 'BLR-03' WHERE tracking_code = 'DSP-1003';

DROP PUBLICATION part1_pub;
DROP TABLE hub_scans;
SELECT count(*) AS discarded FROM pg_logical_slot_get_changes('part1_demo', NULL, NULL);
