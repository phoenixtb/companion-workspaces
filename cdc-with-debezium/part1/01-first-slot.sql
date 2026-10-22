-- Part 1, step 1: read the WAL through a logical replication slot.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/01-first-slot.sql

-- Start from the four seed shipments, so the script can be re-run.
TRUNCATE shipments RESTART IDENTITY;
INSERT INTO shipments (tracking_code, status, hub_id, eta) VALUES
    ('DSP-1001', 'PICKED_UP', 'BLR-07', '2026-10-23 18:00+05:30'),
    ('DSP-1002', 'CREATED',   'BLR-07', '2026-10-24 12:00+05:30'),
    ('DSP-1003', 'PICKED_UP', 'BLR-12', '2026-10-23 20:00+05:30'),
    ('DSP-1004', 'CREATED',   'BLR-12', '2026-10-24 15:00+05:30');

-- A slot plus test_decoding, the demo output plugin that ships with PostgreSQL and prints changes as text.
SELECT * FROM pg_create_logical_replication_slot('part1_demo', 'test_decoding');

-- One transaction, three statements.
BEGIN;
INSERT INTO shipments (tracking_code, status, hub_id, eta)
VALUES ('DSP-1005', 'CREATED', 'BLR-07', '2026-10-25 10:00+05:30');
UPDATE shipments SET status = 'AT_HUB', version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1001';
DELETE FROM shipments WHERE tracking_code = 'DSP-1002';
COMMIT;

-- A transaction that changes its mind.
BEGIN;
UPDATE shipments SET status = 'DELIVERED', version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1003';
ROLLBACK;

-- A single statement in autocommit mode is a transaction too.
UPDATE shipments SET status = 'OUT_FOR_DELIVERY', version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1001';

-- Peek: read the decoded changes without consuming them.
SELECT lsn, xid, data FROM pg_logical_slot_peek_changes('part1_demo', NULL, NULL);

-- Where the slot is, and where the server's WAL is.
SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn, pg_current_wal_lsn() AS current_lsn
FROM pg_replication_slots;

-- Get: read and consume. The slot's confirmed_flush_lsn moves past everything returned.
SELECT count(*) AS consumed FROM pg_logical_slot_get_changes('part1_demo', NULL, NULL);
SELECT count(*) AS still_there FROM pg_logical_slot_peek_changes('part1_demo', NULL, NULL);
