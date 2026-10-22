-- Part 1, step 3: a slot nobody reads keeps every byte of WAL written after it, from every database.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/03-slot-pins-wal.sql

-- Per slot: how much WAL its consumer hasn't confirmed, how much it holds on disk, how close it is to the cap.
\set slot_report 'SELECT slot_name, wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained, pg_size_pretty(safe_wal_size) AS safe_wal_size FROM pg_replication_slots'
\set wal_dir 'SELECT count(*) AS wal_files, pg_size_pretty(sum(size)) AS pg_wal_size FROM pg_ls_waldir()'
:slot_report;
:wal_dir;

-- Busy traffic in ANOTHER database on the same server. The slot belongs to dispatchly,
-- but the WAL is shared by the whole server, so the slot holds on to this too.
CREATE DATABASE hub_archive;
\c hub_archive
CREATE TABLE scan_noise (id bigint, payload text);
INSERT INTO scan_noise SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
CHECKPOINT;
\c dispatchly
:slot_report;
:wal_dir;

-- A consumer confirming its position is what releases WAL. Here we confirm by hand.
SELECT slot_name, end_lsn FROM pg_replication_slot_advance('part1_demo', pg_current_wal_lsn());
CHECKPOINT;
:slot_report;
:wal_dir;

-- Done experimenting: drop the slot, or it starts pinning WAL again from here.
SELECT pg_drop_replication_slot('part1_demo');
DROP DATABASE hub_archive;
