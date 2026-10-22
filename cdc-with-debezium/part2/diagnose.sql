-- First queries to run when a Debezium PostgreSQL connector isn't delivering what you expect.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part2/diagnose.sql

SHOW wal_level;                                            -- must be logical

-- Which tables the connector's publication actually sends (table.include.list can't add to this).
SELECT pubname, schemaname, tablename FROM pg_publication_tables;

-- Who holds each slot, and how far behind it is.
SELECT slot_name, plugin, active, active_pid, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed
FROM pg_replication_slots;

-- Replica identity per captured table: d = default (primary key), f = full, n = nothing, i = index.
SELECT relname, relreplident FROM pg_class WHERE relname IN (SELECT tablename FROM pg_publication_tables);

-- The connector's own session, as PostgreSQL sees it.
SELECT pid, usename, application_name, backend_type, state FROM pg_stat_activity WHERE usename = 'debezium';
