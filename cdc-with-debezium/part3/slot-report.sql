-- Every slot: is it read, how far behind is its consumer, how much WAL does it hold, how close to the cap.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/slot-report.sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))         AS retained,
       pg_size_pretty(safe_wal_size)                                               AS safe_wal_size
FROM pg_replication_slots
ORDER BY slot_name;
