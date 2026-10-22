-- Part 3, drill 2: a table that predates CDC, and the signal table that lets us backfill it.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/02-hubs-and-signals.sql

-- Dispatchly's 40 hubs, created long before anyone captured them.
CREATE TABLE hubs (id text PRIMARY KEY, city text NOT NULL, capacity integer NOT NULL);
INSERT INTO hubs SELECT 'BLR-' || lpad(g::text, 2, '0'), 'Bengaluru', 500 + 10 * g FROM generate_series(1, 40) g;
GRANT SELECT ON hubs TO debezium;

-- Signals: id, type and data, exactly these names. Debezium reads signals from the WAL and
-- writes its snapshot watermarks back into the same table, so it needs INSERT.
CREATE TABLE debezium_signal (id varchar(64) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048) NULL);
GRANT SELECT, INSERT ON debezium_signal TO debezium;

-- Heartbeats (drill 3): a one-row table Debezium updates, so a quiet database still produces WAL it reads.
CREATE TABLE debezium_heartbeat (id integer PRIMARY KEY, beat_at timestamptz NOT NULL);
GRANT SELECT, INSERT, UPDATE ON debezium_heartbeat TO debezium;

-- With a DBA-owned publication, all three must be published, or Debezium never sees them.
ALTER PUBLICATION dbz_dispatchly ADD TABLE hubs, debezium_signal, debezium_heartbeat;
