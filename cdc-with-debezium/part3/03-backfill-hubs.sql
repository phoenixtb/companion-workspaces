-- Part 3, drill 2: ask the connector for an incremental snapshot of hubs.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/03-backfill-hubs.sql
INSERT INTO debezium_signal (id, type, data) VALUES ('backfill-hubs-1', 'execute-snapshot',
  '{"data-collections": ["public.hubs"], "type": "incremental"}');
