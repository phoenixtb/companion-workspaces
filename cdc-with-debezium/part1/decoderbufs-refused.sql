-- Debezium's default plugin.name, requested by hand on a stock PostgreSQL 18.6.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/decoderbufs-refused.sql
SELECT pg_create_logical_replication_slot('try_decoderbufs', 'decoderbufs');
