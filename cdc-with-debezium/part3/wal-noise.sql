-- Busy traffic in a database nobody captures: about 140 MB of WAL. The table is emptied afterwards;
-- the WAL it produced stays until every slot has confirmed past it.
-- Run: docker compose exec -T postgres psql -U dispatchly -d hub_archive < part3/wal-noise.sql
CREATE TABLE IF NOT EXISTS scan_noise (id bigint, payload text);
INSERT INTO scan_noise SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
TRUNCATE scan_noise;
