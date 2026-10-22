-- Tables in the public schema without a primary key: check these before any FOR ALL TABLES publication.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/pk-less-tables.sql
SELECT relname FROM pg_class c WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary);
