-- Part 3, drill 4a: an expand-style change. A nullable column is backward compatible.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/04-add-column.sql
ALTER TABLE shipments ADD COLUMN customer_phone text;
