-- Part 3, drill 4b: a breaking change. To a schema registry, a rename is "remove a field, add a required one".
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/05-rename-column.sql
-- Undo: ALTER TABLE shipments RENAME COLUMN current_hub_id TO hub_id;
ALTER TABLE shipments RENAME COLUMN hub_id TO current_hub_id;
UPDATE shipments SET status = 'AT_HUB' WHERE tracking_code = 'DSP-1004';
