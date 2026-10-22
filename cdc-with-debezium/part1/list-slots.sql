-- After any experiment: every slot on the server, and whether something is reading it.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/list-slots.sql
SELECT slot_name, active FROM pg_replication_slots;
