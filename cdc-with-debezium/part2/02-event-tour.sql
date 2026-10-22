-- Part 2, step 3: changes that produce interesting change events. Run the file, then read the topic.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part2/02-event-tour.sql

-- 1. A long delivery note: big enough that PostgreSQL stores it out of line (TOAST).
UPDATE shipments
SET delivery_notes = (SELECT string_agg(md5(g::text), ' ') FROM generate_series(1, 300) g),
    version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1003';

-- ...then a change that doesn't touch the note.
UPDATE shipments SET status = 'AT_HUB', version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1003';

-- 2. The same kind of change with REPLICA IDENTITY FULL: the old row comes along.
ALTER TABLE shipments REPLICA IDENTITY FULL;
UPDATE shipments SET hub_id = 'BLR-03', version = version + 1, updated_at = now()
WHERE tracking_code = 'DSP-1001';
ALTER TABLE shipments REPLICA IDENTITY DEFAULT;

-- 3. Changing a primary key. Never do this to a captured table on purpose; know what it looks like.
UPDATE shipments SET id = 104 WHERE id = 4;
