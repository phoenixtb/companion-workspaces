#!/usr/bin/env bash
# Part 1 preview: one real Debezium change event, so the envelope isn't abstract.
# Part 2 builds the real pipeline (with a least-privilege user); this throwaway connector
# connects as the database owner and cleans up after itself, slot included.
# Needs the whole stack:  docker compose up -d --wait
set -euo pipefail
cd "$(dirname "$0")/.."
psql() { docker compose exec -T postgres psql -X -q -U dispatchly -d dispatchly "$@"; }

# Start from the four seed shipments, so the update below is always PICKED_UP -> AT_HUB.
psql -c "TRUNCATE shipments RESTART IDENTITY" -c "INSERT INTO shipments (tracking_code, status, hub_id, eta) VALUES
    ('DSP-1001', 'PICKED_UP', 'BLR-07', '2026-10-23 18:00+05:30'),
    ('DSP-1002', 'CREATED',   'BLR-07', '2026-10-24 12:00+05:30'),
    ('DSP-1003', 'PICKED_UP', 'BLR-12', '2026-10-23 20:00+05:30'),
    ('DSP-1004', 'CREATED',   'BLR-12', '2026-10-24 15:00+05:30')"

curl -fsS -o /dev/null -X PUT -H 'Content-Type: application/json' \
  localhost:8083/connectors/part1-preview/config -d @part1/preview-connector.json
until curl -fs localhost:8083/connectors/part1-preview/status | grep -q '"tasks":\[{"id":0,"state":"RUNNING"'; do sleep 1; done
sleep 5   # let the initial snapshot finish

psql -c "UPDATE shipments SET status = 'AT_HUB', version = version + 1, updated_at = now()
         WHERE tracking_code = 'DSP-1003'"

# Read the topic from the start: snapshot READ events (op "r") first, then our update (op "u").
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic preview.public.shipments \
  --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true --formatter-property key.separator=' | ' 2>/dev/null || true

# Clean up everything a connector leaves behind. Deleting a connector removes neither its stored
# offsets (in the connect-offsets topic) nor its replication slot; a new connector with the same
# name would try to resume from the old offset.
curl -fsS -o /dev/null -X PUT localhost:8083/connectors/part1-preview/stop
until curl -fs localhost:8083/connectors/part1-preview/status | grep -q '"state":"STOPPED"'; do sleep 1; done
curl -fsS -o /dev/null -X DELETE localhost:8083/connectors/part1-preview/offsets
curl -fsS -X DELETE localhost:8083/connectors/part1-preview
until psql -tA -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'part1_preview' AND active" | grep -qx 0; do sleep 1; done
psql -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'part1_preview'" \
     -c "DROP PUBLICATION IF EXISTS part1_preview" > /dev/null
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 \
  --delete --topic preview.public.shipments
