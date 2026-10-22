#!/usr/bin/env bash
# Removes a Debezium PostgreSQL connector and everything it leaves behind:
# its offsets in connect-offsets, its replication slot, and (optionally) its topics.
# Usage: part2/remove-connector.sh <connector-name> <slot-name> [topic ...]
set -euo pipefail
cd "$(dirname "$0")/.."
connector=$1 slot=$2; shift 2
psql() { docker compose exec -T postgres psql -X -q -U dispatchly -d dispatchly "$@"; }

curl -fsS -o /dev/null -X PUT "localhost:8083/connectors/$connector/stop"        # offsets can only change when STOPPED
until curl -fs "localhost:8083/connectors/$connector/status" | grep -q '"state":"STOPPED"'; do sleep 1; done
curl -fsS -X DELETE "localhost:8083/connectors/$connector/offsets"; echo
curl -fsS -X DELETE "localhost:8083/connectors/$connector"
until psql -tA -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$slot' AND active" | grep -qx 0; do sleep 1; done
psql -tA -c "SELECT 'dropped slot ' || slot_name FROM pg_replication_slots, pg_drop_replication_slot(slot_name) WHERE slot_name = '$slot'"
for topic in "$@"; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --delete --topic "$topic"
  echo "deleted topic $topic"
done
