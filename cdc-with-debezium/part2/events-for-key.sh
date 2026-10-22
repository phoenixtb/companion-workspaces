#!/usr/bin/env bash
# Prints every change event on dispatchly.public.shipments for one shipment id, one line each:
# Kafka offset, op, status, version and the WAL position (source.lsn) it came from.
# Usage: part2/events-for-key.sh <shipment-id>
#        ISOLATION=read_committed part2/events-for-key.sh <shipment-id>   # skip aborted transactions (Part 3)
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic dispatchly.public.shipments --from-beginning --timeout-ms 8000 \
  --formatter-property print.key=true --formatter-property print.offset=true \
  --command-property "isolation.level=${ISOLATION:-read_uncommitted}" 2>/dev/null \
  | awk -F'\t' -v key="{\"id\":$1}" '$2 == key && $3 != "null" { print $1 "\t" $3 }' \
  | while IFS=$'\t' read -r offset value; do
      jq -r --arg o "$offset" '"\($o)  op=\(.op)  \(.after.status // "-")  v\(.after.version // "-")  lsn=\(.source.lsn)"' <<< "$value"
    done
