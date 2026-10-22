# The Log Never Lies: Change Data Capture with Debezium — Companion Code

Runnable code for the blog series **"The Log Never Lies: Change Data Capture with Debezium"**.

Every command, SQL statement, connector configuration and Java class in the posts lives here and has been run.

## Prerequisites

| Tool | Version |
| --- | --- |
| JDK | 25 (Gradle downloads one through the foojay resolver if none is installed) |
| Gradle | Use the wrapper (`./gradlew`, Gradle 9.8.2) |
| Docker | Docker Desktop or Engine with Compose v2 |
| `curl`, `jq` | For Kafka Connect's REST API |

`psql` is optional: every SQL step runs it inside the Postgres container.

## The stack

| Service | Image | Port |
| --- | --- | --- |
| PostgreSQL (`wal_level=logical`) | `postgres:18.6` | 5432 |
| Kafka, single node, KRaft | `apache/kafka:4.3.2` | 9092 (host), 29092 (compose network) |
| Kafka Connect with Debezium 3.7 | `quay.io/debezium/connect:3.7.0.Final` | 8083 |

```bash
docker compose up -d --wait postgres   # Part 1
docker compose up -d --wait            # Parts 2 and 3
docker compose down -v                 # start over: removes the database volume too
```

All library versions are pinned once in [`gradle/libs.versions.toml`](gradle/libs.versions.toml).

## Parts

| Part | Post | Code |
| --- | --- | --- |
| 1 | [The Dual-Write Trap](https://pub.titasbiswas.com/series/cdc-with-debezium/cdc-with-debezium-part-1-the-dual-write-trap/) | `part1/` (SQL steps, preview script), `naive-poller/` (tag `cdc-with-debezium/part-1`) |
| 2 | [Plugging Into the WAL](https://pub.titasbiswas.com/series/cdc-with-debezium/cdc-with-debezium-part-2-plugging-into-the-wal/) | `connectors/`, `part2/`, `shipment-service/`, `tracking-projection/` (tag `cdc-with-debezium/part-2`) |

### Part 1

```bash
docker compose up -d --wait postgres
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/01-first-slot.sql
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/02-replica-identity.sql
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part1/03-slot-pins-wal.sql
./gradlew :naive-poller:run        # an updated_at poller against a replication slot
./gradlew :naive-poller:test       # the same, asserted against a Testcontainers PostgreSQL
docker compose up -d --wait && ./part1/preview-event.sh   # one real Debezium change event
```

Run the SQL steps in order: step 1 creates the `part1_demo` slot that steps 2 and 3 reuse, and step 3 drops it.

Asserted by tests: the poller-versus-slot results (`PollerVsLogTest`, against a Testcontainers `postgres:18.6`). The psql transcripts in the post are captured from real runs; LSNs and transaction ids match on a fresh volume, timestamps don't.

### Part 2

```bash
docker compose up -d --wait
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part2/01-debezium-user.sql
curl -s -X PUT -H 'Content-Type: application/json' \
  localhost:8083/connectors/shipments-cdc/config -d @connectors/shipments-cdc.json
./gradlew :shipment-service:bootRun        # port 8080
./gradlew :tracking-projection:bootRun     # writes tracking_view in the tracking database
./gradlew :tracking-projection:test        # the whole pipeline in Testcontainers
```

Helpers: `part2/diagnose.sql` (first queries when nothing arrives), `part2/events-for-key.sh <id>` (every event for one shipment), `part2/remove-connector.sh <name> <slot> [topics…]` (removes a connector with its offsets, slot and topics).

Asserted by tests: `ShipmentPipelineTest` registers `connectors/shipments-cdc.json` against PostgreSQL, Kafka and Debezium containers and checks that snapshot rows, a status change and a delete reach `tracking_view`.
