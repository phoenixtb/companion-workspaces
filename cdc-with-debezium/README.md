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
| Second Connect worker (Part 3, profile `ha`) | `quay.io/debezium/connect:3.7.0.Final` | 8084 |
| Apicurio Registry (Part 3) | `apicurio/apicurio-registry:3.3.3` | 8081 |

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
| 3 | [Production Without Prayers](https://pub.titasbiswas.com/series/cdc-with-debezium/cdc-with-debezium-part-3-production-without-prayers/) | `part3/`, `connectors/outbox.json`, `connectors/shipments-avro.json`, `notification-service/` (tag `cdc-with-debezium/part-3`) |

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

Later parts change files that earlier posts show (the connector JSON, `ShipmentService`, `compose.yaml`). To match a post exactly, check out its tag, for example `git checkout cdc-with-debezium/part-2`.

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

### Part 3

```bash
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/01-outbox.sql
curl -s -X PUT -H 'Content-Type: application/json' localhost:8083/connectors/outbox/config -d @connectors/outbox.json
./gradlew :notification-service:bootRun
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/02-hubs-and-signals.sql
curl -s -X PUT -H 'Content-Type: application/json' \
  localhost:8083/connectors/shipments-cdc/config -d @connectors/shipments-cdc.json
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/slot-report.sql
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/03-backfill-hubs.sql
docker compose exec -T postgres createdb -U dispatchly hub_archive
docker compose exec -T postgres psql -U dispatchly -d hub_archive < part3/wal-noise.sql      # drill 3
curl -s -X PUT -H 'Content-Type: application/json' \
  localhost:8083/connectors/shipments-avro/config -d @connectors/shipments-avro.json         # drill 4
curl -s -X POST localhost:8081/apis/registry/v3/admin/rules -H 'Content-Type: application/json' \
  -d '{"ruleType":"COMPATIBILITY","config":"BACKWARD"}'
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/04-add-column.sql
docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/05-rename-column.sql
part2/remove-connector.sh shipments-avro dispatchly_shipments_avro avro.public.shipments    # end of drill 4
docker compose --profile ha up -d --wait connect-2        # drill 5
```

The repository holds the **end-of-day** state of the game day. To replay a drill as the post tells it, start from the state before it:

| Drill | Remove or revert before running it |
| --- | --- |
| 1 (outbox kill drill) | `CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT` in `compose.yaml` (EOS removes the replay this drill shows) |
| 3 (quiet database) | `heartbeat.action.query` in `connectors/shipments-cdc.json`; `lsn.flush.mode` and `offset.mismatch.strategy` in `connectors/outbox.json` |
| 3 (slot cap) | start Postgres with `PG_MAX_SLOT_WAL_KEEP_SIZE=64MB docker compose up -d --wait postgres` |
| 4 (schemas) | nothing extra; undo the rename afterwards with `ALTER TABLE shipments RENAME COLUMN current_hub_id TO hub_id;` and remove the Avro connector |
| 5 (workers) | the Avro connector removed, so two connectors are running |
| 6 (exactly-once) | nothing: this is where EOS is switched on |
| 8 (secrets, masking) | `database.password` back to a literal; `column.mask.hash.v2…` removed |

Asserted by tests: `OutboxPipelineTest` (an outbox row reaches `notification-service` through `connectors/outbox.json`, and a redelivered event is handled once), `InboxTest`, and `ShipmentPipelineTest` with Part 3's connector settings (exactly-once on the worker, the password from an environment variable, masking).
