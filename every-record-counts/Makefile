# Every Record Counts — companion lab. `make help` lists the targets.
COMPOSE := docker compose
CORE := --profile core
BATCH := --profile core --profile batch
ALL := --profile core --profile batch --profile olap --profile stream --profile orchestrate --profile lineage --profile serve
STREAM := --profile core --profile stream
ORCH := --profile core --profile batch --profile orchestrate
LINEAGE := --profile core --profile lineage
SERVE := --profile core --profile batch --profile serve

.PHONY: contracts-install contract-ci dbt-ci help env up-core up-batch up-trino up-spark up-olap up-stream up-backbone iceberg-sink up-flink up-flink-lake up-realtime up-serve up-cube stop-serve stop-flink flink-sql flink-jobs up-orchestrate up-lineage airflow stop-batch stop-olap stop-stream stop-orchestrate stop-lineage down reset data upload sellers-api dbt-install dbt-oss-install dbt-lake test lint ci

help:  ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

env:  ## create .env from .env.example if it doesn't exist; add keys added since (e.g. DELTA_VERSION, Part 8)
	@test -f .env || cp .env.example .env
	@grep -o '^[A-Z0-9_]*=' .env.example | while read k; do grep -q "^$$k" .env || grep "^$$k" .env.example >> .env; done

# `up --wait` treats a finished one-shot container as a failure, so wait on the
# long-running services and run the catalog setup as its own step.
up-core: env  ## start storage, catalog and Postgres
	$(COMPOSE) $(CORE) up -d --wait seaweedfs postgres polaris
	$(COMPOSE) $(CORE) run --rm --no-deps polaris-setup

up-batch: up-core  ## start core plus Trino and Spark
	$(COMPOSE) $(BATCH) up -d --wait --build --no-deps opa trino spark

up-trino: up-core  ## start core plus Trino only (Part 1), with OPA deciding its access (Part 23)
	$(COMPOSE) $(BATCH) up -d --wait --no-deps opa trino

up-spark: up-core  ## start core plus Spark only (Part 7: Spark and Trino don't fit in memory together)
	$(COMPOSE) $(BATCH) up -d --wait --build --no-deps spark

up-olap: up-core  ## start core plus ClickHouse (Part 13; stop Spark first, they don't fit together)
	@! $(COMPOSE) $(BATCH) ps --status running --services | grep -qx spark || (echo "stop Spark first: make stop-batch" && false)
	$(COMPOSE) $(ALL) up -d --wait --no-deps clickhouse

up-stream: up-core  ## start core plus Kafka and Kafka Connect with Debezium (Part 16)
	$(COMPOSE) $(STREAM) up -d --wait --no-deps kafka connect

up-orchestrate: up-trino  ## start core, Trino and Airflow 3.3 (api-server on :8090, scheduler, dag-processor; Part 19) + OTel collector (Part 22)
	$(COMPOSE) $(ORCH) up -d --wait --build otel-collector airflow-apiserver airflow-scheduler airflow-dag-processor

iceberg-sink:  ## build the Iceberg Kafka Connect sink 1.12.0 from source into build/ (Part 27; needs JAVA_HOME = JDK 17 or 21)
	./kafka/iceberg-sink/build.sh

up-backbone: up-core iceberg-sink  ## start core plus Kafka, Apicurio Registry and Connect with the Iceberg sink (Part 27)
	$(COMPOSE) $(STREAM) run --rm --no-deps apicurio-db
	$(COMPOSE) $(STREAM) up -d --wait --no-deps kafka apicurio connect

stop-batch:  ## stop Trino, OPA and Spark, keep core running
	$(COMPOSE) $(BATCH) stop trino opa spark

stop-olap:  ## stop ClickHouse, keep its data volume
	$(COMPOSE) $(ALL) stop clickhouse

stop-stream:  ## stop Kafka, Connect, Apicurio and Flink, keep their data volume
	$(COMPOSE) $(STREAM) stop flink-taskmanager flink-jobmanager connect apicurio kafka

up-flink: env  ## start Kafka and a Flink 2.2 session cluster (JobManager + one TaskManager, UI on :8081; Part 26)
	$(COMPOSE) $(STREAM) up -d --wait --build --no-deps kafka flink-jobmanager flink-taskmanager

up-flink-lake: up-core  ## start core plus Kafka, Apicurio and Flink with the Iceberg runtime; Connect stays off (Part 28)
	$(COMPOSE) $(STREAM) run --rm --no-deps apicurio-db
	$(COMPOSE) $(STREAM) up -d --wait --build --no-deps kafka apicurio flink-jobmanager flink-taskmanager

up-realtime:  ## Part 30's reduced variant: core, Kafka, Apicurio, Flink (one TaskManager) and ClickHouse
	@! $(COMPOSE) $(ALL) ps --status running --services | grep -qxE 'spark|trino|connect' || (echo "stop Spark, Trino and Connect first: make stop-batch; docker compose --profile stream stop connect" && false)
	$(MAKE) up-flink-lake
	$(COMPOSE) $(ALL) up -d --wait --no-deps clickhouse

up-serve:  ## core, Trino + OPA, Cube (REST :4000, SQL :15432) and Superset (:8088); Part 31
	@! $(COMPOSE) $(ALL) ps --status running --services | grep -qxE 'spark|clickhouse|kafka|connect|flink-jobmanager|flink-taskmanager' || (echo "stop Spark, ClickHouse, Kafka, Connect and Flink first: docker compose --profile batch stop spark; make stop-olap stop-stream" && false)
	$(MAKE) up-trino
	$(COMPOSE) $(SERVE) run --rm --no-deps superset-db
	$(COMPOSE) $(SERVE) run --rm --no-deps --build superset-init
	$(COMPOSE) $(SERVE) up -d --wait --no-deps cube superset

up-cube:  ## core, Trino + OPA and Cube only, no Superset (Part 33: the agent's MCP server reads Cube's REST API)
	@! $(COMPOSE) $(ALL) ps --status running --services | grep -qxE 'spark|clickhouse|kafka|connect|flink-jobmanager|flink-taskmanager' || (echo "stop Spark, ClickHouse, Kafka, Connect and Flink first: docker compose --profile batch stop spark; make stop-olap stop-stream" && false)
	$(MAKE) up-trino
	$(COMPOSE) $(SERVE) up -d --wait --no-deps cube

stop-serve:  ## stop Cube and Superset, keep Superset's metadata database
	$(COMPOSE) $(SERVE) stop superset cube

stop-flink:  ## stop Flink and Kafka (Part 26)
	$(COMPOSE) $(STREAM) stop flink-taskmanager flink-jobmanager kafka

flink-sql:  ## run a Flink SQL script with the SQL client in the JobManager, e.g. make flink-sql SQL=part26/windows.sql
	$(COMPOSE) $(STREAM) exec -T flink-jobmanager ./bin/sql-client.sh -f /opt/flink/lab/sql/$(SQL)

flink-jobs:  ## build Part 29's Java job jars (versions 1 and 2, Java 17) into flink-jobs/build/libs
	./gradlew -q :flink-jobs:shadowJar -PjobVersion=1
	./gradlew -q :flink-jobs:shadowJar -PjobVersion=2

airflow:  ## an Airflow CLI command in the scheduler container, e.g. make airflow CMD="dags list" (Part 19)
	$(COMPOSE) $(ORCH) exec -T airflow-scheduler airflow $(CMD)

up-lineage: up-core  ## start core plus OpenMetadata (Part 22; stop Trino and Airflow first, they don't fit together)
	@! $(COMPOSE) $(ORCH) ps --status running --services | grep -qxE 'trino|airflow-scheduler|spark' || (echo "stop Trino, Spark and Airflow first: make stop-orchestrate stop-batch" && false)
	$(COMPOSE) $(LINEAGE) up -d --wait openmetadata-search openmetadata

stop-lineage:  ## stop OpenMetadata, keep its search index and database
	$(COMPOSE) $(LINEAGE) stop openmetadata openmetadata-search

stop-orchestrate:  ## stop Airflow, keep its database and logs
	$(COMPOSE) $(ORCH) stop airflow-apiserver airflow-scheduler airflow-dag-processor otel-collector

down:  ## stop everything, keep data volumes
	$(COMPOSE) $(ALL) down

reset:  ## stop everything and delete all data volumes
	$(COMPOSE) $(ALL) down -v

data:  ## generate the raw zone into data/raw and refresh docs/injected-defects.md
	uv run python -m datagen --docs docs/injected-defects.md

upload:  ## copy data/raw into s3://larkspur-raw/raw/
	uv run python scripts/upload_raw.py

sellers-api:  ## run the mock marketplace sellers API on localhost:8765 (Part 15; Ctrl-C stops it)
	uv run python -m scripts.part15_sellers_api

DBT_OSS_VERSION := 2.0.8

dbt-install:  ## dbt-core 1.12.6 + dbt-trino 1.11.0 + dbt-duckdb 1.11.1 into .venv, and the project's packages (Part 17)
	uv sync --extra dbt
	cd dbt/larkspur && DBT_PROFILES_DIR=. ../../.venv/bin/dbt deps
	cd dbt/larkspur_subset && DBT_PROFILES_DIR=. ../../.venv/bin/dbt deps

dbt-oss-install:  ## dbt-oss (dbt v2, Apache 2.0) into its own .venv-dbt2: it also installs a `dbt` command (Part 17)
	uv venv --allow-existing --python 3.13 .venv-dbt2
	uv pip install --python .venv-dbt2/bin/python dbt-oss==$(DBT_OSS_VERSION)

dbt-lake: dbt-install  ## Part 17 on the lake: grants, landing, 126 snapshot mornings, dbt build, the check (make up-trino)
	uv run python -m scripts.part17_dbt setup
	uv run python -m scripts.part17_dbt land
	uv run python -m scripts.part17_dbt replay
	uv run python -m scripts.part17_dbt dbt build
	uv run python -m scripts.part17_dbt check

# Slim CI (Part 18): build only what a change touches, and everything downstream of it, in the `dev` namespace;
# unselected parents come from production's tables (--defer, --favor-state), as recorded in production's manifest
# (`scripts.part18_dbt prod-state`). Microbatch models build three days, not all of history.
DBT := cd dbt/larkspur && DBT_PROFILES_DIR=. DBT_SEND_ANONYMOUS_USAGE_STATS=false ../../.venv/bin/dbt
dbt-ci:  ## slim CI: dbt build of state:modified+ in dev, deferring to production (Part 18)
	$(DBT) build --target dev --select state:modified+ --state state/prod --defer --favor-state \
	  --event-time-start 2026-10-02 --event-time-end 2026-10-05

DATACONTRACT_VERSION := 1.2.8

contracts-install:  ## datacontract-cli into its own .venv-contracts: its DuckDB extra pins DuckDB 1.5 (Part 20)
	uv venv --allow-existing --python 3.13 .venv-contracts
	uv pip install --python .venv-contracts/bin/python "datacontract-cli[iceberg,s3,trino]==$(DATACONTRACT_VERSION)"

# Contract CI (Part 20): lint every contract in contracts/ and compare the working tree with its version at
# CONTRACT_BASE; a breaking change fails unless it comes as a new major version. The default, auto, is the merge base
# with origin/main (what a pull request changes, committed or not), or HEAD without an origin/main.
# ARGS="--base OLD --head NEW" compares one pair.
CONTRACT_BASE ?= auto
contract-ci:  ## refuse breaking data-contract changes (Part 20)
	uv run python -m scripts.part20_contracts ci --ref $(CONTRACT_BASE) $(ARGS)

lint:  ## ruff lint and format check
	uv run ruff check .
	uv run ruff format --check .

test:  ## run the test suite (stack tests skip themselves if the stack is down)
	uv run pytest -q

ci: lint test  ## what a CI server would run; this repo runs it locally
