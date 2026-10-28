# Every Record Counts — companion lab. `make help` lists the targets.
COMPOSE := docker compose
CORE := --profile core
BATCH := --profile core --profile batch
ALL := --profile core --profile batch --profile olap --profile stream
STREAM := --profile core --profile stream

.PHONY: help env up-core up-batch up-trino up-spark up-olap up-stream stop-batch stop-olap stop-stream down reset data upload sellers-api test lint ci

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
	$(COMPOSE) $(BATCH) up -d --wait --build --no-deps trino spark

up-trino: up-core  ## start core plus Trino only (Part 1)
	$(COMPOSE) $(BATCH) up -d --wait --no-deps trino

up-spark: up-core  ## start core plus Spark only (Part 7: Spark and Trino don't fit in memory together)
	$(COMPOSE) $(BATCH) up -d --wait --build --no-deps spark

up-olap: up-core  ## start core plus ClickHouse (Part 13; stop Spark first, they don't fit together)
	@! $(COMPOSE) $(BATCH) ps --status running --services | grep -qx spark || (echo "stop Spark first: make stop-batch" && false)
	$(COMPOSE) $(ALL) up -d --wait --no-deps clickhouse

up-stream: up-core  ## start core plus Kafka and Kafka Connect with Debezium (Part 16)
	$(COMPOSE) $(STREAM) up -d --wait --no-deps kafka connect

stop-batch:  ## stop Trino and Spark, keep core running
	$(COMPOSE) $(BATCH) stop trino spark

stop-olap:  ## stop ClickHouse, keep its data volume
	$(COMPOSE) $(ALL) stop clickhouse

stop-stream:  ## stop Kafka and Connect, keep their data volume
	$(COMPOSE) $(STREAM) stop connect kafka

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

lint:  ## ruff lint and format check
	uv run ruff check .
	uv run ruff format --check .

test:  ## run the test suite (stack tests skip themselves if the stack is down)
	uv run pytest -q

ci: lint test  ## what a CI server would run; this repo runs it locally
