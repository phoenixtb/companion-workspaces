# Every Record Counts — companion lab. `make help` lists the targets.
COMPOSE := docker compose
CORE := --profile core
BATCH := --profile core --profile batch
ALL := --profile core --profile batch --profile olap --profile stream
STREAM := --profile core --profile stream

.PHONY: dbt-ci help env up-core up-batch up-trino up-spark up-olap up-stream stop-batch stop-olap stop-stream down reset data upload sellers-api dbt-install dbt-oss-install dbt-lake test lint ci

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

lint:  ## ruff lint and format check
	uv run ruff check .
	uv run ruff format --check .

test:  ## run the test suite (stack tests skip themselves if the stack is down)
	uv run pytest -q

ci: lint test  ## what a CI server would run; this repo runs it locally
