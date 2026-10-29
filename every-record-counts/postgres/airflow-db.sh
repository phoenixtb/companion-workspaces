#!/usr/bin/env bash
# Part 19: Airflow's metadata database lives in the lab PostgreSQL, in its own database owned by its own role,
# as Polaris's does (init/01-polaris-db.sh). Idempotent, so it also works on a data volume created before Part 19.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -h postgres -U larkspur -d larkspur <<SQL
SELECT 'CREATE ROLE airflow LOGIN PASSWORD ''${AIRFLOW_DB_PASSWORD}'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow')\gexec
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SQL
echo "database airflow, role airflow: ready"
