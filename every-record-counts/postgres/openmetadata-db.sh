#!/usr/bin/env bash
# Part 22: OpenMetadata's database in the lab PostgreSQL (18; OpenMetadata 2.0 asks for 15 or later), in its own
# database owned by its own role, like Airflow's (airflow-db.sh). Idempotent.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -h postgres -U larkspur -d larkspur <<SQL
SELECT 'CREATE ROLE openmetadata LOGIN PASSWORD ''${OPENMETADATA_DB_PASSWORD}'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata')\gexec
SELECT 'CREATE DATABASE openmetadata_db OWNER openmetadata'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_db')\gexec
SQL
echo "database openmetadata_db, role openmetadata: ready"
