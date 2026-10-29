#!/usr/bin/env bash
# Part 27: Apicurio Registry keeps its schemas in the lab PostgreSQL, in its own database owned by its own role
# (as Polaris and Airflow do). Idempotent, so it also works on a data volume created before Part 27.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -h postgres -U larkspur -d larkspur <<SQL
SELECT 'CREATE ROLE apicurio LOGIN PASSWORD ''${APICURIO_DB_PASSWORD}'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'apicurio')\gexec
SELECT 'CREATE DATABASE apicurio OWNER apicurio'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'apicurio')\gexec
SQL
echo "database apicurio, role apicurio: ready"
