#!/usr/bin/env bash
# Part 31: Superset's metadata (users, databases, datasets, charts) in the lab PostgreSQL, in its own database owned
# by its own role, as Polaris, Airflow and Apicurio do. Idempotent, so it also works on an older data volume.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -h postgres -U larkspur -d larkspur <<SQL
SELECT 'CREATE ROLE superset LOGIN PASSWORD ''${SUPERSET_DB_PASSWORD}'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset')\gexec
SELECT 'CREATE DATABASE superset OWNER superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
SQL
echo "database superset, role superset: ready"
