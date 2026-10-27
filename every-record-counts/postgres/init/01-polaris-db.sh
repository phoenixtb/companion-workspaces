#!/usr/bin/env bash
# Polaris keeps its catalog state in its own database and role.
# The OLTP lab database (larkspur) is created by POSTGRES_DB.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE polaris LOGIN PASSWORD '${POLARIS_DB_PASSWORD}';
CREATE DATABASE polaris OWNER polaris;
\\connect polaris
-- Polaris runs `SET search_path TO POLARIS_SCHEMA` but does not create it.
CREATE SCHEMA polaris_schema AUTHORIZATION polaris;
SQL
