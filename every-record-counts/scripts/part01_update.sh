#!/usr/bin/env bash
# Part 1: correct one basket line in each of the three homes and look at what the change costs.
set -euo pipefail
cd "$(dirname "$0")/.."
trino() { docker compose exec -T trino trino --output-format "${2:-ALIGNED}" --execute "$1"; }

echo "== 1. PostgreSQL"
docker compose exec -T postgres psql -U larkspur -d larkspur < sql/part01/update_postgres.sql

echo "== 2. A folder of Parquet files"
uv run python scripts/duck.py sql/part01/update_parquet.sql

echo "== 3. Iceberg, through Trino"
before=$(trino 'SELECT snapshot_id FROM lakehouse.part01."sales_line$snapshots" ORDER BY committed_at DESC LIMIT 1' TSV)
trino "$(cat sql/part01/update_iceberg.sql)"
echo "-- time travel: the line as it was in snapshot $before"
trino "SELECT amount_minor, discount_minor FROM lakehouse.part01.sales_line FOR VERSION AS OF $before
       WHERE basket_id = 'GB-0042-20260815-00009' AND line_no = 1"
