#!/usr/bin/env bash
# Render spark-defaults.conf from the environment, then stay up so jobs can be
# run with `docker compose exec spark spark-sql ...` or spark-submit.
set -euo pipefail
mkdir -p "${SPARK_CONF_DIR}"
for conf in spark-defaults delta; do   # delta.conf: Part 8's Delta Lake sessions (--properties-file)
  sed -e "s|__POLARIS_CLIENT_ID__|${POLARIS_CLIENT_ID}|" \
      -e "s|__POLARIS_CLIENT_SECRET__|${POLARIS_CLIENT_SECRET}|" \
      -e "s|__S3_ACCESS_KEY__|${S3_ACCESS_KEY}|" \
      -e "s|__S3_SECRET_KEY__|${S3_SECRET_KEY}|" \
      "/opt/lakehouse/${conf}.conf.template" > "${SPARK_CONF_DIR}/${conf}.conf"
done
exec sleep infinity
