# Pinned versions

Verified on 2026-10-28 against project release pages, Maven Central, PyPI and Docker Hub, and by
running the stack on a MacBook Pro M3 Pro (arm64). Image tags live in `.env.example`; Python versions
in `pyproject.toml` and `uv.lock`. Re-verified at the start of every season.

## Containers (Season 1: `core` and `batch` profiles)

| Component | Version | Image | Notes |
|---|---|---|---|
| SeaweedFS | 4.51 | `chrislusf/seaweedfs:4.51` | S3 API on 8333, `weed mini` single container. Its built-in Iceberg catalog is switched off; Polaris is the catalog. |
| PostgreSQL | 18.6 + pgvector 0.8.7 | `pgvector/pgvector:0.8.7-pg18` | OLTP lab database and Polaris's metastore. |
| Apache Polaris | 1.8.0 | `apache/polaris:1.8.0`, `apache/polaris-admin-tool:1.8.0` | Relational JDBC persistence on Postgres. `stsUnavailable: true` (no credential vending; each engine has its own S3 keys). |
| Trino | 484 | `trinodb/trino:484` | Java 25 inside the image. Iceberg tables default to format v2. |
| Apache Spark | 4.1.3 | `apache/spark:4.1.3-scala2.13-java21-python3-ubuntu` + Iceberg jars | 4.1.4 and 4.2.1 were released on 2026-10-27, but there are no images yet, and there is no Iceberg runtime for Spark 4.2. |
| Apache Iceberg | 1.12.0 | `iceberg-spark-runtime-4.1_2.13`, `iceberg-aws-bundle` | Tables are created as format v2 by default (Part 7 explains why). |
| curl (setup jobs) | 8.17.0 | `curlimages/curl:8.17.0` | Creates the Polaris catalog. |

## Host tools

| Tool | Version | Notes |
|---|---|---|
| Python | 3.13.17 | via `uv` (`.python-version`) |
| uv | 0.13.x | |
| DuckDB | 2.0.0 LTS | 2.0.1 is scheduled for 2026-11-18. Database files written by 2.0 can't be opened by 1.x. |
| Polars | 2.1.0 | Row order after `group_by`/`unique`/joins isn't guaranteed without `maintain_order=True`. |
| PyIceberg | 0.12.0 | |
| PyArrow | 26.0.0 | |
| ruff | 0.17.3 | |
| pytest | 9.x | |
| Docker Desktop | 29.8 | Lab measured with a 9.8 GB VM; see the README's memory matrix. |
