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
| DuckDB `avro` extension | ships with DuckDB 2.0.0 | Reads with `read_avro`; also **writes** with `COPY … (FORMAT avro, CODEC 'deflate')` (null, deflate, snappy, zstandard worked in the lab). Writing is documented in the extension's GitHub README, not on the 2.0 docs page (Part 3). |
| DuckDB `excel` extension | ships with DuckDB 2.0.0 | `read_xlsx` takes one file path: a glob returned rows from one workbook only, a list is a binder error. Over S3 it took about 1 s per small workbook, so Part 4 fetches workbooks first. |
| DuckDB `ASOF JOIN` | DuckDB 2.0.0 | Used for promotion versions and price history (Part 4). |
| DuckDB time zones (ICU) | DuckDB 2.0.0 | `AT TIME ZONE` and `timezone(zone, TIMESTAMP …)` with IANA names, for local business dates and DST (Part 5). `round()` rounds half away from zero on DECIMAL and DOUBLE. |
| botocore | 1.43.x | Comes with `s3fs` (via aiobotocore); Part 3 uses it directly for S3 conditional writes. SeaweedFS 4.51 honours `If-None-Match: *` and `If-Match` on PutObject (412 on failure). |
| DuckDB SQL used in Part 6 | DuckDB 2.0.0 | `QUALIFY`, `PIVOT`/`UNPIVOT`, `GROUPING SETS`/`CUBE`, `approx_count_distinct` (HyperLogLog), `MERGE INTO` (since 1.4.0; a target row matched twice is an error, unmatched duplicates insert twice). `read_json` samples 32 files by default: use `union_by_name = true` for drifting JSON. |
| Trino SQL used in Part 6 | Trino 484 | No `QUALIFY`. `PIVOT` since 483 (needs `GROUP BY` inside `PIVOT`, else one row); no `UNPIVOT`. `approx_distinct` (2.3% standard error), `approx_set`/`merge`/`cardinality`. |
| ruff | 0.17.3 | |
| pytest | 9.x | |
| Docker Desktop | 29.8 | Lab measured with a 9.8 GB VM; see the README's memory matrix. |

## Part 7 additions (verified 2026-10-28)

| Tool | Version | Notes |
|---|---|---|
| Iceberg tables | format v2 by default | `format-version` defaults to 2 (since Iceberg 1.4.0); Polaris 1.8 + Spark created v2 without asking. `TBLPROPERTIES ('format-version' = '3')` makes v3. |
| Iceberg write modes | Iceberg 1.12.0 | `write.delete.mode`, `write.update.mode`, `write.merge.mode`: `copy-on-write` (default) or `merge-on-read`. `write.*.isolation-level`: `serializable` (default) or `snapshot`. `commit.retry.num-retries` 4. |
| Spark + Iceberg v3 | Spark 4.1.3, Iceberg 1.12.0 | VARIANT (`parse_json`, `variant_get`), deletion vectors (Puffin), row lineage (`_row_id`, `_last_updated_sequence_number`) worked. Column defaults: "setting default values in Spark is currently unsupported" (apache/iceberg#16698). |
| DuckDB 2.0 + Iceberg v3 | DuckDB 2.0.0 | Reads VARIANT, applies deletion vectors, exposes `_row_id`; writes merge-on-read only. |
| Trino 484 + Iceberg v3 | Trino 484 | Format v3 "experimental": read the VARIANT column (`typeof` = `variant`, `CAST(payload AS JSON)`) and applied the deletion vector. |
| PyIceberg | 0.12.0 + `pyiceberg-core` 0.10.1 | Can't load a table with a `variant` column ("Unsupported field type"). Deletes on tables with delete files need the `pyiceberg-core` extra. Retries `CommitFailedException` automatically and raises `ValidationException` on a real conflict. |
| fastavro | 1.13.1 | Reads manifest lists and manifests directly (Part 7's metadata walk). |
| Polaris drop with purge | Polaris 1.8.0 | Refused unless `DROP_WITH_PURGE_ENABLED` (or the catalog's `polaris.config.drop-with-purge.enabled`) is set; the lab drops without purge and deletes the files itself. |
| Java / Gradle | Temurin 25.0.4, Gradle 9.8.2 (wrapper) | `iceberg-api-demo`: `iceberg-core`, `iceberg-data`, `iceberg-parquet`, `iceberg-aws` 1.12.0, `iceberg-aws-bundle` (runtime), `hadoop-client-api`/`-runtime` 3.5.0 (runtime, for parquet-hadoop), `slf4j-simple` 2.0.20. Versions in `gradle/libs.versions.toml`. |

## Part 8 additions (verified 2026-10-28)

| Tool | Version | Notes |
|---|---|---|
| Delta Lake (Spark) | 4.4.1 (`io.delta:delta-spark_4.1_2.13`, `delta-storage`) | Built for Spark 4.2.0, 4.1.0 and 4.0.1. Jars live in `/opt/lakehouse/delta-jars` in the Spark image and load only with `--properties-file $SPARK_CONF_DIR/delta.conf` (`spark/delta.conf.template`); Iceberg sessions are unchanged. |
| Delta UniForm | `io.delta:delta-iceberg_2.13:4.4.1` | "supports Spark 4.1 and is not compatible with Spark 4.2" (release notes). Bundles a shaded Iceberg (`shadedForDelta.org.apache.iceberg`), so it sits beside the Iceberg 1.12 runtime. `delta.enableIcebergCompatV2` + `delta.universalFormat.enabledFormats = iceberg` turned on column mapping (`name`) and reader/writer protocol 2/7 by themselves. Docs require a Hive Metastore; with Spark's embedded Derby metastore every conversion wrote a fresh one-snapshot Iceberg table (`00000-…metadata.json`, no `converted_delta_version` property). |
| Hadoop S3A for Delta | `hadoop-aws` 3.4.2 + AWS SDK `s3-transfer-manager` and `apache-client` 2.54.17 | Matches the image's Hadoop 3.4.2 and the SDK version inside `iceberg-aws-bundle` 1.12.0 (whose S3 client S3A reuses), instead of the ~600 MB SDK bundle. Delta writes `s3a://larkspur-lake/delta/…`. |
| Delta defaults seen | Delta 4.4.1 | A Parquet checkpoint every 10 commits (versions 10 and 20 written); `delta.enableDeletionVectors = true` made a one-row UPDATE write a 43-byte DV plus a one-row file (`numCopiedRows` 0); without DVs it copied 73,245 rows. |
| DuckDB `ducklake` extension | ships with DuckDB 2.0.0 (DuckLake spec 1.1) | `ATTACH 'ducklake:postgres:<libpq string>' AS dl (DATA_PATH 's3://…')`; catalog = 30 `ducklake_*` tables in PostgreSQL (31 after the first inlined write). Data inlining on by default (10-row limit): a one-row UPDATE added one row to `ducklake_inlined_data_<table>_<schema version>` and one to `ducklake_inlined_delete_<table>`, no file. `CALL ducklake_flush_inlined_data('dl')` wrote one data file and one delete file. |
| DuckDB `delta` extension | ships with DuckDB 2.0.0 | Reads Delta (incl. deletion vectors); `UPDATE`/`DELETE`/`MERGE` unsupported: "Binder Error: Can only update base table" on an attached table. |
| DuckDB `iceberg_scan` | DuckDB 2.0.0 | Reads UniForm's metadata file directly (paths are `s3a://…`, which DuckDB's S3 secret serves). `allow_moved_paths` could not relocate `file:` data paths (Troubleshooting). |
| SeaweedFS filer API | 4.51 | Deleting objects over S3 leaves S3A's directory markers as real directories; Delta then refuses the location (`DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION`). `reset` deletes through the filer (`DELETE /buckets/<prefix>?recursive=true`). |

## Part 9 additions (verified 2026-10-28)

| Tool | Version | Notes |
|---|---|---|
| SeaweedFS STS | 4.51 (`weed/iam/sts`, `weed/s3api/s3api_sts.go`) | `-s3.iam.config=/etc/seaweedfs/iam.json` turns on roles and the STS API on the S3 port (`AssumeRole`, `AssumeRoleWithWebIdentity`, `GetFederationToken`, …). `iam.json`: `sts` (signing key, durations), `policies`, `roles` (trust policy + attached policies). Roles match by name; the account in the ARN is ignored, but Polaris requires a 12-digit one. Static identities in `s3.json` keep working; legacy actions can be bucket-scoped (`Read:larkspur-lake`). `AssumeRole` honours inline session policies (a GET outside the policy's prefix → AccessDenied). Session tokens are JWTs carrying the session policy (`spol`). Without a config file, `weed mini` runs STS but IAM is read-only (`-s3.iam.readOnly` defaults to true), so roles can't be created at runtime. |
| Polaris credential vending | 1.8.0 | `storageConfigInfo.roleArn` + `stsEndpoint` (defaults to `endpointInternal`) + `stsUnavailable: false`. Polaris calls `AssumeRole` with a per-table session policy: read-only (`s3:GetObject`, `GetObjectVersion`, `ListBucket` on the prefix, `GetBucketLocation`) for a principal with `TABLE_READ_DATA` only; write actions when it has `TABLE_WRITE_DATA`. Returned as `s3.access-key-id`/`s3.secret-access-key`/`s3.session-token` + `client.refresh-credentials-endpoint` in `loadTable`'s config. Vended keys are cached (`STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS`, 30 min). Adding a role ARN to an existing catalog fails ("Cannot modify AWS account ID…") unless `polaris.features."ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES"` is true (compose sets it for Part 9). |
| Polaris principals and grants | 1.8.0 | `POST /principals/{name}/reset` accepts a chosen `clientId` and `clientSecret` (`ENABLE_CREDENTIAL_RESET` defaults to true). Privilege names from `spec/polaris-management-service.yml` at `apache-polaris-1.8.0`. A HEAD on a namespace needs `NAMESPACE_READ_PROPERTIES`; listing needs `NAMESPACE_LIST` on the catalog. Denials name the operation: `LOAD_TABLE` (no delegation) vs `LOAD_TABLE_WITH_READ_DELEGATION`. Deleting and re-creating a principal changes its id: tokens Trino already holds then get 401 until Trino restarts. |
| Engines and vending | Iceberg 1.12 (Spark), Trino 484, PyIceberg 0.12, DuckDB 2.0.0 | Spark: `spark.sql.catalog.<n>.header.X-Iceberg-Access-Delegation=vended-credentials` (Iceberg Java does not send the header by default). Trino: `iceberg.rest-catalog.vended-credentials-enabled=true`, no `s3.aws-*` keys. PyIceberg sends the header by default. DuckDB 2.0 with `ACCESS_DELEGATION_MODE 'vended_credentials'` sent the vended keys to `s3.us-east-1.amazonaws.com` because Polaris returns no `s3.endpoint` (the catalog's `endpoint` is deliberately unset, Part 1); the lab's DuckDB keeps a static key. |
| Iceberg REST spec | `open-api/rest-catalog-open-api.yaml` at `apache-iceberg-1.12.0` | Paths for config, namespaces, tables, views, register, rename, metrics, multi-table commit, `credentials`, `sign` (remote signing), `plan`/`tasks` (server-side scan planning), `functions` (new in 1.12), `read-restrictions` on loadTable (new in 1.12: row filter + column projections a reader must apply or fail), `Idempotency-Key`, `referenced-by`; `/v1/oauth/tokens` deprecated for removal. |
