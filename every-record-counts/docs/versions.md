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

## Part 10 additions (verified 2026-10-28)

| Tool | Version | Notes |
|---|---|---|
| Iceberg Spark procedures | 1.12.0 (`docs/docs/spark-procedures.md` at tag `apache-iceberg-1.12.0`) | `rewrite_data_files(table, strategy binpack\|sort, sort_order incl. zorder(...)/hilbert(...), options, where)`; options used: `rewrite-all`, `remove-dangling-deletes` (output `removed_delete_files_count`). `expire_snapshots(table, older_than default 5 days, retain_last default 1, …, clean_expired_metadata)`. `remove_orphan_files(table, older_than default 3 days, location, dry_run, …, equal_schemes, prefix_mismatch_mode, prefix_listing)`. `rewrite_manifests(table, use_caching, spec_id, sort_by)`. `rewrite_position_delete_files(table, options, where)` (documented, not run). |
| `remove_orphan_files` on this stack | Iceberg 1.12.0, Spark 4.1.3 | Without `prefix_listing => true`: `UnsupportedFileSystemException: No FileSystem for scheme "s3"` (the image has no Hadoop S3 file system for `s3://`; `S3FileIO` supports prefix listing). The procedure refuses `older_than` less than 24 hours ago (`RemoveOrphanFilesProcedure`), even for a dry run; the Spark action `SparkActions.get(spark).deleteOrphanFiles(table).olderThan(ms).usePrefixListing(true)` takes any cutoff (`spark/jobs/part10_orphans.py`, through Py4J). Procedure arguments must be literals or `current_timestamp()`: `current_timestamp() - INTERVAL 1 MINUTE` failed binding. |
| Iceberg table properties | 1.12.0 (`configuration.md`) | `write.target-file-size-bytes` 512 MiB, `write.parquet.row-group-size-bytes` 128 MiB, `write.spark.fanout.enabled` false (but Spark writes with hash distribution used `FanoutDataWriter`: 1,511 day partitions over 8 tasks ran out of a 1.5 GB heap; setting it to `false` explicitly made Spark sort by partition), `write.metadata.delete-after-commit.enabled` false, `write.metadata.previous-versions-max` 100, `history.expire.max-snapshot-age-ms` 5 days, `history.expire.min-snapshots-to-keep` 1. Part 10 scales file and row-group targets by 1/64 (8 MiB / 2 MiB) to mimic full-volume file counts. |
| Spark scan metrics | Spark 4.1.3 UI REST API (`/api/v1/applications/<app>/sql?details=true`) | The Iceberg `BatchScan` node reports `number of result data files`, `number of skipped data files`, `number of result delete files`, `total data file size (bytes)`, `number of output rows` (after row-group skipping), `total planning duration (ms)`. |
| Local-mode task retries | Spark 4.1.3 | `local[*]` gives a task one attempt. Repeated scans of the unpartitioned table once failed with `ParquetDecodingException: Failed to read dictionary … Socket is closed by peer` (SeaweedFS logged `connection reset by peer`; cause not found). Part 10's scripts submit with `--master local[*,3]`. |

## Part 11 additions (verified 2026-10-28)

| Tool | Version | Notes |
|---|---|---|
| Polars Iceberg planner | `polars[iceberg]` 2.1.0 → `polars-iceberg` 0.1.0 | The extra installs the native scan planner; without it Polars warns (`PerformanceWarning`) and plans with PyIceberg (`POLARS_ICEBERG_PLANNER=pyiceberg` silences it). `scan_iceberg(..., storage_options=…)` takes PyIceberg FileIO keys (`s3.endpoint`, `s3.access-key-id`, …) and translates them for the native reader; it does **not** reuse the catalog's FileIO settings (without them: `Error performing PUT http://169.254.169.254/latest/api/token`). `sink_iceberg`/`write_iceberg` are marked unstable. |
| Polars streaming engine | 2.1.0 | Default for `collect()` since 2.0.0. `group_by` without `maintain_order=True` returned different first groups on every run (Part 11). Out-of-core on by default; `POLARS_OOC_MEMORY_BUDGET_MB` (default 80% of RAM via `POLARS_OOC_MEMORY_BUDGET_FRACTION`), `POLARS_OOC_DISK_BUDGET_MB` (64,000), `POLARS_MAX_THREADS`: in `crates/polars-config`, not in the user guide. A 1,500 MB budget cut the basket query's peak RSS from 8.9 to 2.3 GiB. |
| DuckDB memory and threads | 2.0.0 | Defaults on a 36 GB, 12-core Mac: `threads` 12, `memory_limit` 28.7 GiB (80%), `temp_directory` `.tmp`. With `memory_limit = '1500MB'` the basket query spilled 2.2 GiB (`peak_temp_dir_size` from `enable_profiling = 'json'` + `profiling_mode = 'detailed'`; set `enable_profiling` before `profiling_output`, or a `.json` output path is refused) and peaked at 1.8 GiB RSS. `custom_profiling_settings` doesn't exist in 2.0.0. |
| DuckDB Iceberg writes | 2.0.0 | `CREATE TABLE lake.ns.t AS …` through Polaris works. `sum(BIGINT)` is `HUGEINT`, written to Iceberg as `DECIMAL(38,0)`: cast to `BIGINT` first. Merge-on-read only (docs). |
| DuckDB storage versions | 2.0.0 vs 1.5.6 | A new 2.0 file: 1.5.6 says "Trying to read a database file with version number 69, but we can only read versions between 64 and 68". `ATTACH '…' (STORAGE_VERSION 'v1.5.0')` writes a file 1.5.6 opens. |
| DuckDB file locks | 2.0.0 | While one process has a file open read-write, another can't open it, even with `read_only=True` ("Could not set lock on file"). |
| DuckDB ↔ Polars | DuckDB 2.0.0, Polars 2.1.0 | `rel.to_arrow_table()`, `pl.from_arrow()`, `con.register(name, polars_df)`. Replacement scans find a frame only by variable name in the calling frame (not inside a lambda). |
| Apache DataFusion | 55.2.0 (crate), 55.0.0 (PyPI) | Not run in the lab; named in Part 11. |

## Part 12 additions (verified 2026-10-28)

| Tool | Version | Notes |
|---|---|---|
| Spark 4.2 / 4.1.4 | not used | Re-checked: still no `iceberg-spark-runtime-4.2_2.13` on Maven Central (404) and no `apache/spark` images for 4.1.4 or 4.2.1 on Docker Hub. The lab stays on 4.1.3. |
| Spark AQE and join settings | Spark 4.1.3 (`sql-performance-tuning.html` 4.1.0, `SQLConf.scala` at v4.1.3) | Defaults: `spark.sql.adaptive.enabled` true, `coalescePartitions.enabled` true (`parallelismFirst` true), `advisoryPartitionSizeInBytes` 64MB, `skewJoin.enabled` true, `skewedPartitionFactor` 5.0, `skewedPartitionThresholdInBytes` 256MB, `autoBroadcastJoinThreshold` 10 MB, `shuffle.partitions` 200 (the lab's `spark-defaults` sets 8). `spark.sql.join.preferSortMergeJoin` is internal (true). Part 12's variants set 200 partitions with coalescing off (one task per partition) and scale the skew thresholds by 1/64 (4MB / 1MB) for `aqe_scaled`. |
| AQE skew split (observed) | Spark 4.1.3 | A 37.9 MiB hot partition (about 75× the other partitions' 0.5 MiB average) was not split with the 256MB default. With 4MB it became 12 pieces, one per map task (`ShufflePartitionsUtil.createSkewPartitionSpecs` splits along map outputs): 200 tasks → 211, plan `SortMergeJoin(skew=true)`, `AQEShuffleRead … Arguments: skewed`. Full outer joins can't be split (`OptimizeSkewedJoin.canSplitLeftSide/RightSide`). |
| Spark UI REST API | Spark 4.1.3 | `/api/v1/applications/<app>/sql?details=true&planDescription=true` (final plan text incl. AQE nodes), `jobs/<id>` (`stageIds`), `stages/<id>` (`shuffleReadBytes`, `shuffleWriteBytes`, `memoryBytesSpilled`, `diskBytesSpilled`), `stages/<id>/<attempt>/taskList` (per task `executorRunTime`, `shuffleReadMetrics`), `executors` (`maxMemory`: 741.6 MiB = (1,536 − 300) × 0.6 for the 1.5 GB driver). |
| Spill (observed) | Spark 4.1.3 | The hot sort-merge task spilled 320–416 MiB (memory form) / 27–34 MiB (disk) in three runs of the same table: how much a task may hold depends on how many tasks share execution memory at that moment (1/2N–1/N). |
| ANSI mode | Spark 4.1.3 | `spark.sql.ansi.enabled` true by default (since 4.0): `CAST_INVALID_INPUT`, `ARITHMETIC_OVERFLOW` (`long overflow`); `try_cast`/`try_add` return NULL; with ANSI off, a `BIGINT` sum wraps to −9223372036854775808. |
| PySpark 4.1 API | 4.1.3 | `pyspark.sql.functions.months` as a partition transform warns "Deprecated in 4.0.0, use partitioning.months instead." `DataFrameWriterV2.overwritePartitions()` replaces whole partitions (dynamic overwrite). An `IN (SELECT …)` inside `LATERAL VIEW explode(...)` fails with `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY`. A filter on `date_format(business_date, …)` isn't pushed into the Iceberg scan; a range on the column is. |
| Spark Connect in the image | `apache/spark:4.1.3-…-python3-ubuntu` | The image has `spark-connect_2.13-4.1.3.jar` and `spark-pipelines_2.13-4.1.3.jar` and `bin/spark-pipelines`, but Python lacks `grpc` and `pandas`; `spark-submit --conf spark.api.mode=connect` failed with `TypeError: 'JavaPackage' object is not callable` (cause not confirmed). Connect and SDP are described, not run. |
| SeaweedFS reads from Spark | 4.51 | Another intermittent short read: `EOFException: Reached the end of stream with 628 bytes left to read` (Part 10 saw "Socket is closed by peer"). Part 12 submits with `--master local[*,3]`. |
