# Every Record Counts — companion lab

Runnable code for **"Every Record Counts: Data Engineering for Backend Engineers"** on
[pub.titasbiswas.com](https://pub.titasbiswas.com). It builds the data platform of Larkspur, a fictional
omnichannel retailer in India, the UK and Singapore, from landed raw files to a governed lakehouse.

Every command, query and output in the posts comes from this folder. Each part's state is tagged
`every-record-counts/part-<N>`; check out a tag to see the code as that part left it.

## What you need

| Tool | Version | Used for |
| --- | --- | --- |
| Docker Desktop (or another Docker engine with Compose v2) | recent; arm64 or amd64 | the services |
| [uv](https://docs.astral.sh/uv/) | 0.13 or later | Python 3.13, the generator, DuckDB, tests |
| `make` | any | the targets below |
| JDK | 25 (Gradle downloads one if missing) | the Java modules (`./gradlew`), from Part 7 |

Python packages (DuckDB 2.0, Polars 2.1 with its Iceberg extra, PyIceberg 0.12, PyArrow 26) install into `.venv` on the first
`uv run`. Versions are pinned in `docs/versions.md`, `.env.example` and `uv.lock`.

## Quick start

```bash
make up-core      # SeaweedFS (S3), Polaris (Iceberg REST catalog), PostgreSQL 18
make data         # generate the raw zone into data/raw (about a minute, ~600 MB)
make upload       # copy it into s3://larkspur-raw/raw/
make test         # tests that need the stack skip themselves when it's down
```

Add Trino with `make up-trino`, Spark with `make up-spark`, or both with `make up-batch` (together they peaked at 5.8 GiB of containers in Part 9;
Part 7 ran them one at a time to be safe on a 9.8 GB Docker VM). Add ClickHouse with `make up-olap` (Part 13; it refuses to
start while Spark runs). Stop everything with `make down`; delete all data with
`make reset`. `make help` lists every target.

| Service | URL | Credentials (lab only) |
| --- | --- | --- |
| SeaweedFS S3 API | http://localhost:8333 | `larkspur` / `larkspur-lab-secret` |
| SeaweedFS filer UI (browse buckets) | http://localhost:8888 | none |
| Polaris (Iceberg REST catalog) | http://localhost:8181/api/catalog | client `root` / `s3cr3t`, scope `PRINCIPAL_ROLE:ALL` |
| Polaris principals (Part 9) | same | `analyst` / `analyst-lab-secret`, `pipeline` / `pipeline-lab-secret` (created by `scripts.part09_catalog setup`) |
| SeaweedFS identities (Part 9) | S3 API | `polaris` (Polaris's own key, lake bucket only), `lake-reader` (read-only lake key for DuckDB); STS role `polaris-vending` in `seaweedfs/iam.json` |
| PostgreSQL | localhost:5432, database `larkspur` | `larkspur` / `larkspur` |
| Trino (batch) | http://localhost:8080 | any user name, no password; catalog `oltp` logs in to PostgreSQL as `trino_reader` / `trino-reader-lab-secret` (Part 13) |
| ClickHouse (olap, Part 13) | http://localhost:8123 (HTTP) | `larkspur` / `larkspur-lab-secret` |
| Spark UI (batch, while a job runs) | http://localhost:4040 | none |

The credentials are also in `.env.example`; `seaweedfs/s3.json` repeats the S3 keys because SeaweedFS
reads identities from a file.

## Profiles and memory

Every service has a memory limit, and the JVMs size their heaps from it. Start only what a part needs.

| Profile | Services | Limits |
| --- | --- | --- |
| `core` | seaweedfs, postgres, polaris (+ one-shot bootstrap and catalog setup) | 1.5 GB + 1 GB + 1 GB |
| `batch` | trino, spark | 3 GB + 2.5 GB |
| `olap` | clickhouse (`make up-olap`; refuses to start while Spark runs) | 2 GB (server capped at 1.5 GiB in `clickhouse/config.d/lab.xml`) |

Measured on a MacBook Pro M3 Pro, Docker Desktop VM with 9.8 GB:

| Part | Profiles | Peak memory (containers) | Notes |
| --- | --- | --- | --- |
| 0 | core | 0.86 GB | DuckDB runs on the host |
| 1 | core + Trino (`make up-trino`) | 2.4 GB | SeaweedFS 0.69, Polaris 0.27, PostgreSQL 0.35, Trino 1.05; Spark not needed |
| 2 | none (host only) | 0.62 GiB RSS (DuckDB) | no containers; the full backfill (128 runs) peaked at 619 MiB resident on the host |
| 3 | core | 1.5 GiB | SeaweedFS 0.93 (limit raised to 1.5 GB for the 1.7 GB upload), Polaris 0.35, PostgreSQL 0.29; Trino stopped. Host: `part03_formats.py write` 6–7 GB resident, `part03_arrow.py` 3.7 GB |
| 4 | none (`build --local`); core to read the bucket | 5.7 GiB RSS (DuckDB, host) | the star is one DuckDB file (`data/lab/part04/larkspur_star.duckdb`, ~375 MiB); from the bucket: containers 1.1 GiB (SeaweedFS 0.49, Polaris 0.34, PostgreSQL 0.28), host 6.1 GiB |
| 5 | none (`build --local`) | 2.8 GiB RSS (DuckDB, host) | `scripts/part05_shapes.py build` copies Part 4's star and adds the vault, wide table, daily FX and closing stock in 16 s; the DuckDB file grows to 1.6 GiB (the vault's text hash keys and key indexes) |
| 6 | none for the DuckDB workbook; core + Trino (`make up-trino`) for the Trino steps | 4.3 GiB (containers) | SeaweedFS 0.86, Trino 2.9, Polaris 0.35, PostgreSQL 0.29. Host: `scripts.part06_sql build` 3.7 GiB resident; stop Trino afterwards |
| 7 | core + Spark (`make up-spark`), then core + Trino (`make up-trino`) with Spark stopped | 3.2 GiB with Spark; 2.3 GiB with Trino | Spark 2.0, SeaweedFS 0.58, Polaris 0.41, PostgreSQL 0.28 (sampled every ~2 s). Trino 1.06 for the Part 7 reads (2.3 during `make ci` with Trino up, which runs Part 6's Trino tests). Spark and Trino together don't fit the 9.8 GB VM next to other containers. The Java demo (`./gradlew :iceberg-api-demo:run`), PyIceberg and DuckDB run on the host |
| 8 | core + Spark (`make up-spark`) | 3.2 GiB | Spark 2.06 (Delta sessions), SeaweedFS 0.47, Polaris 0.42, PostgreSQL 0.29 (sampled every ~2 s). Trino not needed. DuckDB (Iceberg, DuckLake, Delta reads) runs on the host |
| 9 | core + Trino + Spark (`make up-trino`, `make up-spark`) | 5.8 GiB (during `make ci`) | Trino 2.34, Spark 2.04, SeaweedFS 0.83, PostgreSQL 0.68, Polaris 0.35 (sampled every ~2 s). The Part 9 steps alone stay lower; Spark and Trino steps are skipped when the engine is down. PyIceberg and DuckDB run on the host |
| 10 | core + Spark (`make up-spark`) | 3.55 GiB | Spark 2.13, SeaweedFS 0.83, Polaris 0.39, PostgreSQL 0.28 (sampled every ~2 s over the whole sequence, including the 72.6-million-row build and the Z-order rewrite). Trino not needed. Writing 1,511 day partitions needed `write.spark.fanout.enabled = false` to fit the 1.5 GB driver |
| 11 | core; core + Spark (`make up-spark`) only for the Spark baseline | 2.9 GiB with Spark; 2.1 GiB core only | Spark 2.03–2.08, SeaweedFS 1.42 (of its 1.5 GB limit, while the host engines read with 12 threads), Polaris 0.40, PostgreSQL 0.28 (sampled every few seconds). Host: DuckDB peaked at 3.8 GiB RSS and Polars at 8.9 GiB on the basket query with no limits; 1.8 and 2.3 GiB with 1,500 MB limits. Stop Spark before profiling the host engines |
| 12 | core + Spark (`make up-spark`) | 2.97 GiB | Spark 1.95 (of 2.5; 741.6 MiB unified memory in the 1.5 GB driver), Polaris 0.43, SeaweedFS 0.35, PostgreSQL 0.28 (sampled every ~2 s over pipeline ×2, ANSI demo, campaign build and the five join variants). The hot sort-merge task spills 0.3–0.4 GiB to local disk inside the container. Trino not needed |
| 13 | core + Trino + ClickHouse (`make up-trino`, `make up-olap`; Spark stopped) | 4.8 GiB | Trino 3.0 (its full 3 GB limit: the image commits 80% as heap at start), ClickHouse 0.94 (of 2; server capped at 1.5 GiB), Polaris 0.43, SeaweedFS 0.30, PostgreSQL 0.29 (sampled every ~2 s over seed, Trino, federation, ClickHouse over Iceberg, the MergeTree load, the serving view, the load test and the tests). ClickHouse spills big GROUP BYs to its volume (up to ~0.6 GiB) |

## The data

`datagen/` writes a deterministic synthetic world (seed 42) for 1 June to 4 October 2026: 600 stores,
2,000 products, POS baskets, online orders, refunds, payments, PSP settlement files, daily stock
snapshots, supplier delivery feeds and app/web clickstream. It also injects documented defects; see
[`docs/injected-defects.md`](docs/injected-defects.md) for every defect, its count and what it does to
the three headline numbers (UK net revenue, stock availability, online conversion).

```text
data/raw/
  reference/   catalog/   pricing/   promotions/   fx/            reference data and daily extracts
  pos/events/upload_date=…/        POS basket events, JSON Lines (gzip), by upload date
  oltp/orders_poll/extract_date=…/ the legacy nightly updated_at poller's extracts (Parquet)
  oltp/{orders,order_lines,payments,refunds}/snapshot_date=2026-10-05/   full OLTP extracts
  inventory/snapshots/business_date=…/   inventory/adjustments/…
  procurement/purchase_orders/     suppliers/deliveries/<supplier>/      CSV and XLSX feeds
  clickstream/events/event_date=…/ app and web events, JSON Lines (gzip)
  psp/settlements/country=…/       the payment provider's daily settlement CSVs
```

`uv run python -m datagen --scale 0.2 --store-fraction 0.2 --out /tmp/raw` makes a smaller world.

## Parts

| Part | Title | What's here |
| --- | --- | --- |
| 0 | The Field Guide | the stack, the generator, `sql/incident/` and `sql/part00/`: the three headline numbers, two answers each |
| 1 | Three Houses for Data | `sql/part01/`, `scripts/part01_load.py`, `part01_compare.py`, `part01_update.sh`: one table in PostgreSQL, a Parquet folder and an Iceberg table; the same questions and one update in each |
| 2 | Think in Pipelines | `sql/part02/`, `scripts/part02_pipeline.py`, `part02_profile.py`: a daily sales job that appends (and doubles on re-run) vs one that rebuilds the business dates each upload touched, raw → silver → gold in DuckDB; `tests/test_part02.py` proves re-runs, reverse backfills and crashes leave the tables identical |
| 3 | Bytes on Disk | `sql/part03/`, `scripts/part03_formats.py` (`write`, `compare`, `skip`), `part03_footer.py`, `part03_arrow.py`, `part03_object_store.py`: one table as CSV, JSON Lines, Avro and Parquet (codecs, sort orders, row-group sizes, 600 small files); bytes and GETs fetched from SeaweedFS; a Parquet footer read by hand; Arrow hand-off; S3 conditional writes |
| 4 | Grain Before Glory | `sql/part04/`, `scripts/part04_star.py` (`build`, `query`): a star schema in DuckDB with transaction, periodic snapshot and accumulating snapshot facts, Type 2/Type 6 product and promotion dimensions, placeholders for early-arriving facts, an audit dimension; queries for the discount grain bug, refund dating (revenue step 3), semi-additive stock, as-sold vs restated categories and as-of promotions and prices; `tests/test_part04.py` |
| 5 | Other Shapes | `sql/part05/`, `scripts/part05_shapes.py` (`build`, `query`; run with `python -m`): a Data Vault (hubs, links, satellites, insert-only, loaded as filed and again) and a wide table for the same refunds; FX rates densified per day; UK revenue waterfall steps 4 (FX as of the transaction date) and 5 (London business dates) landing on the correct figure; stock counted at close of day; money and DST primitives; a report reproduced as filed; `tests/test_part05.py` |
| 6 | SQL You Will Actually Use | `sql/part06/`, `scripts/part06_sql.py` (`build`, `query`; run with `python -m`), `scripts/part06_trino.py` (`load`, `query`): a DuckDB workbook over the raw zone and Part 5's star: re-sent POS events found by `basket_id` (revenue step 1), the clickstream sessionized two ways with the conversion gap taken apart, a funnel with `UNPIVOT`/`PIVOT`, window frames and ties, gaps and islands on stock-outs, `CUBE`, HyperLogLog vs exact, `MERGE` semantics, high-water marks, `EXPLAIN ANALYZE`; the same in Trino over an Iceberg copy of the clickstream; `tests/test_part06.py` |
| 7 | The Table That Remembers | `sql/part07/` (`stage.sql` for DuckDB, `spark/*.sql`, `trino/reads.sql`), `spark/jobs/run_sql.py`, `scripts/part07_iceberg.py` (`reset`, `spark`, `tree`, `conflict`, `engines`; run with `python -m`), `iceberg-api-demo/` (Java 25, Gradle): Iceberg tables created through Polaris by Spark; the metadata tree read as plain JSON and Avro files; schema and partition evolution without rewrites; late POS lines merged; time travel by tag; a branch; one MERGE under copy-on-write and merge-on-read; a format v3 table with VARIANT, a deletion vector and row lineage, read by Spark, DuckDB and Trino; a PyIceberg vs Spark conflict; commits, retries and a failed validation through the Java API; `tests/test_part07.py` |
| 8 | Format Wars, Ceasefire | `sql/part08/` (`stage.sql`, `ducklake_attach.sql`, `late_basket.sql`, `spark/01_delta.sql`), `spark/jobs/part08_late_baskets.py`, `spark/delta.conf.template` (Delta Lake 4.4.1 + S3A sessions), `scripts/part08_formats.py` (`reset`, `load`, `layout`, `late`, `flush`, `interop`; run with `python -m`): one gold table as Iceberg (DuckDB through Polaris), DuckLake 1.1 (catalog in PostgreSQL, files in SeaweedFS) and two Delta tables (Spark: deletion vectors; UniForm); where each keeps its metadata; what one and twenty one-row UPDATEs write (DuckLake data inlining, Delta checkpoints, copy-on-write under UniForm); every table read back by DuckDB, UniForm through `iceberg_scan`; `tests/test_part08.py` |
| 9 | Who Owns the Truth | `scripts/part09_catalog.py` (`reset`, `setup`, `load`, `engines`, `access`, `pointer`, `load-table`, `duckdb-vended`; run with `python -m`), `sql/part09/attach.sql`, `trino/catalog/lakehouse_analyst.properties`, `seaweedfs/iam.json`: credential vending switched on in Polaris through SeaweedFS's STS; `raw`/`silver`/`gold` namespaces, `analyst` and `pipeline` principals, principal and catalog roles and grants, all over HTTP; the pipeline writes with vended keys; Spark, Trino, DuckDB and PyIceberg read one gold table as the analyst; the analyst refused on raw by the catalog in every engine, refused writes by storage, and a static bucket key going around the catalog; the catalog's row vs the bucket's objects; `tests/test_part09.py` |
| 10 | Where the Bytes Live | `sql/part10/` (`spark/01_build.sql` … `08_orphans.sql`, `queries/*.sql`), `spark/jobs/part10_measure.py`, `part10_microbatches.py`, `part10_orphans.py`, `scripts/part10_layout.py` (`reset`, `spark`, `measure`, `microbatch`, `objects`, `crash`, `orphans`; run with `python -m`): four years of POS lines (72.6 million, twelve shifted copies of the lab's 126 days) as three Iceberg layouts (unpartitioned, day partitions, month partitions sorted by store); files, row groups and date spans per layout; a revenue and a store-promotion query measured from the Iceberg scan's own metrics; a Z-order rewrite; 41 hourly micro-batches and a merge-on-read delete; compaction, manifest rewrite, snapshot expiry and orphan-file removal, counted as objects in SeaweedFS; `tests/test_part10.py` |
| 11 | You Might Not Need a Cluster | `sql/part11/` (`queries/*.sql`, `publish.sql`), `scripts/part11_single_node.py` (`host`, `profile`, `spark`, `explain`, `order`, `arrow`, `publish`, `lock`, `oldfile`; run with `python -m`), `spark/jobs/part11_spark.py`: Part 10's four years of POS lines read through Polaris by DuckDB 2.0 and Polars 2.1 on the host and by Spark in its container; the revenue, store-promotion, four-year history and basket-size queries profiled (first and warm times, peak RSS, bytes from S3, spill) with and without a Spark-sized share of the machine; identical digests across the three engines; Polars' plan and row order; Arrow hand-offs; DuckDB publishing an Iceberg gold table that Polars reads; DuckDB's file lock and 2.0 storage format; `tests/test_part11.py` |
| 12 | Shuffle, Spill and Skew | `spark/jobs/part12_pipeline.py`, `part12_skew.py`, `part12_ui.py`, `sql/part12/ansi.sql`, `scripts/part12_spark.py` (`stage`, `reset`, `pipeline`, `ansi`, `build`, `hot`, `skew`; run with `python -m`): a PySpark job writing `silver.dim_store`, `silver.sales_line` (Q3 2026, month partitions, `overwritePartitions()` on re-runs) and `gold.daily_sales_by_format` with a broadcast join; ANSI mode on and off; `part12.campaign_lines`, a synthetic year in which store ID `GB-0001` carries 30% of 25 million lines; one join run five ways (broadcast, sort-merge, AQE skew join at default and lab-scaled thresholds, salting) with plans, task spread, shuffle bytes and spill read from the Spark UI's REST API; `tests/test_part12.py` |
| 13 | Engines at Scale | `scripts/part13_engines.py` (`reset`, `seed`, `trino`, `federate`, `clickhouse`, `load`, `serve`, `dashboard`; run with `python -m`), `sql/part13/` (`postgres/` targets, `trino/plan.sql` and `federation.sql`, `clickhouse/01_lake.sql` … `03_serve.sql`, dashboard tiles), `trino/catalog/oltp.properties`, `clickhouse/`: Part 11's queries in Trino over Iceberg (and the basket query hitting Trino's per-node memory limit); Trino joining Iceberg with store-ops sales targets in PostgreSQL, with what was pushed into each side; ClickHouse 26.8 reading the same table through Polaris (DataLakeCatalog, Beta), then from a MergeTree copy; a refreshable materialized view serving daily store sales and the query cache; a dashboard-tile load test at 1, 8 and 32 clients; `tests/test_part13.py` |

## Layout

```text
compose.yaml  .env.example  Makefile     the stack and its targets
datagen/                                 the synthetic Larkspur generator
sql/                                     DuckDB / Trino / Spark SQL used in the posts
scripts/                                 upload_raw.py, duck.py (run SQL files against the raw zone)
polaris/  postgres/  seaweedfs/  trino/  spark/   service configuration (spark/jobs: PySpark runners; seaweedfs/iam.json: STS role)
settings.gradle.kts  gradle/  iceberg-api-demo/  Java modules (Gradle, version catalog in gradle/libs.versions.toml)
tests/                                   pytest; `make ci` runs ruff and the tests
docs/versions.md  docs/injected-defects.md
```

There is no remote CI: `make ci` is the pipeline, run locally before every commit.
