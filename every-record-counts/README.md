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

Python packages (DuckDB 2.0, Polars 2.1, PyIceberg 0.12, PyArrow 26) install into `.venv` on the first
`uv run`. Versions are pinned in `docs/versions.md`, `.env.example` and `uv.lock`.

## Quick start

```bash
make up-core      # SeaweedFS (S3), Polaris (Iceberg REST catalog), PostgreSQL 18
make data         # generate the raw zone into data/raw (about a minute, ~600 MB)
make upload       # copy it into s3://larkspur-raw/raw/
make test         # tests that need the stack skip themselves when it's down
```

Add Trino with `make up-trino`, Spark with `make up-spark`, or both with `make up-batch` (both need more memory
than a 9.8 GB Docker VM leaves spare; Part 7 runs them one at a time). Stop everything with `make down`; delete all data with
`make reset`. `make help` lists every target.

| Service | URL | Credentials (lab only) |
| --- | --- | --- |
| SeaweedFS S3 API | http://localhost:8333 | `larkspur` / `larkspur-lab-secret` |
| SeaweedFS filer UI (browse buckets) | http://localhost:8888 | none |
| Polaris (Iceberg REST catalog) | http://localhost:8181/api/catalog | client `root` / `s3cr3t`, scope `PRINCIPAL_ROLE:ALL` |
| PostgreSQL | localhost:5432, database `larkspur` | `larkspur` / `larkspur` |
| Trino (batch) | http://localhost:8080 | any user name, no password |
| Spark UI (batch, while a job runs) | http://localhost:4040 | none |

The credentials are also in `.env.example`; `seaweedfs/s3.json` repeats the S3 keys because SeaweedFS
reads identities from a file.

## Profiles and memory

Every service has a memory limit, and the JVMs size their heaps from it. Start only what a part needs.

| Profile | Services | Limits |
| --- | --- | --- |
| `core` | seaweedfs, postgres, polaris (+ one-shot bootstrap and catalog setup) | 1.5 GB + 1 GB + 1 GB |
| `batch` | trino, spark | 3 GB + 2.5 GB |

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

## Layout

```text
compose.yaml  .env.example  Makefile     the stack and its targets
datagen/                                 the synthetic Larkspur generator
sql/                                     DuckDB / Trino / Spark SQL used in the posts
scripts/                                 upload_raw.py, duck.py (run SQL files against the raw zone)
polaris/  postgres/  seaweedfs/  trino/  spark/   service configuration (spark/jobs: PySpark runners)
settings.gradle.kts  gradle/  iceberg-api-demo/  Java modules (Gradle, version catalog in gradle/libs.versions.toml)
tests/                                   pytest; `make ci` runs ruff and the tests
docs/versions.md  docs/injected-defects.md
```

There is no remote CI: `make ci` is the pipeline, run locally before every commit.
