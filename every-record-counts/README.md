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

Python packages (DuckDB 2.0, Polars 2.1, PyIceberg 0.12, PyArrow 26) install into `.venv` on the first
`uv run`. Versions are pinned in `docs/versions.md`, `.env.example` and `uv.lock`.

## Quick start

```bash
make up-core      # SeaweedFS (S3), Polaris (Iceberg REST catalog), PostgreSQL 18
make data         # generate the raw zone into data/raw (about a minute, ~600 MB)
make upload       # copy it into s3://larkspur-raw/raw/
make test         # tests that need the stack skip themselves when it's down
```

Add Trino with `make up-trino`, or Trino and Spark with `make up-batch`. Stop everything with `make down`; delete all data with
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
| `core` | seaweedfs, postgres, polaris (+ one-shot bootstrap and catalog setup) | 1 GB + 1 GB + 1 GB |
| `batch` | trino, spark | 3 GB + 2.5 GB |

Measured on a MacBook Pro M3 Pro, Docker Desktop VM with 9.8 GB:

| Part | Profiles | Peak memory (containers) | Notes |
| --- | --- | --- | --- |
| 0 | core | 0.86 GB | DuckDB runs on the host |
| 1 | core + Trino (`make up-trino`) | 2.4 GB | SeaweedFS 0.69, Polaris 0.27, PostgreSQL 0.35, Trino 1.05; Spark not needed |

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

## Layout

```text
compose.yaml  .env.example  Makefile     the stack and its targets
datagen/                                 the synthetic Larkspur generator
sql/                                     DuckDB / Trino / Spark SQL used in the posts
scripts/                                 upload_raw.py, duck.py (run SQL files against the raw zone)
polaris/  postgres/  seaweedfs/  trino/  spark/   service configuration
tests/                                   pytest; `make ci` runs ruff and the tests
docs/versions.md  docs/injected-defects.md
```

There is no remote CI: `make ci` is the pipeline, run locally before every commit.
