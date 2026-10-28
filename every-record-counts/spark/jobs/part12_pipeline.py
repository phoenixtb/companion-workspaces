"""Part 12: Larkspur's nightly sales job in PySpark: silver sales lines and a gold daily table, by whole months.

docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part12_pipeline.py [--months 2026-07,...]

Reads Part 10's four years of POS lines (lakehouse.part10.pos_monthly) and the store reference file staged by
`scripts/part12_spark.py stage`, and writes three Iceberg tables through Polaris:
  silver.dim_store                one row per store (replaced on every run)
  silver.sales_line               the month partitions of the requested months, replaced as a whole
  gold.daily_sales_by_format      lines, baskets and sales per business date, country, currency and store format
Both fact tables are partitioned by month and written with overwritePartitions(), so a re-run replaces the same
months instead of appending them again (Part 2). The gold query's plan is printed before it runs, and what each
stage did is read from the Spark UI's REST API after it ran.
"""

import sys
import time
from datetime import date, timedelta

from part12_ui import execution_report, get, print_stages
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning

STORES_CSV = "/opt/lakehouse/lab/part12/stores.csv"
STORES_SCHEMA = (
    "store_id STRING, country STRING, currency STRING, timezone STRING, city STRING, format STRING, "
    "open_24h BOOLEAN, opens_at STRING, closes_at STRING, wms_snapshot_schedule STRING, opened_on DATE"
)


def write(df: DataFrame, table: str, spark: SparkSession, partitioned: bool) -> None:
    """Create the table on the first run; afterwards replace only the month partitions present in df."""
    writer = df.writeTo(table).using("iceberg")
    if not spark.catalog.tableExists(table):
        (writer.partitionedBy(partitioning.months("business_date")) if partitioned else writer).create()
    elif partitioned:
        writer.overwritePartitions()
    else:
        writer.createOrReplace()


def main(months: list[str]) -> None:
    spark = SparkSession.builder.appName("part12_pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    for ns in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
    driver = get(spark, "executors")[0]  # local mode: the driver is the only executor
    print(f"unified memory (execution + storage): {driver['maxMemory'] / 1048576:.1f} MiB of a 1.5 GB heap")

    # Whole months only (see write()), as a date range the Iceberg scan can prune partitions and files with.
    first = date.fromisoformat(f"{min(months)}-01")
    last = (date.fromisoformat(f"{max(months)}-01") + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    in_window = F.col("business_date").between(first, last)

    # Nothing below reads data until a write (an action) runs: these are plans, not results (lazy evaluation).
    stores = spark.read.csv(STORES_CSV, header=True, schema=STORES_SCHEMA)
    lines = (
        spark.table("lakehouse.part10.pos_monthly")
        .where(in_window)
        .withColumn("loaded_at", F.current_timestamp())  # when this run wrote the row (ingestion time stays)
    )

    started = time.monotonic()
    write(stores, "lakehouse.silver.dim_store", spark, partitioned=False)
    write(lines, "lakehouse.silver.sales_line", spark, partitioned=True)
    print(f"silver written in {time.monotonic() - started:.1f} s")

    started = time.monotonic()
    silver = spark.table("lakehouse.silver.sales_line").where(in_window)
    dim = spark.table("lakehouse.silver.dim_store").select("store_id", "format")
    gold = (
        silver.join(dim, "store_id")  # 600 rows: Spark broadcasts it instead of shuffling 4 million lines
        .groupBy("business_date", "country", "currency", "format")
        .agg(
            F.count("*").alias("lines"),
            F.countDistinct("basket_id").alias("baskets"),
            F.sum("amount_minor").alias("sales_minor"),
        )
    )
    print(f"gold DataFrame defined in {time.monotonic() - started:.2f} s; no data read yet")
    gold.explain(mode="formatted")
    started = time.monotonic()
    write(gold, "lakehouse.gold.daily_sales_by_format", spark, partitioned=True)
    print(f"gold written in {time.monotonic() - started:.1f} s")
    _, stages = execution_report(spark)
    print_stages(stages)

    for table in ("silver.dim_store", "silver.sales_line", "gold.daily_sales_by_format"):
        n = spark.table(f"lakehouse.{table}").count()
        snaps = spark.sql(f"SELECT operation FROM lakehouse.{table}.snapshots ORDER BY committed_at").collect()
        print(f"{table:<28} {n:>10,} rows  snapshots: {', '.join(s.operation for s in snaps)}")


if __name__ == "__main__":
    args = sys.argv[1:]
    chosen = args[args.index("--months") + 1].split(",") if "--months" in args else ["2026-07", "2026-08", "2026-09"]
    main(chosen)
