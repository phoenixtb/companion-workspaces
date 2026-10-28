"""Part 12: a hot store, one join, five ways to run it. PySpark, local mode, measured from the Spark UI's REST API.

docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part12_skew.py build [--from D --to D]
docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part12_skew.py hot
docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part12_skew.py run <variant> [...]
    (variants: broadcast, merge, aqe_default, aqe_scaled, salted)

`build` writes part12.campaign_lines: a window of Part 10's sales lines plus synthetic baskets for one store
(re-keyed copies of other UK stores' baskets) until that store carries ~30% of the lines. Nothing here is real
campaign data. `run` joins the lines to silver.dim_store (written by part12_pipeline.py) and aggregates by day,
format and city, then prints the time, the final plan's join and exchange lines, and every stage's task spread.
"""

import statistics
import sys
import time

from part12_ui import execution_report, plan_lines, print_stages
from pyspark.sql import SparkSession

HOT = "GB-0001"
TARGET_SHARE = 0.30
SALTS = 16

QUERY = """
SELECT l.business_date, s.format, s.city, l.currency, count(*) AS lines, sum(l.amount_minor) AS sales_minor
FROM {lines} l JOIN lakehouse.silver.dim_store s ON l.store_id = s.store_id
GROUP BY l.business_date, s.format, s.city, l.currency
"""

# Salting, only for the stores that need it: their lines get a salt 0..15 from the basket id (deterministic, so a
# retried task gets the same salt); their dimension row is copied 16 times, once per salt. Other stores keep salt 0.
SALTED = """
WITH lines AS (
    SELECT l.*, CASE WHEN l.store_id IN ({hot}) THEN pmod(xxhash64(l.basket_id), {salts}) ELSE 0 END AS salt
    FROM part12.campaign_lines l),
stores AS (
    SELECT s.*, salt
    FROM lakehouse.silver.dim_store s
    LATERAL VIEW explode(sequence(0, CASE WHEN s.store_id IN ({hot}) THEN {salts} - 1 ELSE 0 END)) t AS salt)
SELECT l.business_date, s.format, s.city, l.currency, count(*) AS lines, sum(l.amount_minor) AS sales_minor
FROM lines l JOIN stores s ON l.store_id = s.store_id AND l.salt = s.salt
GROUP BY l.business_date, s.format, s.city, l.currency
"""

# Every variant runs Spark's default 200 shuffle partitions as 200 tasks (coalescing off), the way a cluster with
# ~200 cores would run them; the lab's 4 cores would otherwise get them merged into a handful of tasks.
BASE = {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.coalescePartitions.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10 MB, Spark's default
    "spark.sql.adaptive.skewJoin.enabled": "false",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",  # Spark's default
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",  # Spark's default
}
NO_BROADCAST = {"spark.sql.autoBroadcastJoinThreshold": "-1"}
VARIANTS = {
    "broadcast": {},  # Spark's own choice: dim_store is far below the broadcast threshold
    "merge": NO_BROADCAST,  # what the plan becomes when the small side is too big to broadcast
    "aqe_default": NO_BROADCAST | {"spark.sql.adaptive.skewJoin.enabled": "true"},
    # The lab holds ~1/50 of Larkspur's volume; like Part 10's file targets, the size thresholds are scaled by 1/64.
    "aqe_scaled": NO_BROADCAST
    | {
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "4MB",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1MB",
    },
    "salted": NO_BROADCAST,
}
PLAN_WORDS = ("Join", "Exchange", "AQEShuffleRead")


def build(spark: SparkSession, start: str, end: str) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.part12")
    spark.sql("DROP TABLE IF EXISTS lakehouse.part12.campaign_lines")
    window = f"business_date BETWEEN DATE '{start}' AND DATE '{end}'"
    base = spark.sql(f"SELECT * FROM lakehouse.part10.pos_monthly WHERE {window}")
    n = base.count()
    own = base.filter(f"store_id = '{HOT}'").count()
    extra = round((TARGET_SHARE * n - own) / (1 - TARGET_SHARE))
    uk_others = base.filter(f"country = 'GB' AND store_id <> '{HOT}'")
    copies = -(-extra // uk_others.count())  # ceiling: whole copies of the other UK stores' lines
    fraction = extra / (copies * uk_others.count())
    base.createOrReplaceTempView("base")
    spark.sql(f"""
        CREATE TABLE lakehouse.part12.campaign_lines
        USING iceberg
        PARTITIONED BY (months(business_date))
        AS SELECT * FROM base
        UNION ALL
        SELECT concat('{HOT}-', substr(basket_id, 9, 8), '-C', k, substr(basket_id, 4, 4), substr(basket_id, 17))
                   AS basket_id,
               line_no, '{HOT}' AS store_id, country, currency, event_ts, business_date, sku, qty,
               unit_price_minor, discount_minor, amount_minor, uploaded_at
        FROM base LATERAL VIEW explode(sequence(1, {copies})) c AS k
        WHERE country = 'GB' AND store_id <> '{HOT}'
          AND pmod(xxhash64(basket_id, k), 1000000) < {round(fraction * 1_000_000)}
    """)
    hot(spark)


def hot(spark: SparkSession | None = None) -> None:
    """The first question about a slow join: is one key carrying the data?"""
    spark = spark or SparkSession.getActiveSession()
    spark.sql("""
        SELECT store_id, count(*) AS lines, round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct_of_lines
        FROM lakehouse.part12.campaign_lines GROUP BY store_id ORDER BY lines DESC LIMIT 3
    """).show()
    spark.sql("""
        SELECT count(*) AS lines, count(DISTINCT store_id) AS stores, min(business_date) AS first_day,
               max(business_date) AS last_day
        FROM lakehouse.part12.campaign_lines
    """).show()


def run(spark: SparkSession, variant: str) -> None:
    for key, value in (BASE | VARIANTS[variant]).items():
        spark.conf.set(key, value)
    if variant == "salted":
        sql = SALTED.format(hot=f"'{HOT}'", salts=SALTS)
    else:
        sql = QUERY.format(lines="part12.campaign_lines")
    times = []
    for _ in range(3):  # three runs; the median time is printed, the metrics come from the last run
        df = spark.sql(sql)
        started = time.monotonic()
        rows = df.collect()
        times.append(time.monotonic() - started)
    seconds = statistics.median(times)
    ex, stages = execution_report(spark)
    total, sales = sum(r["lines"] for r in rows), sum(r["sales_minor"] for r in rows)
    print(f"variant {variant}: {seconds:.1f} s (median of 3), {len(rows):,} rows, {total:,} lines, checksum {sales:,}")
    print("final plan (join, exchange and shuffle-read lines):")
    for line in plan_lines(ex, PLAN_WORDS):
        print("  " + line.rstrip())
    print_stages(stages)


def main(args: list[str]) -> None:
    spark = SparkSession.builder.appName(f"part12_skew_{'_'.join(args[:2])}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE lakehouse")
    if args[0] == "build":
        opts = dict(zip(args[1::2], args[2::2], strict=False))
        build(spark, opts.get("--from", "2025-10-01"), opts.get("--to", "2026-09-30"))
    elif args[0] == "hot":
        hot(spark)
    elif args[0] == "run":
        for variant in args[1:]:
            run(spark, variant)


if __name__ == "__main__":
    main(sys.argv[1:])
