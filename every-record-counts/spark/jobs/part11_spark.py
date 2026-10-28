"""Part 11: Spark's side of the comparison. Runs sql/part11/queries against one of Part 10's tables.

docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part11_spark.py pos_monthly \
    [--queries revenue,store_promo,history,baskets]

Same method as scripts/part11_single_node.py: the first run is timed on its own, then three more runs give the
median. The digest normalises every value to text, so it matches DuckDB's and Polars' digests for the same answer.
"""

import hashlib
import json
import statistics
import sys
import time

from pyspark.sql import SparkSession

QUERIES = ["revenue", "store_promo", "history", "baskets"]


def digest(rows) -> str:
    return hashlib.sha256(repr(sorted(tuple(str(v) for v in r) for r in rows)).encode()).hexdigest()[:8]


def main(table: str, queries: list[str]) -> None:
    started = time.monotonic()
    spark = SparkSession.builder.appName("part11_spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(json.dumps({"session_seconds": round(time.monotonic() - started, 2)}))
    for q in queries:
        text = open(f"/opt/lakehouse/sql/part11/queries/{q}.sql").read()
        sql = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
        sql = sql.format(table=f"lakehouse.part10.{table}")
        t0 = time.monotonic()
        rows = spark.sql(sql).collect()
        first = time.monotonic() - t0
        times = []
        for _ in range(3):
            t0 = time.monotonic()
            rows = spark.sql(sql).collect()
            times.append(time.monotonic() - t0)
        result = {"engine": "spark", "table": table, "query": q, "first": first, "warm": statistics.median(times)}
        print(json.dumps(result | {"rows": len(rows), "digest": digest(rows)}))


if __name__ == "__main__":
    args = sys.argv[1:]
    names = QUERIES
    if "--queries" in args:
        i = args.index("--queries")
        names = args[i + 1].split(",")
        del args[i : i + 2]
    main(args[0] if args else "pos_monthly", names)
