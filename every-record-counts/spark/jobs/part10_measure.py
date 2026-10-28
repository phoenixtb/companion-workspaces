"""Run Part 10's queries against each layout and report what the Iceberg scan read, from Spark's own SQL metrics.

docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part10_measure.py pos_flat pos_daily ...
    [--queries revenue,store_promo]   (sql/part10/queries/<name>.sql; default: revenue and store_promo)

Each query runs once to warm up and three times timed; the median time is printed. The scan metrics come from the
BatchScan node of the last run, read from the Spark UI's REST API (/api/v1/applications/<app>/sql).
"""

import hashlib
import json
import statistics
import sys
import time
import urllib.request

from pyspark.sql import SparkSession

WANT = {  # BatchScan metric -> column
    "number of result data files": "files_read",
    "number of skipped data files": "files_skipped",
    "total data file size (bytes)": "file_bytes",
    "number of output rows": "rows_read",
    "number of result delete files": "delete_files",
}


def scan_metrics(spark: SparkSession) -> dict:
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/sql?details=true&planDescription=false&length=10000"
    last = json.load(urllib.request.urlopen(url))[-1]
    scan = next(n for n in last["nodes"] if n["nodeName"].startswith("BatchScan"))
    got = {m["name"]: m["value"] for m in scan["metrics"]}
    return {col: int(got[name].replace(",", "")) for name, col in WANT.items()}


def main(tables: list[str], queries: list[str]) -> None:
    spark = SparkSession.builder.appName("part10_measure").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    header = f"{'table':<12} {'query':<12} {'files_read':>10} {'skipped':>7} {'deletes':>7} {'MiB_in_files':>12} "
    header += f"{'rows_read':>10} {'seconds':>7}  result"
    print(header)
    print("-" * len(header))
    for q in queries:
        sql_text = open(f"/opt/lakehouse/sql/part10/queries/{q}.sql").read()
        sql_text = "\n".join(line for line in sql_text.splitlines() if not line.startswith("--"))
        for table in tables:
            sql = sql_text.format(table=f"lakehouse.part10.{table}")
            spark.sql(sql).collect()  # warm-up: metadata and object-store caches
            times = []
            for _ in range(3):
                started = time.monotonic()
                rows = spark.sql(sql).collect()
                times.append(time.monotonic() - started)
            m = scan_metrics(spark)
            digest = hashlib.sha256(repr(sorted(tuple(r) for r in rows)).encode()).hexdigest()[:8]
            print(
                f"{table:<12} {q:<12} {m['files_read']:>10,} {m['files_skipped']:>7,} {m['delete_files']:>7,} "
                f"{m['file_bytes'] / 1048576:>12.1f} {m['rows_read']:>10,} {statistics.median(times):>7.2f}  {digest}"
            )


if __name__ == "__main__":
    args = sys.argv[1:]
    names = ["revenue", "store_promo"]
    if "--queries" in args:
        i = args.index("--queries")
        names = args[i + 1].split(",")
        del args[i : i + 2]
    main(args, names)
