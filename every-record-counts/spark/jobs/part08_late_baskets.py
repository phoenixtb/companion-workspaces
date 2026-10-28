"""Part 8: apply late baskets to the Delta tables, one UPDATE (one commit) per store-day.

spark-submit --properties-file $SPARK_CONF_DIR/delta.conf part08_late_baskets.py FIRST LAST TABLE [TABLE ...]
runs sql/part08/late_basket.sql for stores GB-<FIRST>..GB-<LAST> on each table.
"""

import sys
import time

from pyspark.sql import SparkSession


def main(first: int, last: int, tables: list[str]) -> None:
    spark = SparkSession.builder.appName("late_baskets").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    template = open("/opt/lakehouse/sql/part08/late_basket.sql").read()
    sql = "\n".join(line for line in template.splitlines() if not line.startswith("--"))
    for table in tables:
        started = time.monotonic()
        for n in range(first, last + 1):
            spark.sql(sql.format(table=table, store_id=f"GB-{n:04d}"))
        print(f"{table}: {last - first + 1} UPDATE commits ({time.monotonic() - started:.1f}s)")


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3:])
