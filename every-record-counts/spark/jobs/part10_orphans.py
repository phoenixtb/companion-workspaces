"""Part 10, step 7: remove the orphan with Iceberg's Spark action, which (unlike the procedure) takes any interval.

docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part10_orphans.py

Safe in this lab only because nothing else writes pos_monthly now. In production keep the default 3 days, or at least
longer than your longest write: an in-flight write's files look exactly like orphans until it commits.
"""

import time

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("part10_orphans").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    jvm, session = spark.sparkContext._jvm, spark._jsparkSession
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(session, "lakehouse.part10.pos_monthly")
    result = (
        jvm.org.apache.iceberg.spark.actions.SparkActions.get(session)
        .deleteOrphanFiles(table)
        .olderThan(int(time.time() * 1000))  # anything written before this job started
        .usePrefixListing(True)
        .execute()
    )
    orphans = list(result.orphanFileLocations())
    print(f"deleted {len(orphans)} orphan file(s):")
    for path in orphans:
        print(f"  {path}")


if __name__ == "__main__":
    main()
