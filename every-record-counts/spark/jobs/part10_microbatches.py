"""Part 10, step 5: 4 October arrives as a stream would deliver it, one append per hour of uploads.

docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/part10_microbatches.py

Each hour's uploads become one Iceberg commit to pos_monthly (41 of them), then a correction deletes one store's day
with merge-on-read. That is what a micro-batch ingestion job leaves behind: many small files, many snapshots and
manifests, and delete files that every reader must apply until compaction.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TABLE = "lakehouse.part10.pos_monthly"
COLUMNS = ["basket_id", "line_no", "store_id", "country", "currency", "event_ts", "business_date", "sku", "qty",
           "unit_price_minor", "discount_minor", "amount_minor", "uploaded_at"]  # fmt: skip


def main() -> None:
    spark = SparkSession.builder.appName("part10_microbatches").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    first = Window.partitionBy("basket_id", "line_no").orderBy("uploaded_at")
    day = (
        spark.read.parquet("/opt/lakehouse/lab/part07/pos_lines")
        .withColumn("copy_no", F.row_number().over(first))
        .where("copy_no = 1 AND business_date = DATE '2026-10-04'")
        .select(*COLUMNS)
        .withColumn("line_no", F.col("line_no").cast("int"))
        .withColumn("qty", F.col("qty").cast("int"))
        .withColumn("upload_hour", F.date_trunc("HOUR", "uploaded_at"))
        .cache()
    )
    hours = [r.upload_hour for r in day.select("upload_hour").distinct().orderBy("upload_hour").collect()]
    for n, hour in enumerate(hours, 1):
        batch = day.where(F.col("upload_hour") == hour).drop("upload_hour")
        batch.writeTo(TABLE).append()  # one commit: new data files, a manifest, a manifest list, a metadata file
        if n in (1, len(hours)) or n % 10 == 0:
            print(f"micro-batch {n:>2}: uploads of {hour:%Y-%m-%d %H}:00, {batch.count():>5,} lines")

    spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')")
    spark.sql(f"DELETE FROM {TABLE} WHERE business_date = DATE '2026-10-04' AND store_id = 'SG-0007'")
    print("correction: SG-0007's 4 October lines deleted (merge-on-read)")


if __name__ == "__main__":
    main()
