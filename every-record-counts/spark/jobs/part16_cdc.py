"""Part 16: Debezium change events from Kafka into Iceberg, one micro-batch at a time.

For each micro-batch (foreachBatch) and each of shop.orders, shop.payments and shop.refunds:
  1. append every change event to raw.<table>_changes, the history (skipped if this batch already committed it);
  2. MERGE the latest change per key into raw.<table>, the current state (an LSN guard makes a replay a no-op).

Submitted by `scripts.part16_cdc stream` with a `pipeline` catalog: Part 9's pipeline principal, vended credentials.
--fail-after-batch N raises after batch N's commits, before Spark records the batch as done: the next run replays it.
"""

import argparse
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

CATALOG = "pipeline.raw"
CHECKPOINT = "/opt/lakehouse/state/part16/cdc"
STREAM = "part16-cdc"  # names this stream in snapshot summaries; reset drops tables and checkpoint together
# Columns as the services own them; Debezium sends timestamptz as ISO-8601 text, cast after parsing.
TABLES = {
    "orders": (
        "order_id",
        "order_id string, customer_id string, country string, currency string, channel string, status string, "
        "total_minor bigint, tender_type string, created_at timestamp, updated_at timestamp, fulfilled_at timestamp",
    ),
    "payments": (
        "payment_id",
        "payment_id string, source string, reference string, country string, currency string, tender_type string, "
        "amount_minor bigint, status string, authorised_at timestamp, captured_at timestamp, voided_at timestamp",
    ),
    "refunds": (
        "refund_id",
        "refund_id string, original_channel string, original_reference string, country string, store_id string, "
        "currency string, tender_type string, refund_ts timestamp, amount_minor bigint, reason string",
    ),
}
CHANGE_COLS = (
    "_op string, _lsn bigint, _tx_id bigint, _source_ts timestamp, _snapshot string, "
    "_kafka_partition int, _kafka_offset bigint, _batch_id bigint, _ingested_at timestamp"
)


def columns(ddl: str) -> list[tuple[str, str]]:
    return [tuple(c.strip().split(" ")) for c in ddl.split(",")]


def envelope(ddl: str) -> StructType:
    """The Debezium value (JSON converter, schemas off): before, after, source, op."""
    row = StructType([StructField(n, LongType() if t == "bigint" else StringType()) for n, t in columns(ddl)])
    source = StructType(
        [
            StructField(n, t)
            for n, t in [("lsn", LongType()), ("txId", LongType()), ("ts_us", LongType()), ("snapshot", StringType())]
        ]
    )
    return StructType(
        [
            StructField("before", row),
            StructField("after", row),
            StructField("source", source),
            StructField("op", StringType()),
        ]
    )


def create_tables(spark: SparkSession) -> None:
    for table, (_, ddl) in TABLES.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.{table}_changes ({CHANGE_COLS}, {ddl}) USING iceberg")
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{table} ({ddl}, _lsn bigint, _from_snapshot boolean,
                _source_ts timestamp) USING iceberg
                TBLPROPERTIES ('write.merge.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read',
                               'write.delete.mode' = 'merge-on-read')"""
        )


def changes(batch: DataFrame, table: str, batch_id: int) -> DataFrame:
    """One row per change event: Debezium metadata plus the row (`after`, or `before` for a delete)."""
    _, ddl = TABLES[table]
    e = batch.where(F.col("topic") == f"larkspur.shop.{table}").select(
        F.from_json(F.col("value").cast("string"), envelope(ddl)).alias("e"), "partition", "offset"
    )
    row = [
        F.when(F.col("e.op") == "d", F.col(f"e.before.{n}")).otherwise(F.col(f"e.after.{n}")).cast(t).alias(n)
        for n, t in columns(ddl)
    ]
    return e.select(
        F.col("e.op").alias("_op"),
        F.col("e.source.lsn").alias("_lsn"),
        F.col("e.source.txId").alias("_tx_id"),
        F.timestamp_micros(F.col("e.source.ts_us")).alias("_source_ts"),  # commit time in PostgreSQL
        F.col("e.source.snapshot").alias("_snapshot"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.lit(batch_id).cast("bigint").alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
        *row,
    )


def committed(spark: SparkSession, table: str, tag: str) -> bool:
    found = spark.sql(f"SELECT 1 FROM {table}.snapshots WHERE summary['part16.batch'] = '{tag}'")
    return found.limit(1).count() > 0


# s is newer than t: a streamed change beats a snapshot read; otherwise the higher LSN wins. Equal is not newer,
# so a replayed batch changes nothing.
NEWER = "((s._lsn > t._lsn AND s._from_snapshot = t._from_snapshot) OR (t._from_snapshot AND NOT s._from_snapshot))"


def merge(spark: SparkSession, table: str, ch: DataFrame) -> None:
    key, ddl = TABLES[table]
    cols = [n for n, _ in columns(ddl)] + ["_lsn", "_from_snapshot", "_source_ts"]
    # Newest change per key in this batch. A snapshot read carries the LSN where the snapshot started, and a
    # transaction in flight at that moment can stream changes with lower LSNs, so any streamed change outranks a
    # snapshot read; among streamed changes (one row's changes are serialised by its lock) the higher LSN wins.
    newest = Window.partitionBy(key).orderBy(
        F.col("_from_snapshot"), F.col("_lsn").desc(), F.col("_kafka_offset").desc()
    )
    latest = (
        ch.withColumn("_from_snapshot", F.col("_op") == "r")
        .withColumn("_rn", F.row_number().over(newest))
        .where("_rn = 1")
    )
    latest.createOrReplaceTempView(f"latest_{table}")
    spark.sql(f"""
        MERGE INTO {CATALOG}.{table} t
        USING latest_{table} s
        ON t.{key} = s.{key}
        WHEN MATCHED AND {NEWER} AND s._op = 'd' THEN DELETE
        WHEN MATCHED AND {NEWER} THEN UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in cols)}
        WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT ({", ".join(cols)}) VALUES ({", ".join(f"s.{c}" for c in cols)})
    """)


def last_snapshot(spark: SparkSession, table: str) -> dict:
    rows = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    )
    r = rows.collect()
    return {"id": r[0]["snapshot_id"], "op": r[0]["operation"], **r[0]["summary"]} if r else {"id": None}


def process(batch: DataFrame, batch_id: int, fail_after: int | None) -> None:
    spark = batch.sparkSession
    started = time.monotonic()
    batch.persist()
    tag = f"{STREAM}:{batch_id}"
    parts = []
    for table in TABLES:
        ch = changes(batch, table, batch_id).persist()
        ops = {r["_op"]: r["count"] for r in ch.groupBy("_op").count().collect()}
        if not ops:
            ch.unpersist()
            continue
        hist = f"{CATALOG}.{table}_changes"
        if committed(spark, hist, tag):
            appended = "already committed, skipped"
        else:
            ch.writeTo(hist).option("snapshot-property.part16.batch", tag).append()
            appended = f"+{sum(ops.values()):,}"
        before = last_snapshot(spark, f"{CATALOG}.{table}")
        merge(spark, table, ch)
        after = last_snapshot(spark, f"{CATALOG}.{table}")
        if after["id"] == before["id"]:
            merged = "no commit"
        else:
            merged = (
                f"{after['op']} +{int(after.get('added-records', 0)):,} rows "
                f"+{int(after.get('added-position-deletes', 0)):,} position deletes"
            )
        counts = " ".join(f"{k}={ops[k]:,}" for k in "rcud" if k in ops)
        parts.append(f"  {table:<8} {counts:<34} changes {appended:<26} current {merged}")
        ch.unpersist()
    batch.unpersist()
    print(f"batch {batch_id}: {time.monotonic() - started:.1f} s", flush=True)
    print("\n".join(parts), flush=True)
    if fail_after is not None and batch_id == fail_after:
        raise RuntimeError(f"simulated crash after batch {batch_id} committed, before Spark recorded it")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--max-offsets", type=int, default=250_000, help="maxOffsetsPerTrigger: records per micro-batch")
    p.add_argument("--fail-after-batch", type=int)
    a = p.parse_args()
    spark = SparkSession.builder.appName("part16-cdc").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    create_tables(spark)
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", ",".join(f"larkspur.shop.{t}" for t in TABLES))
        .option("startingOffsets", "earliest")  # only for the first run; later runs resume from the checkpoint
        .option("maxOffsetsPerTrigger", a.max_offsets)
        .load()
    )
    query = (
        source.writeStream.foreachBatch(lambda df, bid: process(df, bid, a.fail_after_batch))
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)  # everything Kafka has now, in batches of --max-offsets, then stop
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
