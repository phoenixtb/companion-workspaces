"""Run Spark SQL files statement by statement and print each result as a compact table.

docker compose exec spark /opt/spark/bin/spark-submit /opt/lakehouse/jobs/run_sql.py /opt/lakehouse/sql/part07/x.sql

Comment lines are dropped before splitting on ';'. A statement that fails prints its error and the run goes on,
unless --strict is given (the tests use it). One Spark session runs every file, so temporary views carry over.
"""

import logging
import sys
import time

from pyspark.sql import SparkSession


def show(df, limit: int = 40) -> None:
    rows = df.take(limit + 1)
    cells = [df.columns] + [["NULL" if v is None else str(v) for v in r] for r in rows[:limit]]
    widths = [max(len(r[i]) for r in cells) for i in range(len(df.columns))]
    for n, r in enumerate(cells):
        print("  ".join(v.rjust(w) for v, w in zip(r, widths, strict=True)))
        if n == 0:
            print("  ".join("-" * w for w in widths))
    if len(rows) > limit:
        print(f"... more than {limit} rows")


def statements(path: str) -> list[str]:
    with open(path) as f:
        code = "\n".join(line for line in f.read().splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in code.split(";") if s.strip()]


def main(args: list[str]) -> int:
    strict = "--strict" in args
    files = [a for a in args if a != "--strict"]
    spark = SparkSession.builder.appName("run_sql").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("SQLQueryContextLogger").disabled = True  # we print our own one-line error
    for path in files:
        print(f"-- {path.removeprefix('/opt/lakehouse/')}")
        for sql in statements(path):
            started = time.monotonic()
            try:
                df = spark.sql(sql)
                if df.columns:
                    show(df)
                    print(f"({time.monotonic() - started:.1f}s)\n")
                else:  # DDL and DML: one line, the statement's first line and its time
                    print(f"> {sql.splitlines()[0]}  ({time.monotonic() - started:.1f}s)")
            except Exception as e:  # noqa: BLE001 - print and continue: some statements fail on purpose
                print(f"> {sql.splitlines()[0]}\nERROR: {type(e).__name__}: {str(e).splitlines()[0]}")
                if strict:
                    return 1
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
