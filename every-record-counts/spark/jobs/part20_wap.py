"""Part 20: Write-Audit-Publish for the POS sales fact, gold.fct_pos_line, on an Iceberg branch.

--init --until D   create the table (write.wap.enabled) and load every upload day up to D straight into main
--day D            publish upload day D: write to branch `audit`, run sql/part20/wap_checks.sql there, and
                   fast-forward main to audit only if every check passes; otherwise main is not touched
--bug skip-dedup   the day's write forgets to drop re-sent baskets (a plausible regression), to watch the audit stop it

Submitted by `scripts.part20_wap` with the `pipeline` catalog: Part 9's pipeline principal, vended credentials.
The write itself doesn't know about branches: `spark.wap.branch` sends it to `audit`.
"""

import argparse
import sys
import time

from pyspark.sql import SparkSession

TABLE = "pipeline.gold.fct_pos_line"
RAW = "pipeline.raw.pos_uploads"
BRANCH = "audit"
CHECKS = "/opt/lakehouse/sql/part20/wap_checks.sql"

# One row per basket line. A till that reconnects re-sends a basket (same basket_id and event_ts, new event_id,
# later upload): keep the first upload, and skip baskets the fact already holds from an earlier upload day.
LINES = """
SELECT b.basket_id AS transaction_id, l.line_no, b.store_id, b.currency, b.event_ts, b.uploaded_at, b.upload_date,
       l.sku, l.qty, l.unit_price_minor, l.discount_minor, l.promo_id,
       l.qty * l.unit_price_minor - l.discount_minor AS amount_minor
FROM (SELECT *, row_number() OVER (PARTITION BY basket_id, event_ts ORDER BY uploaded_at, event_id) AS copy_no
      FROM {raw} r WHERE {where}) b
LATERAL VIEW inline(b.lines) l
WHERE {keep}
"""
FIRST_COPY = "b.copy_no = 1"
NOT_PUBLISHED = """NOT EXISTS (SELECT 1 FROM {published} f WHERE f.transaction_id = r.basket_id
                                AND f.event_ts = r.event_ts AND f.upload_date < DATE '{day}')"""


def statements(path: str) -> list[str]:
    with open(path) as f:
        code = "\n".join(line for line in f.read().splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in code.split(";") if s.strip()]


def ref(spark: SparkSession, name: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = '{name}'").collect()
    return row[0][0] if row else None


def init(spark: SparkSession, until: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    lines = LINES.format(raw=RAW, where=f"upload_date <= DATE '{until}'", keep=FIRST_COPY)
    spark.sql(f"""CREATE TABLE {TABLE} USING iceberg PARTITIONED BY (months(event_ts))
                  TBLPROPERTIES ('format-version' = '2', 'write.wap.enabled' = 'true')
                  AS {lines}""")
    n = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
    print(f"init: {TABLE} main = snapshot {ref(spark, 'main')}, {n:,} lines (upload days to {until})")


def publish(spark: SparkSession, day: str, bug: str | None) -> int:
    main_before = ref(spark, "main")
    # A fresh audit branch at main's head: whatever a failed run left on it is discarded.
    spark.sql(f"ALTER TABLE {TABLE} CREATE OR REPLACE BRANCH {BRANCH}")

    # WRITE. From here on, every write to the table commits to `audit`; main doesn't move.
    spark.conf.set("spark.wap.branch", BRANCH)
    spark.sql(f"DELETE FROM {TABLE} WHERE upload_date = DATE '{day}'")  # a re-run replaces the day
    where, keep = f"upload_date = DATE '{day}'", "true"
    if bug != "skip-dedup":  # drop re-sends within the day, and baskets main already published from an earlier day
        where += " AND " + NOT_PUBLISHED.format(published=f"{TABLE} VERSION AS OF 'main'", day=day)
        keep = FIRST_COPY
    spark.sql(f"INSERT INTO {TABLE} {LINES.format(raw=RAW, where=where, keep=keep)}")
    spark.conf.unset("spark.wap.branch")
    written = spark.sql(f"SELECT count(*) FROM {TABLE} VERSION AS OF '{BRANCH}' WHERE upload_date = DATE '{day}'")
    audit_head = ref(spark, BRANCH)
    parent = dict(spark.sql(f"SELECT snapshot_id, parent_id FROM {TABLE}.snapshots").collect())
    commits, s = 0, audit_head
    while s != main_before:  # walk the branch back to where it left main
        commits, s = commits + 1, parent[s]
    print(f"write: {written.collect()[0][0]:,} lines for upload day {day} on branch {BRANCH} in {commits} commit(s), "
          f"head {audit_head}; main still {ref(spark, 'main')}")  # fmt: skip

    # AUDIT. The same table, read at the branch head and at main.
    failed = []
    for sql in statements(CHECKS):
        check, bad = spark.sql(sql.format(audit=f"{TABLE}.branch_{BRANCH}", main=TABLE, raw=RAW, day=day)).collect()[0]
        print(f"audit: {check:<20} {'pass' if bad == 0 else f'FAIL {bad:,}'}")
        if bad:
            failed.append(check)
    if failed:
        print(f"publish: refused ({', '.join(failed)}); main stays at snapshot {ref(spark, 'main')}")
        return 1

    # PUBLISH. Main moves to the audited snapshot in one metadata commit; no data file is written or copied.
    row = spark.sql(f"CALL pipeline.system.fast_forward('gold.fct_pos_line', 'main', '{BRANCH}')").collect()[0]
    print(f"publish: main {row.previous_ref} -> {row.updated_ref} (was {main_before})")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--init", action="store_true")
    p.add_argument("--until")
    p.add_argument("--day")
    p.add_argument("--bug", choices=["skip-dedup"])
    a = p.parse_args()
    spark = SparkSession.builder.appName("part20_wap").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    started = time.monotonic()
    rc = init(spark, a.until) if a.init else publish(spark, a.day, a.bug)
    print(f"({time.monotonic() - started:.1f} s)")
    return rc or 0


if __name__ == "__main__":
    sys.exit(main())
