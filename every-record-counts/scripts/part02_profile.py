"""Part 2: where a v2 run spends its time. Run after `part02_pipeline.py land` and `run` over every upload.

uv run python scripts/part02_profile.py

1. JSON schema inference: the same six-upload count through a view that samples files the default way and through
   one that samples a single file (what sql/part02/views.sql does).
2. Ten v2 runs (uploads 10 to 19 August), timed statement by statement. v2 is idempotent, so re-running is safe.
"""

import statistics
import time
from datetime import date, timedelta

import part02_pipeline as p  # sits next to this script

con = p.connect()
root = con.execute("SELECT getvariable('landed_root')").fetchone()[0]
events = f"'{root}/pos/events/*/*.jsonl.gz'"
count = "SELECT count(*) FROM {v} WHERE upload_date BETWEEN DATE '2026-08-13' AND DATE '2026-08-18'"


def median_seconds(sql: str, repeat: int = 5) -> float:
    times = []
    for _ in range(repeat):
        started = time.monotonic()
        con.execute(sql).fetchall()
        times.append(time.monotonic() - started)
    return statistics.median(times)


print("1. count(*) over six uploads, median of 5")
for label, options in (("default sampling", ""), ("maximum_sample_files = 1", ", maximum_sample_files = 1")):
    view = f"SELECT * FROM read_json({events}, hive_partitioning = true{options})"
    con.execute(f"CREATE OR REPLACE TEMP VIEW probe AS {view}")
    print(f"   {label:<28} {median_seconds(count.format(v='probe')):.2f}s")

steps = {"find affected dates (read 1 upload)": 0.0}
labels = ["silver DELETE", "silver INSERT (read + dedupe JSON)", "gold DELETE", "gold INSERT", "COMMIT"]
steps |= dict.fromkeys(labels, 0.0)
uploads = p.days(date(2026, 8, 10), date(2026, 8, 19))
for day in uploads:
    con.execute(f"SET VARIABLE run_date = DATE '{day}'")
    started = time.monotonic()
    dates = [r[0] for r in con.execute(p.sql("affected_dates")).fetchall()]
    steps["find affected dates (read 1 upload)"] += time.monotonic() - started
    con.execute("CREATE OR REPLACE TEMP TABLE affected AS SELECT unnest(?::DATE[]) AS business_date", [dates])
    con.execute(f"SET VARIABLE first_upload = DATE '{min(dates) - timedelta(days=1)}'")
    con.execute(f"SET VARIABLE last_upload = DATE '{max(dates) + timedelta(days=p.LATE_DAYS)}'")
    con.execute("BEGIN TRANSACTION")
    for label, statement in zip(labels, [*con.extract_statements(p.sql("rebuild")), None], strict=True):
        started = time.monotonic()
        con.execute(statement.query if statement else "COMMIT")
        steps[label] += time.monotonic() - started

total = sum(steps.values())
print(f"2. v2 runs for uploads {uploads[0]} to {uploads[-1]}: {total / len(uploads):.2f}s per run")
for label, seconds in steps.items():
    print(f"   {label:<38} {seconds / len(uploads):.3f}s  {seconds / total:4.0%}")
