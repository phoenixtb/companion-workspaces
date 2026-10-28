-- Part 5: two primitives every model sits on (DuckDB).

-- Money. Minor units are integers; DECIMAL shows them in major units. Dividing by 100 gives a DOUBLE.
SELECT sum(total_minor / 100)                         AS double_gbp,
       major(sum(total_minor))                        AS decimal_gbp,
       typeof(sum(total_minor / 100))                 AS double_type
FROM raw_orders
WHERE country = 'GB' AND currency = 'GBP';

-- Time. Where each country's business day starts, in UTC (none of the three changes clocks in Q3).
SELECT tz, strftime(timezone(tz, TIMESTAMP '2026-10-01 00:00:00'), '%Y-%m-%d %H:%M') || ' UTC' AS oct_1_starts_at
FROM (VALUES ('Europe/London'), ('Asia/Kolkata'), ('Asia/Singapore')) AS t(tz);

-- The UK's clocks go back at 02:00 BST on Sunday 25 October 2026: that business day lasts 25 hours, and
-- local 01:30 happens twice.
SELECT timezone('Europe/London', TIMESTAMP '2026-10-26 00:00:00')
           - timezone('Europe/London', TIMESTAMP '2026-10-25 00:00:00')       AS oct_25_length,
       strftime(TIMESTAMPTZ '2026-10-25 00:30:00+00' AT TIME ZONE 'Europe/London', '%H:%M') AS utc_00_30_in_london,
       strftime(TIMESTAMPTZ '2026-10-25 01:30:00+00' AT TIME ZONE 'Europe/London', '%H:%M') AS utc_01_30_in_london;
