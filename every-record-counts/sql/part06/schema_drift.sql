-- Part 6: the clickstream changed shape twice in Q3 (DuckDB).
-- Statement 1: read_json infers one schema from a sample of files. Without union_by_name, fields that only appear in
-- later files are not in the schema at all, and no error says so.
WITH by_default AS (
    DESCRIBE SELECT * FROM read_json(getvariable('raw_root') || '/clickstream/events/*/*.jsonl.gz',
                                     hive_partitioning = true)
),
by_name AS (
    DESCRIBE SELECT * FROM raw_clickstream    -- the lab's view: union_by_name = true
)
SELECT (SELECT count(*) FROM by_default)                                          AS default_columns,
       (SELECT count(*) FROM by_name)                                             AS union_by_name_columns,
       (SELECT list(column_name ORDER BY column_name) FROM by_name ANTI JOIN by_default USING (column_name))
                                                                                  AS missing_by_default;

-- Statement 2: page was renamed page_path (web on 15 August, app as users updated). Per ISO week, the share of
-- events with a page, reading only the old name vs coalescing both.
SELECT date_trunc('week', event_ts)::DATE                     AS week,
       count(*)                                               AS events,
       round(100 * count(page) / count(*), 1)                 AS page_only_pct,
       round(100 * count(coalesce(page_path, page)) / count(*), 1) AS coalesced_pct,
       round(100 * count(consent) / count(*), 1)              AS with_consent_pct
FROM raw_clickstream
WHERE event_ts >= TIMESTAMPTZ '2026-07-27' AND event_ts < TIMESTAMPTZ '2026-09-07'
GROUP BY ALL
ORDER BY week;
