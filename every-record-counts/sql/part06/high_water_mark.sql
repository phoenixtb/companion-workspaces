-- Part 6: incremental loads with a high-water mark, simulated over every daily POS upload (DuckDB).
-- Each night's job loads rows newer than the largest value it has loaded so far: WHERE col > :high_water_mark.
-- Which column holds the mark decides what gets lost.
WITH batches AS (
    SELECT upload_date, max(event_ts) AS max_event_ts, max(uploaded_at) AS max_uploaded_at
    FROM raw_pos_events
    GROUP BY upload_date
),
marks AS (                  -- the mark each night's job starts from: the maximum over every earlier night
    SELECT upload_date,
           max(max_event_ts)    OVER (ORDER BY upload_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS hwm_event_ts,
           max(max_uploaded_at) OVER (ORDER BY upload_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS hwm_uploaded_at
    FROM batches
),
baskets AS (
    SELECT * FROM raw_pos_events
    QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) = 1
)
SELECT count(*)                                                              AS baskets,
       count(*) FILTER (WHERE b.event_ts <= m.hwm_event_ts)                  AS never_loaded_by_event_ts,
       count(*) FILTER (WHERE b.uploaded_at <= m.hwm_uploaded_at)            AS never_loaded_by_uploaded_at,
       median(m.hwm_event_ts - b.event_ts) FILTER (WHERE b.event_ts <= m.hwm_event_ts) AS median_behind_the_mark
FROM baskets b
JOIN marks m USING (upload_date);
