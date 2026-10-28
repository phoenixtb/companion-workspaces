-- Part 6: find the duplicate POS events behind revenue step 1 (DuckDB).
-- Statement 1: which column is the key? Every event_id is unique; basket_id is not.
SELECT left(store_id, 2)          AS country,
       count(*)                   AS events,
       count(DISTINCT event_id)   AS event_ids,
       count(DISTINCT basket_id)  AS baskets,
       count(*) - count(DISTINCT basket_id) AS extra_copies
FROM raw_pos_events
GROUP BY ALL
ORDER BY country;

-- Statement 2: what a re-send looks like. Number the copies of each basket in upload order; compare copy 2 with 1.
WITH copies AS (
    SELECT *,
           row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) AS copy_no,
           lag(uploaded_at) OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) AS previous_upload,
           lag(md5(lines::VARCHAR || tenders::VARCHAR || event_ts::VARCHAR))
               OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) AS previous_payload
    FROM raw_pos_events
)
SELECT count(*)                                    AS resends,
       count(DISTINCT till_id)                     AS tills,
       count(*) FILTER (WHERE md5(lines::VARCHAR || tenders::VARCHAR || event_ts::VARCHAR) = previous_payload)
                                                   AS same_payload,
       min(uploaded_at - previous_upload)          AS soonest,
       median(uploaded_at - previous_upload)       AS median,
       max(uploaded_at - previous_upload)          AS latest
FROM copies
WHERE copy_no > 1;

-- Statement 3: the legacy board-deck POS figure (UTC dates, every event), with and without the re-sends.
WITH gb AS (
    SELECT tenders[1].amount_minor AS amount_minor,
           row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) AS copy_no
    FROM raw_pos_events
    WHERE store_id LIKE 'GB-%'
      AND event_ts::DATE BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
)
SELECT count(*) FILTER (WHERE copy_no > 1)                     AS resent_events,
       major(sum(amount_minor))                                 AS every_event_gbp,
       major(sum(amount_minor) FILTER (WHERE copy_no = 1))      AS one_per_basket_gbp,
       major(sum(amount_minor) FILTER (WHERE copy_no > 1))      AS resent_gbp
FROM gb;

-- Statement 4: the same dedup three ways. QUALIFY filters on a window result without another subquery.
SELECT 'QUALIFY row_number() = 1 by basket_id' AS method, count(*) AS events
FROM (SELECT * FROM raw_pos_events
      QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) = 1)
UNION ALL
SELECT 'DISTINCT ON (basket_id)', count(*)
FROM (SELECT DISTINCT ON (basket_id) * FROM raw_pos_events ORDER BY basket_id, uploaded_at, event_id)
UNION ALL
SELECT 'QUALIFY row_number() = 1 by event_id', count(*)
FROM (SELECT * FROM raw_pos_events QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY uploaded_at) = 1)
ORDER BY events;
