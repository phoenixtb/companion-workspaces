-- Part 21: one upload day's POS events, each given a disposition (DuckDB). Raw keeps every event as the tills sent it;
-- this decides, once, which events count. `batch` is raw.pos_uploads for the upload day; `accepted_before` is the
-- accepted events of the dedup window's earlier upload days, from silver.pos_event_dispositions.
--   accepted   the first copy of a basket: it counts
--   resend     the same basket again (same basket_id, identical payload): counted here, never in revenue
--   collision  a known basket_id with a different payload: two baskets share a key; quarantined, and the control fails
WITH batch_hashed AS (
    SELECT event_id, basket_id, store_id, till_id, event_ts, uploaded_at, upload_date,
           tenders[1].amount_minor AS amount_minor,
           -- the payload without what changes per send: event_id (minted per send) and uploaded_at
           md5(concat_ws('|', store_id, till_id, basket_id, event_ts::VARCHAR, currency, loyalty_id,
                         lines::VARCHAR, tenders::VARCHAR)) AS payload_hash
    FROM batch
),
ranked AS (
    SELECT *,
           row_number() OVER w AS copy_no,
           first_value(event_id) OVER w AS first_in_batch,
           first_value(payload_hash) OVER w AS first_hash_in_batch
    FROM batch_hashed
    WINDOW w AS (PARTITION BY basket_id ORDER BY uploaded_at, event_id)
)
SELECT r.event_id, r.basket_id, r.store_id, r.till_id, left(r.store_id, 2) AS country,
       r.event_ts, r.uploaded_at, r.upload_date, r.amount_minor, r.payload_hash,
       CASE WHEN p.basket_id IS NULL AND r.copy_no = 1 THEN 'accepted'
            WHEN r.payload_hash = coalesce(p.payload_hash, r.first_hash_in_batch) THEN 'resend'
            ELSE 'collision' END AS disposition,
       CASE WHEN p.basket_id IS NULL AND r.copy_no = 1 THEN NULL
            ELSE coalesce(p.event_id, r.first_in_batch) END AS first_event_id
FROM ranked r
LEFT JOIN accepted_before p ON p.basket_id = r.basket_id
ORDER BY r.uploaded_at, r.event_id
