-- The incremental replica against a full extract of the same API: what an updated_at cursor can miss.
-- {replica} is filled in by the script (raw.marketplace_sellers, or the no-lag experiment's copy).
WITH r AS (SELECT seller_id, updated_at,
                  md5(concat_ws('|', display_name, country, city, category, status, commission_bps,
                                payout_currency, created_at, updated_at)) AS h
           FROM {replica}),
f AS (SELECT seller_id, updated_at,
             md5(concat_ws('|', display_name, country, city, category, status, commission_bps,
                           payout_currency, created_at, updated_at)) AS h
      FROM full_extract),
j AS (SELECT seller_id,
             CASE WHEN f.seller_id IS NULL THEN 'gone from the API (deleted at source)'
                  WHEN r.seller_id IS NULL THEN 'missing from the replica'
                  WHEN r.h <> f.h AND r.updated_at <> f.updated_at THEN 'stale: missed a change, updated_at moved'
                  WHEN r.h <> f.h THEN 'stale: changed without touching updated_at'
             END AS finding
      FROM r FULL JOIN f USING (seller_id))
SELECT finding, count(*) AS sellers, string_agg(seller_id, ', ' ORDER BY seller_id) AS seller_ids
FROM j
WHERE finding IS NOT NULL
GROUP BY finding
ORDER BY finding;
