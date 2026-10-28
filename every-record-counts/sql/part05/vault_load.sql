-- Part 5: load the Data Vault with everything that had landed by getvariable('load_until') (DuckDB).
-- Safe to re-run: hubs and links insert only keys they don't have, satellites only rows whose hash_diff
-- differs from the key's previous row. Nothing is ever updated or deleted.

-- Staging: each source's rows with their hash keys, load time and record source.
CREATE OR REPLACE TEMP TABLE stg_pos AS        -- a till re-send is the same basket again, uploaded later
SELECT md5(upper(trim(basket_id))) AS sale_hk, basket_id AS sale_id, store_id,
       uploaded_at AS load_ts, 'pos/events' AS record_source,
       event_ts, till_id, currency, tenders[1].amount_minor AS amount_minor
FROM raw_pos_events
WHERE uploaded_at <= getvariable('load_until');

CREATE OR REPLACE TEMP TABLE stg_order AS      -- the poller runs at 01:00 UTC; one row per order per run
SELECT md5(upper(trim(order_id))) AS sale_hk, order_id AS sale_id, country || '-ONLINE' AS store_id,
       extract_date + INTERVAL 1 HOUR AS load_ts, 'oltp/orders_poll' AS record_source,
       status, created_at, channel, currency, total_minor
FROM raw_orders_poll
WHERE extract_date + INTERVAL 1 HOUR <= getvariable('load_until')
QUALIFY row_number() OVER (PARTITION BY order_id, extract_date ORDER BY updated_at DESC) = 1;

CREATE OR REPLACE TEMP TABLE stg_refund AS     -- the refunds extract lands at 01:00 UTC on its snapshot date
SELECT md5(upper(trim(refund_id))) AS refund_hk, refund_id,
       md5(upper(trim(original_reference))) AS sale_hk, original_reference AS sale_id,
       snapshot_date + INTERVAL 1 HOUR AS load_ts, 'oltp/refunds' AS record_source,
       refund_ts, country, original_channel, currency, amount_minor, reason, tender_type
FROM raw_refunds
WHERE snapshot_date + INTERVAL 1 HOUR <= getvariable('load_until');

CREATE OR REPLACE TEMP TABLE stg_store AS      -- reference data, landed before the window opened
WITH stores AS (
    SELECT store_id, country, timezone, city, format FROM raw_stores
    UNION ALL
    SELECT DISTINCT country || '-ONLINE', country, timezone, NULL, 'online' FROM raw_stores
)
SELECT md5(upper(trim(store_id))) AS store_hk, store_id, TIMESTAMPTZ '2026-06-01 00:00:00+00' AS load_ts,
       'reference/stores' AS record_source, country, timezone, city, format
FROM stores
WHERE TIMESTAMPTZ '2026-06-01 00:00:00+00' <= getvariable('load_until');

-- Hubs: the first time any source named the key. A refund can name a sale before any sale source has.
INSERT INTO hub_sale
SELECT sale_hk, any_value(sale_id), min(load_ts), arg_min(record_source, load_ts)
FROM (SELECT sale_hk, sale_id, load_ts, record_source FROM stg_pos
      UNION ALL SELECT sale_hk, sale_id, load_ts, record_source FROM stg_order
      UNION ALL SELECT sale_hk, sale_id, load_ts, record_source FROM stg_refund) s
WHERE NOT EXISTS (SELECT 1 FROM hub_sale h WHERE h.sale_hk = s.sale_hk)
GROUP BY sale_hk;

INSERT INTO hub_refund
SELECT refund_hk, any_value(refund_id), min(load_ts), arg_min(record_source, load_ts)
FROM stg_refund s
WHERE NOT EXISTS (SELECT 1 FROM hub_refund h WHERE h.refund_hk = s.refund_hk)
GROUP BY refund_hk;

INSERT INTO hub_store
SELECT store_hk, any_value(store_id), min(load_ts), arg_min(record_source, load_ts)
FROM stg_store s
WHERE NOT EXISTS (SELECT 1 FROM hub_store h WHERE h.store_hk = s.store_hk)
GROUP BY store_hk;

-- Links: a relationship's hash key is the hash of its business keys together.
INSERT INTO lnk_refund_sale
SELECT md5(upper(trim(refund_id)) || '||' || upper(trim(sale_id))) AS refund_sale_hk,
       any_value(refund_hk), any_value(sale_hk), min(load_ts), arg_min(record_source, load_ts)
FROM stg_refund s
WHERE NOT EXISTS (SELECT 1 FROM lnk_refund_sale l
                  WHERE l.refund_sale_hk = md5(upper(trim(s.refund_id)) || '||' || upper(trim(s.sale_id))))
GROUP BY refund_sale_hk;

INSERT INTO lnk_sale_store
SELECT md5(upper(trim(sale_id)) || '||' || upper(trim(store_id))) AS sale_store_hk,
       any_value(sale_hk), md5(upper(trim(any_value(store_id)))), min(load_ts), arg_min(record_source, load_ts)
FROM (SELECT sale_hk, sale_id, store_id, load_ts, record_source FROM stg_pos
      UNION ALL SELECT sale_hk, sale_id, store_id, load_ts, record_source FROM stg_order) s
WHERE NOT EXISTS (SELECT 1 FROM lnk_sale_store l
                  WHERE l.sale_store_hk = md5(upper(trim(s.sale_id)) || '||' || upper(trim(s.store_id))))
GROUP BY sale_store_hk;

-- Satellites. Each one: hash the attributes, line the new rows up after the key's history, keep a new row only
-- where the hash changed. A row already loaded (same key and load_ts) is skipped, so a re-run inserts nothing.
INSERT INTO sat_sale_order
WITH new AS (
    SELECT *, md5(CAST((status, created_at, channel, currency, total_minor) AS VARCHAR)) AS hash_diff
    FROM stg_order s
    WHERE NOT EXISTS (SELECT 1 FROM sat_sale_order t WHERE t.sale_hk = s.sale_hk AND t.load_ts = s.load_ts)
),
history AS (
    SELECT sale_hk, load_ts, hash_diff, false AS is_new FROM sat_sale_order
    WHERE sale_hk IN (SELECT sale_hk FROM new)
    UNION ALL
    SELECT sale_hk, load_ts, hash_diff, true FROM new
),
changed AS (
    SELECT sale_hk, load_ts FROM history
    QUALIFY is_new AND hash_diff IS DISTINCT FROM lag(hash_diff) OVER (PARTITION BY sale_hk ORDER BY load_ts)
)
SELECT n.sale_hk, n.load_ts, n.hash_diff, n.record_source, n.status, n.created_at, n.channel, n.currency,
       n.total_minor
FROM new n JOIN changed USING (sale_hk, load_ts);

INSERT INTO sat_sale_pos
WITH new AS (
    SELECT *, md5(CAST((event_ts, till_id, currency, amount_minor) AS VARCHAR)) AS hash_diff
    FROM stg_pos s
    WHERE NOT EXISTS (SELECT 1 FROM sat_sale_pos t WHERE t.sale_hk = s.sale_hk AND t.load_ts = s.load_ts)
),
history AS (
    SELECT sale_hk, load_ts, hash_diff, false AS is_new FROM sat_sale_pos
    WHERE sale_hk IN (SELECT sale_hk FROM new)
    UNION ALL
    SELECT sale_hk, load_ts, hash_diff, true FROM new
),
changed AS (
    SELECT sale_hk, load_ts FROM history
    QUALIFY is_new AND hash_diff IS DISTINCT FROM lag(hash_diff) OVER (PARTITION BY sale_hk ORDER BY load_ts)
)
SELECT n.sale_hk, n.load_ts, n.hash_diff, n.record_source, n.event_ts, n.till_id, n.currency, n.amount_minor
FROM new n JOIN changed USING (sale_hk, load_ts);

INSERT INTO sat_refund
WITH new AS (
    SELECT *, md5(CAST((refund_ts, country, original_channel, currency, amount_minor, reason, tender_type)
                       AS VARCHAR)) AS hash_diff
    FROM stg_refund s
    WHERE NOT EXISTS (SELECT 1 FROM sat_refund t WHERE t.refund_hk = s.refund_hk AND t.load_ts = s.load_ts)
),
history AS (
    SELECT refund_hk, load_ts, hash_diff, false AS is_new FROM sat_refund
    WHERE refund_hk IN (SELECT refund_hk FROM new)
    UNION ALL
    SELECT refund_hk, load_ts, hash_diff, true FROM new
),
changed AS (
    SELECT refund_hk, load_ts FROM history
    QUALIFY is_new AND hash_diff IS DISTINCT FROM lag(hash_diff) OVER (PARTITION BY refund_hk ORDER BY load_ts)
)
SELECT n.refund_hk, n.load_ts, n.hash_diff, n.record_source, n.refund_ts, n.country, n.original_channel,
       n.currency, n.amount_minor, n.reason, n.tender_type
FROM new n JOIN changed USING (refund_hk, load_ts);

INSERT INTO sat_store
WITH new AS (
    SELECT *, md5(CAST((country, timezone, city, format) AS VARCHAR)) AS hash_diff
    FROM stg_store s
    WHERE NOT EXISTS (SELECT 1 FROM sat_store t WHERE t.store_hk = s.store_hk AND t.load_ts = s.load_ts)
),
history AS (
    SELECT store_hk, load_ts, hash_diff, false AS is_new FROM sat_store
    WHERE store_hk IN (SELECT store_hk FROM new)
    UNION ALL
    SELECT store_hk, load_ts, hash_diff, true FROM new
),
changed AS (
    SELECT store_hk, load_ts FROM history
    QUALIFY is_new AND hash_diff IS DISTINCT FROM lag(hash_diff) OVER (PARTITION BY store_hk ORDER BY load_ts)
)
SELECT n.store_hk, n.load_ts, n.hash_diff, n.record_source, n.country, n.timezone, n.city, n.format
FROM new n JOIN changed USING (store_hk, load_ts);
