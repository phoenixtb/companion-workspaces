-- Part 21: the daily control table's rows for one run (DuckDB). Variables: run_day (upload day D), first_day and
-- last_day (D-7 .. D-1: the closed UTC days, re-checked every run) and files_until (D+1: the newest PSP file that has
-- arrived when the run starts). scripts.part21_reconcile registers the inputs, already filtered to the window.
-- Each row compares a source with a target on records, money (minor units, presentment currency) and a hash total:
-- the sum of a hash of each record's key and amount. Equal counts and sums can hide a swapped record; hash totals can't.
-- rule: equal = source and target must match; zero = the target must be empty; info = recorded, never fails.
CREATE OR REPLACE TEMP MACRO h(k, amount) AS hash(k || ':' || amount::VARCHAR) % 1000000007;
CREATE OR REPLACE TEMP MACRO local_date(ts, country) AS
    timezone(CASE country WHEN 'GB' THEN 'Europe/London' WHEN 'IN' THEN 'Asia/Kolkata' ELSE 'Asia/Singapore' END, ts)::DATE;
CREATE OR REPLACE TEMP MACRO settles(d) AS          -- the PSP's contract: two business days after the local date
    d + CASE isodow(d) WHEN 4 THEN 4 WHEN 5 THEN 4 WHEN 6 THEN 4 WHEN 7 THEN 3 ELSE 2 END;

WITH grid AS (
    SELECT d::DATE AS d, country
    FROM generate_series(getvariable('first_day'), getvariable('last_day'), INTERVAL 1 DAY) t(d),
         (VALUES ('GB'), ('IN'), ('SG')) c(country)
),
ledger_sales AS (       -- payments not voided, by UTC day of authorisation (= the sale's event time)
    SELECT country, authorised_at::DATE AS d, 'S|' || reference AS k, amount_minor AS amount FROM ledger
    WHERE status <> 'voided'
),
gold_card_sales AS (    -- gold baskets and orders paid through the PSP (cash never reaches the ledger)
    SELECT s.country, b.event_ts::DATE AS d, 'S|' || b.transaction_id AS k, b.amount_minor AS amount
    FROM gold_sales b JOIN stores s USING (store_key) JOIN profiles p USING (profile_key)
    WHERE p.tender_type IN ('card', 'wallet', 'upi')
),
accepted_baskets AS (   -- what the acceptance step let in: every gold POS basket must be one of these, once
    SELECT country, event_ts::DATE AS d, 'S|' || basket_id AS k, amount_minor AS amount FROM disp_closed
),
gold_pos_baskets AS (   -- every gold till basket, whatever the tender
    SELECT s.country, b.event_ts::DATE AS d, 'S|' || b.transaction_id AS k, b.amount_minor AS amount
    FROM gold_sales b JOIN stores s USING (store_key)
    WHERE s.store_id NOT LIKE '%-ONLINE'
),
cdc_refund AS (
    SELECT country, refund_ts::DATE AS d, 'R|' || original_reference || '|' || epoch_ms(refund_ts) AS k,
           -amount_minor AS amount, tender_type, refund_ts AS ts
    FROM cdc_refunds
),
gold_refund AS (
    SELECT s.country, f.refund_ts::DATE AS d, 'R|' || f.transaction_id || '|' || epoch_ms(f.refund_ts) AS k,
           -f.refund_minor AS amount
    FROM gold_refunds f JOIN stores s USING (store_key)
),
to_settle AS (          -- what the PSP owes a row for, by UTC day of capture, and when its contract says it's due
    SELECT country, captured_at::DATE AS d, 'S|' || reference AS k, amount_minor AS amount,
           settles(local_date(captured_at, country)) AS due_on
    FROM ledger WHERE status = 'captured'
    UNION ALL
    SELECT country, d, k, amount, settles(local_date(ts, country)) FROM cdc_refund
    WHERE tender_type IN ('card', 'wallet', 'upi')
),
settled AS (
    SELECT country, transaction_ts::DATE AS d,
           CASE record_type WHEN 'SALE' THEN 'S|' || merchant_reference
                ELSE 'R|' || merchant_reference || '|' || epoch_ms(transaction_ts) END AS k,
           presentment_minor AS amount
    FROM psp WHERE record_type IN ('SALE', 'REFUND') AND settlement_date <= getvariable('files_until')
),
agg AS (
    SELECT 'ledger_sales' AS sys, country, d, count(*) AS n, sum(amount) AS minor, sum(h(k, amount)) AS hash_total
    FROM ledger_sales GROUP BY ALL
    UNION ALL SELECT 'gold_card_sales', country, d, count(*), sum(amount), sum(h(k, amount)) FROM gold_card_sales GROUP BY ALL
    UNION ALL SELECT 'accepted_baskets', country, d, count(*), sum(amount), sum(h(k, amount)) FROM accepted_baskets
              GROUP BY ALL
    UNION ALL SELECT 'gold_pos_baskets', country, d, count(*), sum(amount), sum(h(k, amount)) FROM gold_pos_baskets
              GROUP BY ALL
    UNION ALL SELECT 'cdc_refund', country, d, count(*), sum(amount), sum(h(k, amount)) FROM cdc_refund GROUP BY ALL
    UNION ALL SELECT 'gold_refund', country, d, count(*), sum(amount), sum(h(k, amount)) FROM gold_refund GROUP BY ALL
    UNION ALL SELECT 'to_settle', country, d, count(*), sum(amount), sum(h(k, amount)) FROM to_settle
              WHERE due_on <= getvariable('files_until') GROUP BY ALL
    UNION ALL SELECT 'settled', country, d, count(*), sum(amount), sum(h(k, amount)) FROM settled GROUP BY ALL
),
pending AS (            -- captured, but the PSP's file for it isn't due yet: timing, not a difference
    SELECT country, d, count(*) AS n FROM to_settle WHERE due_on > getvariable('files_until') GROUP BY ALL
),
pairs(check_name, source_sys, source_name, target_sys, target_name) AS (
    VALUES ('accepted_vs_gold_baskets', 'accepted_baskets', 'silver.pos_event_dispositions', 'gold_pos_baskets',
            'gold.fct_basket'),
           ('ledger_vs_gold_sales', 'ledger_sales', 'raw.payments', 'gold_card_sales', 'gold.fct_basket'),
           ('cdc_vs_gold_refunds', 'cdc_refund', 'raw.refunds', 'gold_refund', 'gold.fct_refund'),
           ('ledger_vs_psp', 'to_settle', 'raw.payments + raw.refunds', 'settled', 'raw.psp_settlements')
),
compared AS (
    SELECT c.check_name, g.d AS scope_date, g.country,
           c.source_name, s.n AS source_count, s.minor AS source_minor, s.hash_total AS source_hash,
           c.target_name, t.n AS target_count, t.minor AS target_minor, t.hash_total AS target_hash,
           CASE WHEN c.check_name = 'ledger_vs_psp' THEN coalesce(p.n, 0) ELSE 0 END AS pending, 'equal' AS rule
    FROM pairs c CROSS JOIN grid g
    LEFT JOIN agg s ON s.sys = c.source_sys AND s.country = g.country AND s.d = g.d
    LEFT JOIN agg t ON t.sys = c.target_sys AND t.country = g.country AND t.d = g.d
    LEFT JOIN pending p ON p.country = g.country AND p.d = g.d
    UNION ALL           -- landing: every event the tills sent in the window's upload days has exactly one disposition
    SELECT 'raw_vs_dispositions', upload_date, country, 'raw.pos_uploads', r.n, r.minor, r.hash_total,
           'silver.pos_event_dispositions', d.n, d.minor, d.hash_total, 0, 'equal'
    FROM (SELECT upload_date, left(store_id, 2) AS country, count(*) AS n, sum(tenders[1].amount_minor) AS minor,
                 sum(h(event_id, tenders[1].amount_minor)) AS hash_total FROM raw_window GROUP BY ALL) r
    FULL JOIN (SELECT upload_date, country, count(*) AS n, sum(amount_minor) AS minor,
                      sum(h(event_id, amount_minor)) AS hash_total
               FROM disp_all GROUP BY ALL) d USING (upload_date, country)
    UNION ALL
    SELECT 'resends', getvariable('run_day'), country, NULL, NULL, NULL, NULL, 'silver.pos_event_dispositions',
           count(*) FILTER (WHERE disposition = 'resend'), sum(amount_minor) FILTER (WHERE disposition = 'resend'),
           NULL, 0, 'info'
    FROM disp_day GROUP BY ALL
    UNION ALL
    SELECT 'collisions', getvariable('run_day'), country, NULL, NULL, NULL, NULL, 'silver.pos_event_dispositions',
           count(*) FILTER (WHERE disposition = 'collision'), NULL, NULL, 0, 'zero'
    FROM disp_day GROUP BY ALL
    UNION ALL           -- the key holds across the dedup window, not just inside one upload day
    SELECT 'accepted_twice', getvariable('run_day'), NULL, NULL, NULL, NULL, NULL, 'silver.pos_event_dispositions',
           count(*), NULL, NULL, 0, 'zero'
    FROM (SELECT basket_id FROM disp_window GROUP BY basket_id HAVING count(*) > 1)
    UNION ALL           -- the PSP's contract itself: a row settled on another day than two business days on is drift
    SELECT 'psp_settlement_lag', getvariable('run_day'), country, NULL, NULL, NULL, NULL, 'raw.psp_settlements',
           count(*) FILTER (WHERE settlement_date <> settles(local_date(transaction_ts, country))), NULL, NULL, 0, 'zero'
    FROM psp GROUP BY ALL
    UNION ALL           -- stock snapshots: rows and units, raw against gold, for the last closed business date
    SELECT 'raw_vs_gold_stock', getvariable('last_day'), r.country, 'raw.stock_snapshots', r.n, r.units, NULL,
           'gold.fct_stock_daily', g.n, g.units, NULL, 0, 'equal'
    FROM (SELECT left(store_id, 2) AS country, count(*) AS n, sum(on_hand_qty) AS units FROM stock_raw GROUP BY ALL) r
    FULL JOIN (SELECT s.country, count(*) AS n, sum(f.on_hand_qty) AS units
               FROM stock_gold f JOIN stores s USING (store_key) GROUP BY ALL) g USING (country)
    UNION ALL           -- clickstream: the day's file, as sent, against the landed table
    SELECT 'files_vs_raw_clicks', getvariable('last_day'), NULL, 'clickstream file', f.n, NULL, f.hash_total,
           'raw.clickstream_events', r.n, NULL, r.hash_total, 0, 'equal'
    FROM (SELECT count(*) AS n, sum(h(event_id, 0)) AS hash_total FROM click_file) f,
         (SELECT count(*) AS n, sum(h(event_id, 0)) AS hash_total FROM clicks_raw) r
)
SELECT *,
       CASE rule
           WHEN 'info' THEN 'info'
           WHEN 'zero' THEN CASE WHEN target_count = 0 THEN 'pass' ELSE 'FAIL' END
           WHEN 'equal' THEN CASE WHEN source_count IS NOT DISTINCT FROM target_count
                                       AND source_minor IS NOT DISTINCT FROM target_minor
                                       AND source_hash IS NOT DISTINCT FROM target_hash THEN 'pass' ELSE 'FAIL' END
       END AS status
FROM compared
ORDER BY check_name, scope_date, country
