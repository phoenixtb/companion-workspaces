-- Part 21: from what the PSP settled for Larkspur UK in Q3 2026 to gold net revenue, record by record (DuckDB).
-- Inputs, registered by scripts.part21_reconcile:
--   psp      raw.psp_settlements (the PSP's daily files, as landed)
--   ledger   raw.payments (the payment ledger, CDC replica of PostgreSQL shop.payments)
--   sales    gold.fct_basket, refunds  gold.fct_refund, stores  gold.dim_store, fx  gold.dim_fx_rate
-- Every gold record and every PSP row is joined on its key; each difference gets exactly one reason.

-- 1. Gold revenue records for GB, in GBP exactly as the report converts them (per transaction, business-date rate).
CREATE OR REPLACE TEMP TABLE rev AS
WITH r AS (
    SELECT 'SALE' AS kind, b.transaction_id AS ref, NULL::TIMESTAMPTZ AS ts, d.store_id,
           strptime(b.date_key::VARCHAR, '%Y%m%d')::DATE AS business_date, b.currency, b.amount_minor AS amount
    FROM sales b JOIN stores d USING (store_key) WHERE d.country = 'GB'
    UNION ALL
    SELECT 'REFUND', f.transaction_id, f.refund_ts, d.store_id,
           strptime(f.refund_date_key::VARCHAR, '%Y%m%d')::DATE, f.currency, -f.refund_minor
    FROM refunds f JOIN stores d USING (store_key) WHERE d.country = 'GB'
)
SELECT r.*, CAST(sign(r.amount) * round(abs(r.amount) * x.rate) AS BIGINT) AS gbp
FROM r JOIN fx x ON x."date" = r.business_date AND x.base_currency = r.currency AND x.quote_currency = 'GBP';

-- 2. Pair them with the PSP's rows (sales by merchant reference; refunds by reference and refund time) and the ledger.
CREATE OR REPLACE TEMP TABLE pairs AS
SELECT coalesce(r.kind, p.record_type) AS kind, coalesce(r.ref, p.merchant_reference) AS ref,
       r.store_id LIKE '%-ONLINE' AS online, r.business_date, r.currency, r.amount, r.gbp,
       p.settlement_date, p.presentment_minor, p.gross_minor,
       (l.captured_at AT TIME ZONE 'Europe/London')::DATE AS captured_date, l.status AS ledger_status,
       r.business_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30' AS in_revenue,
       p.settlement_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30' AS in_settled
FROM rev r
FULL JOIN (SELECT * FROM psp WHERE country = 'GB' AND record_type IN ('SALE', 'REFUND')) p
       ON p.record_type = r.kind AND p.merchant_reference = r.ref
      AND (r.kind = 'SALE' OR p.transaction_ts = r.ts)   -- a refund is its sale's reference at the refund's time
LEFT JOIN ledger l ON r.kind = 'SALE' AND l.reference = r.ref AND l.country = 'GB';

-- 3. The bridge. Each line is one reason; legitimate differences first, then anything that is a bug.
WITH q3_psp AS (
    SELECT * FROM psp WHERE country = 'GB' AND settlement_date BETWEEN DATE '2026-07-01' AND DATE '2026-09-30'
),
reasons AS (
    SELECT CASE
             WHEN in_revenue AND in_settled AND amount = presentment_minor AND currency = 'EUR'
                  THEN '08 FX: euros at the capture date''s rate (PSP), the business date''s (gold)'
             WHEN in_revenue AND in_settled AND amount = presentment_minor AND gbp = gross_minor
                  THEN '09 matched to the penny (pounds)'
             WHEN in_revenue AND in_settled THEN '90 BUG: gold and PSP amounts differ'
             WHEN in_settled AND business_date IS NULL THEN '91 BUG: settled by the PSP, missing from gold'
             WHEN in_settled AND online AND kind = 'SALE' THEN '04 timing: June orders, captured in Q3'
             WHEN in_settled THEN '03 timing: June business days, settled in July (settlement lag)'
             WHEN online AND kind = 'SALE' AND captured_date > DATE '2026-09-30'
                  THEN '06 timing: Q3 orders, captured after 30 September'
             WHEN settlement_date IS NOT NULL THEN '05 timing: Q3 business days, settled in October (settlement lag)'
             WHEN ledger_status = 'authorised' THEN '07 timing: Q3 orders, not captured by 5 October'
             WHEN ledger_status = 'captured' THEN '92 BUG: captured in Q3, PSP row overdue'
             ELSE '93 BUG: in gold, no PSP or ledger record'
           END AS reason,
           CASE WHEN in_revenue AND in_settled THEN gbp - gross_minor
                WHEN in_revenue THEN gbp
                ELSE -gross_minor END AS minor,
           1 AS records
    FROM pairs
    WHERE in_revenue OR in_settled
)
SELECT '00 PSP net settled in Q3 (settlement date)' AS line, sum(net_minor)::BIGINT AS minor, count(*) AS records FROM q3_psp
UNION ALL
SELECT '01 + PSP fees: a cost of sale, not less revenue', sum(fee_minor)::BIGINT, count(*) FILTER (WHERE fee_minor <> 0) FROM q3_psp
UNION ALL
SELECT '02 + chargebacks: disputes, booked as losses, not refunds', -sum(gross_minor)::BIGINT, count(*)
FROM q3_psp WHERE record_type = 'CHARGEBACK'
UNION ALL
SELECT reason, sum(minor)::BIGINT, sum(records) FROM reasons GROUP BY reason
ORDER BY line
