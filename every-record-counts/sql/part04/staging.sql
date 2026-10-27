-- Part 4, step 1: staging. Raw sources cleaned just enough to model (DuckDB).
-- Every table here is one row per business event, with the local business date next to the UTC timestamp.

CREATE OR REPLACE TABLE stg_country AS               -- each country's time zone, for online sales and refunds
SELECT DISTINCT country, timezone FROM raw_stores;

-- Sale lines from both channels. A till re-sends a basket with a new event_id: keep one per basket_id.
CREATE OR REPLACE TABLE stg_sales_line AS
WITH baskets AS (
    SELECT DISTINCT ON (basket_id) * FROM raw_pos_events ORDER BY basket_id, uploaded_at, event_id
)
SELECT 'pos'                                         AS channel,
       b.basket_id                                   AS transaction_id,
       l.line_no,
       b.store_id,
       s.country,
       b.currency,
       b.event_ts,
       (b.event_ts AT TIME ZONE s.timezone)::DATE    AS business_date,
       b.tenders[1].type                             AS tender_type,
       b.loyalty_id IS NOT NULL                      AS has_loyalty_card,
       l.sku, l.qty, l.unit_price_minor, l.discount_minor, l.promo_id,
       l.qty * l.unit_price_minor - l.discount_minor AS amount_minor
FROM baskets b, unnest(b.lines) AS t(l)
JOIN raw_stores s ON s.store_id = b.store_id
UNION ALL
SELECT o.channel,                                    -- 'web' or 'app'; deleted (cancelled) orders are gone
       o.order_id,
       l.line_no,
       o.country || '-ONLINE',
       o.country,
       o.currency,
       o.created_at,
       (o.created_at AT TIME ZONE c.timezone)::DATE,
       o.tender_type,
       false,
       l.sku, l.qty, l.unit_price_minor, l.discount_minor, l.promo_id, l.amount_minor
FROM raw_orders o
JOIN raw_order_lines l ON l.order_id = o.order_id
JOIN stg_country c ON c.country = o.country;

-- Refunds, with the timestamp of the purchase each one refunds.
CREATE OR REPLACE TABLE stg_refund AS
WITH purchases AS (
    SELECT DISTINCT transaction_id, event_ts AS purchased_at FROM stg_sales_line
)
SELECT r.refund_id,
       r.original_channel,
       r.original_reference,
       coalesce(r.store_id, r.country || '-ONLINE')         AS store_id,
       r.country,
       r.currency,
       r.tender_type,
       r.reason,
       r.amount_minor,
       r.refund_ts,
       (r.refund_ts AT TIME ZONE c.timezone)::DATE          AS refund_date,
       p.purchased_at,
       (p.purchased_at AT TIME ZONE c.timezone)::DATE       AS purchase_date
FROM raw_refunds r
JOIN stg_country c ON c.country = r.country
JOIN purchases p ON p.transaction_id = r.original_reference;

-- Supplier delivery rows that parse, one per PO line (the first file that reported it).
-- Part 15 ingests these files properly; here a malformed row is simply left out.
CREATE OR REPLACE TABLE stg_delivery_parsed AS
SELECT *, coalesce(po_number IS NOT NULL AND po_line IS NOT NULL AND delivered_qty >= 0
                   AND delivery_date IS NOT NULL, false) AS is_valid
FROM (SELECT supplier_id,
             file_date,
             nullif(po_number, '')                                   AS po_number,
             TRY_CAST(po_line AS INTEGER)                            AS po_line,
             sku,
             TRY_CAST(delivered_qty AS INTEGER)                      AS delivered_qty,
             coalesce(TRY_CAST(delivery_date AS DATE),               -- ISO dates, or DD/MM/YYYY from 7 suppliers
                      TRY_STRPTIME(delivery_date, '%d/%m/%Y')::DATE) AS delivery_date,
             dc_id
      FROM landed_supplier_deliveries);

CREATE OR REPLACE TABLE stg_delivery AS
SELECT DISTINCT ON (po_number, po_line) * EXCLUDE (is_valid)
FROM stg_delivery_parsed
WHERE is_valid
ORDER BY po_number, po_line, file_date;
