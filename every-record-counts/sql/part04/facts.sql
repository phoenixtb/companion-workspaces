-- Part 4, step 4: fact tables (DuckDB). Each one's grain is in the comment above it; tests/test_part04.py
-- checks every grain is unique. Dimension keys are looked up as of the event: the product version valid
-- that business date, the promotion version in force at that instant.

-- Audit dimension: one row per table load, so any fact row can say which run, from which sources, made it.
CREATE OR REPLACE TABLE dim_audit (
    audit_key     INTEGER,
    table_name    VARCHAR,
    sources       VARCHAR,
    raw_root      VARCHAR,
    loaded_at     TIMESTAMPTZ,
    rows_loaded    BIGINT,
    rows_rejected  BIGINT,  -- source rows that failed validation
    rows_duplicate BIGINT   -- valid source rows dropped as repeats
);
INSERT INTO dim_audit (audit_key, table_name, sources, raw_root, loaded_at) VALUES
    (1, 'fct_sales_line', 'pos/events, oltp/orders, oltp/order_lines', getvariable('raw_root'), now()),
    (2, 'fct_refund', 'oltp/refunds', getvariable('raw_root'), now()),
    (3, 'fct_stock_daily', 'inventory/snapshots', getvariable('raw_root'), now()),
    (4, 'fct_stock_adjustment', 'inventory/adjustments', getvariable('raw_root'), now()),
    (5, 'fct_po_line', 'procurement/purchase_orders, suppliers/deliveries', getvariable('raw_root'), now()),
    (6, 'fct_basket', 'fct_sales_line', getvariable('raw_root'), now());

-- Transaction fact. Grain: one row per line of a POS basket or an online order.
CREATE OR REPLACE TABLE fct_sales_line AS
SELECT d.date_key,
       s.store_key,
       p.product_key,
       coalesce(pr.promo_key, 0) AS promo_key,
       sp.profile_key,
       1                         AS audit_key,
       l.transaction_id,         -- degenerate dimension: basket or order number, no table behind it
       l.line_no,
       l.event_ts,
       l.currency,
       l.qty,                    -- additive
       l.unit_price_minor,       -- non-additive: the price the till charged, kept on the fact
       l.discount_minor,         -- additive
       l.amount_minor            -- additive
FROM stg_sales_line l
JOIN dim_date d ON d.date = l.business_date
JOIN dim_store s ON s.store_id = l.store_id
JOIN dim_product p ON p.sku = l.sku AND l.business_date >= p.valid_from AND l.business_date < p.valid_to
JOIN dim_sale_profile sp
    ON sp.channel = l.channel AND sp.tender_type = l.tender_type AND sp.has_loyalty_card = l.has_loyalty_card
ASOF LEFT JOIN dim_promotion pr ON pr.promo_id = l.promo_id AND l.event_ts >= pr.version_from;

-- The receipt. Grain: one row per basket or order. Right for basket questions (size, spend, savings),
-- wrong to join back to lines: its measures would repeat once per line.
CREATE OR REPLACE TABLE fct_basket AS
SELECT transaction_id,
       any_value(date_key)    AS date_key,
       any_value(store_key)   AS store_key,
       any_value(profile_key) AS profile_key,
       any_value(currency)    AS currency,
       6                      AS audit_key,
       count(*)               AS lines,
       sum(qty)               AS units,
       sum(discount_minor)    AS discount_minor,   -- "You saved" on the receipt
       sum(amount_minor)      AS amount_minor
FROM fct_sales_line
GROUP BY transaction_id;

-- Transaction fact. Grain: one row per refund, dated by the refund. The purchase date is a second,
-- role-playing date key; refunds have no product key because most refund a whole basket.
CREATE OR REPLACE TABLE fct_refund AS
SELECT rd.date_key AS refund_date_key,
       pd.date_key AS purchase_date_key,
       s.store_key,
       2           AS audit_key,
       r.refund_id,
       r.original_reference AS transaction_id,
       r.original_channel,
       r.reason,
       r.refund_ts,
       r.purchased_at,
       r.currency,
       r.amount_minor AS refund_minor
FROM stg_refund r
JOIN dim_date rd ON rd.date = r.refund_date
JOIN dim_date pd ON pd.date = r.purchase_date
JOIN dim_store s ON s.store_id = r.store_id;

-- Periodic snapshot fact. Grain: one row per store, product and business day. on_hand_qty is semi-additive:
-- add it across stores and products, never across days.
CREATE OR REPLACE TABLE fct_stock_daily AS
SELECT d.date_key, s.store_key, p.product_key, 3 AS audit_key,
       x.snapshot_ts, x.on_hand_qty, x.received_qty
FROM raw_stock_snapshots x
JOIN dim_date d ON d.date = x.business_date
JOIN dim_store s ON s.store_id = x.store_id
JOIN dim_product p ON p.sku = x.sku AND x.business_date >= p.valid_from AND x.business_date < p.valid_to;

-- Transaction fact. Grain: one row per stock adjustment (shrink: damage, expiry, unknown loss).
CREATE OR REPLACE TABLE fct_stock_adjustment AS
SELECT d.date_key, s.store_key, p.product_key, 4 AS audit_key, x.reason, x.qty_change
FROM raw_stock_adjustments x
JOIN dim_date d ON d.date = x.business_date
JOIN dim_store s ON s.store_id = x.store_id
JOIN dim_product p ON p.sku = x.sku AND x.business_date >= p.valid_from AND x.business_date < p.valid_to;

-- Accumulating snapshot fact. Grain: one row per purchase-order line, created when the line is ordered and
-- updated in place as later milestones happen. Three role-playing dates.
CREATE OR REPLACE TABLE fct_po_line AS
SELECT po.po_number, po.po_line,                  -- degenerate dimensions
       sup.supplier_key, p.product_key, po.dc_id,
       od.date_key           AS ordered_date_key,
       ed.date_key           AS expected_date_key,
       -1                    AS delivered_date_key, -- the "Not yet" row of dim_date
       5                     AS audit_key,
       po.ordered_qty,
       NULL::INTEGER         AS delivered_qty,
       NULL::INTEGER         AS days_late,
       'ordered'             AS status
FROM raw_po_lines po
JOIN dim_supplier sup ON sup.supplier_id = po.supplier_id
JOIN dim_product p ON p.sku = po.sku AND po.po_date >= p.valid_from AND po.po_date < p.valid_to
JOIN dim_date od ON od.date = po.po_date
JOIN dim_date ed ON ed.date = po.expected_date;
