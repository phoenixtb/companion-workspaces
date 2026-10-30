-- Part 29: opening stock per store and SKU on the morning of business day D (= getvariable('day')), DuckDB.
-- Opening(D) = closing(D-1) + the delivery put away before trading on D. Part 5's rule decides which snapshot holds
-- that delivery: a snapshot taken after its business day ended (store's own time zone) already includes the next
-- morning's delivery in on_hand_qty; otherwise the delivery is the next snapshot's received_qty.
CREATE OR REPLACE VIEW opening_stock AS
WITH snap AS (
    SELECT k.*, (k.snapshot_ts AT TIME ZONE s.timezone)::DATE > k.business_date AS after_close
    FROM raw_stock_snapshots k JOIN raw_stores s USING (store_id)
)
SELECT y.store_id, y.sku,
       CASE WHEN y.after_close THEN y.on_hand_qty               -- closing + the morning delivery, already in it
            ELSE y.on_hand_qty + t.received_qty END AS opening_qty
FROM snap y
JOIN snap t ON t.store_id = y.store_id AND t.sku = y.sku AND t.business_date = getvariable('day')::DATE
WHERE y.business_date = getvariable('day')::DATE - 1;

SELECT count(*) AS store_skus, sum(opening_qty) AS units, count(*) FILTER (WHERE opening_qty <= 0) AS opening_at_zero
FROM opening_stock;
