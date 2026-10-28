-- Part 5: stock on hand measured at one instant everywhere, the close of the business day (DuckDB).
-- A snapshot taken after the business day ended (in the store's own time zone) already includes the next
-- morning's delivery; take it back out. The rule reads the timestamps, not a list of stores, so it still holds
-- when a store's WMS schedule changes.
ALTER TABLE fct_stock_daily ADD COLUMN closing_on_hand_qty BIGINT;

UPDATE fct_stock_daily f
SET closing_on_hand_qty = CASE WHEN (f.snapshot_ts AT TIME ZONE s.timezone)::DATE > d.date
                               THEN f.on_hand_qty - f.received_qty
                               ELSE f.on_hand_qty END
FROM dim_store s, dim_date d
WHERE s.store_key = f.store_key AND d.date_key = f.date_key;
