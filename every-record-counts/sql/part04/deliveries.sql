-- Part 4, step 6: the delivery milestone lands on the accumulating snapshot (DuckDB).
-- The same row changes: the delivered date, quantity and lateness fill in. No other fact table updates rows.
UPDATE fct_po_line AS f
SET delivered_date_key = dd.date_key,
    delivered_qty      = x.delivered_qty,
    days_late          = x.delivery_date - ed.date,
    status             = 'delivered'
FROM stg_delivery AS x, dim_date AS dd, dim_date AS ed
WHERE x.po_number = f.po_number AND x.po_line = f.po_line
  AND dd.date = x.delivery_date
  AND ed.date_key = f.expected_date_key;
