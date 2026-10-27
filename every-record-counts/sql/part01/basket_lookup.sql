-- One basket, the kind of read a service does all day.
SELECT line_no, sku, qty, amount_minor
FROM {sales_line}
WHERE basket_id = 'GB-0042-20260815-00009'
ORDER BY line_no
