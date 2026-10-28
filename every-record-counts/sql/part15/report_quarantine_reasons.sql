-- Quarantined rows by reason, with one value as sent. `rows` counts repeats too; `distinct_rows` doesn't.
SELECT issue, count(*) AS rows, count(*) FILTER (WHERE copy_no = 1) AS distinct_rows,
       count(DISTINCT supplier_id) AS suppliers,
       min(CASE issue WHEN 'missing PO number' THEN '(empty)'
                      WHEN 'date not valid in the declared format' THEN delivery_date
                      ELSE delivered_qty END) AS example_as_sent
FROM delivery_quarantine
GROUP BY issue
ORDER BY rows DESC, issue;
