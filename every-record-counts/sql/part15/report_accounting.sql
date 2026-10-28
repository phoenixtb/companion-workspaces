-- Every landed row in exactly one bucket; the buckets add up to what landed.
SELECT what, rows FROM (
    SELECT 1 AS ord, 'landed in raw.supplier_deliveries' AS what, count(*) AS rows FROM delivery_classified
    UNION ALL SELECT 2, '  first copies, valid -> silver', count(*) FILTER (WHERE issue IS NULL AND copy_no = 1)
              FROM delivery_classified
    UNION ALL SELECT 3, '  first copies, quarantined', count(*) FILTER (WHERE issue IS NOT NULL AND copy_no = 1)
              FROM delivery_classified
    UNION ALL SELECT 4, '  repeats in the same file', count(*) FILTER (WHERE copy_no > 1 AND _source_file = first_file)
              FROM delivery_classified
    UNION ALL SELECT 5, '  repeats in a later file', count(*) FILTER (WHERE copy_no > 1 AND _source_file <> first_file)
              FROM delivery_classified
    UNION ALL SELECT 6, '  (repeats of quarantined rows)', count(*) FILTER (WHERE copy_no > 1 AND issue IS NOT NULL)
              FROM delivery_classified
    UNION ALL SELECT 7, 'valid PO lines delivered twice with different values',
              count(*) FROM (SELECT po_number, po_line FROM delivery_valid GROUP BY ALL HAVING count(*) > 1)
)
ORDER BY ord;
