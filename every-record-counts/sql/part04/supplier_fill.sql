-- Supplier fill rate, Q3 2026, by expected date, from the accumulating snapshot (DuckDB).
SELECT round(100 * sum(coalesce(f.delivered_qty, 0)) / sum(f.ordered_qty), 1)              AS fill_rate_pct,
       round(100 * avg(coalesce(f.delivered_qty = f.ordered_qty AND f.days_late <= 0, false)::INT), 1)
                                                                                           AS on_time_in_full_pct,
       count(*) FILTER (WHERE f.status = 'ordered')                                        AS lines_not_delivered
FROM fct_po_line f
JOIN dim_date e ON e.date_key = f.expected_date_key
WHERE e.quarter = '2026-Q3';

-- What the audit dimension recorded about that load.
SELECT table_name, sources, rows_loaded, rows_rejected, rows_duplicate FROM dim_audit WHERE table_name = 'fct_po_line';
