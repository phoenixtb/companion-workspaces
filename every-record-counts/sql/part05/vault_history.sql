-- Part 5: what a satellite keeps (DuckDB). The order poller's nightly extracts, one row per order per run,
-- against the satellite rows they became: one per change of status.
SELECT (SELECT count(*) FROM raw_orders_poll)                       AS extract_rows,
       (SELECT count(DISTINCT extract_date) FROM raw_orders_poll)  AS extracts,
       count(*)                                                     AS satellite_rows,
       count(DISTINCT sale_hk)                                      AS orders
FROM sat_sale_order;

-- Each order's status history, as the satellite tells it.
SELECT history, count(*) AS orders
FROM (SELECT sale_hk, string_agg(status, ' -> ' ORDER BY load_ts) AS history FROM sat_sale_order GROUP BY sale_hk)
GROUP BY history
ORDER BY orders DESC;
