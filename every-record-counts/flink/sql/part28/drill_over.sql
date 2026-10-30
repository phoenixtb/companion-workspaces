-- Part 28, Design Drill: a per-event distinct count over the last 10 minutes is an OVER window. Here tills per store,
-- standing in for cards per device. EXPLAIN only: Flink 2.2.1 accepts COUNT(DISTINCT) in a RANGE OVER window.
SET 'execution.runtime-mode' = 'streaming';
EXPLAIN CHANGELOG_MODE
SELECT store_id, event_ts,
       COUNT(DISTINCT till_id) OVER (PARTITION BY store_id ORDER BY event_ts
                                     RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) AS tills_10m
FROM pos_basket_events;
