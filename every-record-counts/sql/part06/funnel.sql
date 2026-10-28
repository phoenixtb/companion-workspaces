-- Part 6: the conversion funnel, per session definition, each team with its own bot rule (DuckDB, Q3 2026).
-- UNPIVOT turns five step columns into rows; PIVOT turns the two definitions into columns.
WITH team_sessions AS (
    SELECT * FROM sessions WHERE definition = 'marketing' OR NOT is_bot
),
steps AS (
    UNPIVOT team_sessions
    ON page_view, product_view, add_to_cart, checkout_start, purchase
    INTO NAME step VALUE reached
),
funnel AS (
    SELECT definition, step,
           round(100 * count(*) FILTER (WHERE reached) / count(*), 2) AS pct_of_sessions
    FROM steps
    GROUP BY definition, step
)
PIVOT funnel ON definition USING any_value(pct_of_sessions) GROUP BY step
ORDER BY list_position(['page_view', 'product_view', 'add_to_cart', 'checkout_start', 'purchase'], step);

-- Strictly ordered: a session counts at a step only if it reached every earlier step first, in time order.
WITH ordered AS (
    SELECT s.definition, s.session_id,
           min(c.event_ts) FILTER (WHERE c.event_type = 'page_view')      AS t1,
           min(c.event_ts) FILTER (WHERE c.event_type = 'product_view')   AS t2,
           min(c.event_ts) FILTER (WHERE c.event_type = 'add_to_cart')    AS t3,
           min(c.event_ts) FILTER (WHERE c.event_type = 'checkout_start') AS t4,
           min(c.event_ts) FILTER (WHERE c.event_type = 'purchase')       AS t5
    FROM sessions s
    JOIN clicks c ON c.anonymous_id = s.anonymous_id AND c.event_ts BETWEEN s.started_at AND s.ended_at
    WHERE s.definition = 'product' AND NOT s.is_bot AND NOT c.is_retry
    GROUP BY ALL
)
SELECT count(*)                                        AS sessions,
       count(t1)                                       AS page_view,
       count(*) FILTER (WHERE t2 > t1)                 AS then_product_view,
       count(*) FILTER (WHERE t3 > t2 AND t2 > t1)     AS then_add_to_cart,
       count(*) FILTER (WHERE t4 > t3 AND t3 > t2 AND t2 > t1)             AS then_checkout,
       count(*) FILTER (WHERE t5 > t4 AND t4 > t3 AND t3 > t2 AND t2 > t1) AS then_purchase
FROM ordered;
