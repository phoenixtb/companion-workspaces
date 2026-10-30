-- Part 32: features for recommendations, from the lakehouse (DuckDB). scripts.part32_models registers the inputs:
-- orders (raw.orders, Part 16's CDC replica, suppression list applied), order_lines (raw.order_lines) and
-- products (gold.dim_product, current rows). All times UTC.

-- name: order_categories
-- units per category in each fulfilled online order, at the order's event time
CREATE TABLE order_categories AS
SELECT o.customer_id, o.order_id, o.created_at AS event_ts, p.category, sum(l.qty) AS units
FROM orders o
JOIN order_lines l USING (order_id)
JOIN products p USING (sku)
WHERE o.status = 'fulfilled'
GROUP BY ALL;

-- name: customer_features
-- The feature table: one row per customer per order, holding the values as they stood right after that order.
-- feature_ts is event time. available_at is when the value can be served: the nightly job runs at 02:00 UTC and
-- publishes everything up to the previous midnight.
CREATE TABLE customer_features AS
WITH per_order AS (
    SELECT DISTINCT customer_id, order_id, event_ts FROM order_categories
), numbered AS (
    SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY event_ts, order_id) AS orders_so_far
    FROM per_order
), running AS (            -- each category's units up to and including each order
    SELECT o.customer_id, o.order_id, o.event_ts, c.category, sum(c.units) AS units
    FROM per_order o
    JOIN order_categories c ON c.customer_id = o.customer_id AND c.event_ts <= o.event_ts
    GROUP BY ALL
)
SELECT r.customer_id, r.order_id AS through_order, r.event_ts AS feature_ts,
       date_trunc('day', r.event_ts) + INTERVAL 26 HOUR AS available_at,
       n.orders_so_far, r.category AS favourite_category, r.units AS favourite_units
FROM running r
JOIN numbered n USING (customer_id, order_id)
QUALIFY row_number() OVER (PARTITION BY r.customer_id, r.order_id ORDER BY r.units DESC, r.category) = 1;

-- name: published
-- what the online store holds after each nightly run: the last value per customer per run
CREATE TABLE published AS
SELECT * FROM customer_features
QUALIFY row_number() OVER (PARTITION BY customer_id, available_at ORDER BY feature_ts DESC, through_order DESC) = 1;

-- name: examples
-- prediction moments: every repeat order. The label is the set of categories the order contained.
CREATE TABLE examples AS
SELECT customer_id, order_id, event_ts, list(DISTINCT category ORDER BY category) AS bought
FROM order_categories o
WHERE EXISTS (SELECT 1 FROM order_categories p WHERE p.customer_id = o.customer_id AND p.event_ts < o.event_ts)
GROUP BY ALL;

-- name: popularity
-- the fallback: the best-selling category in everything each nightly run has published
CREATE TABLE popularity AS
WITH daily AS (
    SELECT date_trunc('day', event_ts) + INTERVAL 26 HOUR AS available_at, category, sum(units) AS units
    FROM order_categories GROUP BY ALL
), cumulative AS (
    SELECT available_at, category, sum(units) OVER (PARTITION BY category ORDER BY available_at) AS units FROM daily
)
SELECT available_at, arg_max(category, units) AS top_category FROM cumulative GROUP BY available_at;

-- name: training
-- three ways to attach the favourite category to each example
CREATE TABLE training AS
WITH latest AS (           -- the naive way: join today's customer profile
    SELECT * FROM customer_features
    QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY feature_ts DESC, through_order DESC) = 1
)
SELECT e.customer_id, e.order_id, e.event_ts, e.bought,
       l.favourite_category AS fav_naive,
       fe.favourite_category AS fav_event_time,
       fa.favourite_category AS fav_available, fa.available_at,
       p.top_category AS popular
FROM examples e
JOIN latest l USING (customer_id)
ASOF LEFT JOIN customer_features fe ON e.customer_id = fe.customer_id AND e.event_ts > fe.feature_ts
ASOF LEFT JOIN published fa ON e.customer_id = fa.customer_id AND e.event_ts >= fa.available_at
ASOF LEFT JOIN popularity p ON e.event_ts >= p.available_at;

-- name: metrics
-- hit@1: the predicted category (the customer's favourite, else the most popular) is one the order contained
SELECT how, examples, with_feature, hit_at_1 FROM (
    SELECT 1 AS k, 'naive: today''s profile' AS how, count(*) AS examples, count(fav_naive) AS with_feature,
           avg(list_contains(bought, coalesce(fav_naive, popular))::int) AS hit_at_1 FROM training
    UNION ALL
    SELECT 2, 'as of event time (feature_ts < order)', count(*), count(fav_event_time),
           avg(list_contains(bought, coalesce(fav_event_time, popular))::int) FROM training
    UNION ALL
    SELECT 3, 'as of availability (available_at <= order)', count(*), count(fav_available),
           avg(list_contains(bought, coalesce(fav_available, popular))::int) FROM training
    UNION ALL
    SELECT 4, 'popularity only', count(*), count(popular), avg(list_contains(bought, popular)::int) FROM training
) ORDER BY k;

-- name: serve
-- the online path: the latest published value for one customer at one moment
SELECT favourite_category FROM published
WHERE customer_id = ? AND available_at <= ?
ORDER BY available_at DESC
LIMIT 1;
