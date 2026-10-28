-- Basket sizes over four years: ~28 million baskets aggregated first (a hash table keyed by basket_id), then
-- counted by country and lines per basket. The memory-hungry one.
SELECT country, lines_in_basket, count(*) AS baskets, sum(basket_minor) AS sales_minor
FROM (
    SELECT basket_id, country, count(*) AS lines_in_basket, sum(amount_minor) AS basket_minor
    FROM {table}
    GROUP BY basket_id, country
)
GROUP BY country, lines_in_basket
