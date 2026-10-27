-- Prices change mid-day (389 changes at 13:00 local). Which price list explains what the till charged? (DuckDB)
-- EUR lines are left out: euro prices come from a separate list.
WITH lines AS (
    SELECT f.unit_price_minor, f.event_ts, p.product_id, s.country,
           d.date::TIMESTAMP AT TIME ZONE s.timezone AS local_midnight
    FROM fct_sales_line f
    JOIN dim_product p ON p.product_key = f.product_key
    JOIN dim_store s ON s.store_key = f.store_key
    JOIN dim_date d ON d.date_key = f.date_key
    WHERE f.currency <> 'EUR'
),
at_sale AS (                  -- the price in force at the instant of the sale
    SELECT l.unit_price_minor, pc.price_minor
    FROM lines l ASOF JOIN raw_price_changes pc
        ON pc.product_id = l.product_id AND pc.country = l.country AND l.event_ts >= pc.effective_from
),
daily AS (                    -- one price per product per day, taken at local midnight
    SELECT l.unit_price_minor, pc.price_minor
    FROM lines l ASOF JOIN raw_price_changes pc
        ON pc.product_id = l.product_id AND pc.country = l.country AND l.local_midnight >= pc.effective_from
)
SELECT 'price at the time of sale' AS price_list, count(*) AS lines,
       count(*) FILTER (WHERE price_minor <> unit_price_minor) AS wrong_price
FROM at_sale
UNION ALL
SELECT 'daily price, at local midnight', count(*), count(*) FILTER (WHERE price_minor <> unit_price_minor)
FROM daily;
