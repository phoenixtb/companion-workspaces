-- Q3 2026 discounts on GBP sales, by category: last quarter's discount report vs the line grain (DuckDB).
-- The report took each receipt's savings (fct_basket, one row per basket) and joined it to the basket's lines
-- to find a category. Every line of a basket then carried the whole basket's discount.
WITH report AS (
    SELECT p.category, sum(b.discount_minor) AS discount_minor
    FROM fct_basket b
    JOIN fct_sales_line l ON l.transaction_id = b.transaction_id   -- one basket row becomes n line rows
    JOIN dim_product p ON p.product_key = l.product_key
    JOIN dim_date d ON d.date_key = b.date_key
    WHERE d.quarter = '2026-Q3' AND b.currency = 'GBP'
    GROUP BY p.category
),
line_grain AS (                       -- the fact whose grain matches the question: one row per line
    SELECT p.category, sum(l.discount_minor) AS discount_minor
    FROM fct_sales_line l
    JOIN dim_product p ON p.product_key = l.product_key
    JOIN dim_date d ON d.date_key = l.date_key
    WHERE d.quarter = '2026-Q3' AND l.currency = 'GBP'
    GROUP BY p.category
)
SELECT category,
       major(r.discount_minor) AS report_gbp,
       major(g.discount_minor) AS line_grain_gbp,
       round(r.discount_minor / g.discount_minor, 2) AS overstated_x
FROM report r JOIN line_grain g USING (category)
ORDER BY line_grain_gbp DESC;

-- Totals: the report against the line fact and against the receipts themselves.
SELECT major((SELECT sum(b.discount_minor) FROM fct_basket b JOIN fct_sales_line l USING (transaction_id)
              JOIN dim_date d ON d.date_key = b.date_key
              WHERE d.quarter = '2026-Q3' AND b.currency = 'GBP'))  AS report_gbp,
       major((SELECT sum(l.discount_minor) FROM fct_sales_line l JOIN dim_date d USING (date_key)
              WHERE d.quarter = '2026-Q3' AND l.currency = 'GBP'))  AS line_grain_gbp,
       major((SELECT sum(b.discount_minor) FROM fct_basket b JOIN dim_date d USING (date_key)
              WHERE d.quarter = '2026-Q3' AND b.currency = 'GBP'))  AS receipts_gbp;
