-- The product dimension: daily catalogue extracts compressed into Type 2 rows, plus placeholders (DuckDB).
SELECT (SELECT count(*) FROM raw_catalog_products)                AS extract_rows,
       count(*) FILTER (WHERE NOT is_inferred)                     AS type_2_rows,
       count(*) FILTER (WHERE is_inferred)                         AS placeholders,
       count(DISTINCT sku) FILTER (WHERE version = 2)              AS products_moved
FROM dim_product;

-- One product the 1 August reorganisation moved: Type 2 rows, each carrying today's category (Type 6).
SELECT product_key, sku, category, current_category, valid_from, valid_to
FROM dim_product
WHERE sku = (SELECT min(sku) FROM dim_product WHERE version = 1 AND category <> current_category)
ORDER BY version;

-- Units sold, 1 June to 4 October, by category: as the category was on the day of sale (the Type 2 row)
-- vs restated to today's categories (the Type 1 column). Only categories where the two differ.
WITH units AS (
    SELECT p.category, p.current_category, f.qty
    FROM fct_sales_line f JOIN dim_product p ON p.product_key = f.product_key
),
as_sold AS (SELECT category, sum(qty) AS units_as_sold FROM units GROUP BY category),
restated AS (SELECT current_category AS category, sum(qty) AS units_restated FROM units GROUP BY current_category)
SELECT category, units_as_sold, units_restated, units_restated - units_as_sold AS difference
FROM as_sold JOIN restated USING (category)
WHERE units_restated <> units_as_sold
ORDER BY difference;
