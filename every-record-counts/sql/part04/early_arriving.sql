-- Part 4, step 3: early-arriving facts (DuckDB). Sales and stock rows can name a product the catalogue
-- extract doesn't list yet: products sell on launch day, and stores stock them before launch.
CREATE OR REPLACE TABLE stg_unresolved_product AS
WITH facts AS (
    SELECT 'sale line' AS source, sku, business_date FROM stg_sales_line
    UNION ALL SELECT 'stock snapshot', sku, business_date FROM raw_stock_snapshots
    UNION ALL SELECT 'stock adjustment', sku, business_date FROM raw_stock_adjustments
    UNION ALL SELECT 'purchase order line', sku, po_date FROM raw_po_lines
)
SELECT f.*
FROM facts f
ANTI JOIN dim_product p
    ON p.sku = f.sku AND f.business_date >= p.valid_from AND f.business_date < p.valid_to;

-- One placeholder row per unknown SKU (Kimball's late-arriving dimension row): a real surrogate key the facts
-- can point to now, attributes unknown, covering the days before the catalogue's first version.
INSERT INTO dim_product
SELECT (SELECT max(product_key) FROM dim_product) + row_number() OVER (ORDER BY u.sku),
       u.sku, NULL, 0, 'Unknown ' || u.sku, '(not yet in catalogue)', '(not yet in catalogue)', NULL, NULL, NULL,
       min(u.business_date),
       coalesce(min(p.valid_from), DATE '9999-12-31'),
       false,
       '(not yet in catalogue)', '(not yet in catalogue)',
       true
FROM stg_unresolved_product u
LEFT JOIN dim_product p ON p.sku = u.sku
GROUP BY u.sku;
