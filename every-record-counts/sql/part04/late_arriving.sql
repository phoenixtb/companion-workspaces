-- Part 4, step 5: the catalogue catches up (DuckDB). Each placeholder takes the attributes of the product's
-- first real version, in place (Type 1). Its surrogate key doesn't change, so no fact row has to be touched.
UPDATE dim_product AS ph
SET product_id = v.product_id, product_name = v.product_name, category = v.category,
    subcategory = v.subcategory, brand = v.brand, is_chilled = v.is_chilled, supplier_id = v.supplier_id,
    current_category = v.current_category, current_subcategory = v.current_subcategory
FROM dim_product AS v
WHERE ph.is_inferred AND v.sku = ph.sku AND v.version = 1;
