{{ config(tags=['portable']) }}
-- One row per product per daily extract.
select snapshot_date, sku, product_id, product_name, category, subcategory, brand, is_chilled, supplier_id
from {{ source('raw', 'catalog_products') }}
