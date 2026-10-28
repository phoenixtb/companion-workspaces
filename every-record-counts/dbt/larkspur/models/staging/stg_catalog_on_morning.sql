-- The product catalogue as the 06:00 UTC run sees it on the morning of var('as_of'): that day's extract.
-- A snapshot of this view, run every morning, is how you'd keep history of a source that keeps none.
select sku, product_id, product_name, category, subcategory, brand, is_chilled, supplier_id,
       cast(snapshot_date as timestamp(6) with time zone) as extracted_at
from {{ source('raw', 'catalog_products') }}
where snapshot_date = date '{{ var("as_of") }}'
