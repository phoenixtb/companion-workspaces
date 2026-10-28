-- Type 2 versions from the extracts plus Type 1 current_ columns (Type 6), and one placeholder per product that
-- facts named before the catalogue listed it (Part 4's early-arriving facts). A placeholder carries the
-- attributes of the product's first real version: Part 4 applied that as a Type 1 update once the catalogue caught
-- up; a model rebuilt from raw gets the same end state in one pass.
with versions as (
    select * from {{ ref('int_product_versions') }}
),
placeholders as (
    select u.sku,
           0                                          as version,
           min(u.business_date)                       as valid_from,
           coalesce(min(v.valid_from), date '9999-12-31') as valid_to
    from {{ ref('int_unresolved_products') }} u
    left join versions v on v.sku = u.sku
    group by u.sku
)
select {{ dbt_utils.generate_surrogate_key(['sku', 'valid_from']) }} as product_key,
       sku, product_id, version, product_name, category, subcategory, brand, is_chilled, supplier_id,
       valid_from, valid_to, is_current, current_category, current_subcategory, false as is_inferred
from versions
union all
select {{ dbt_utils.generate_surrogate_key(['p.sku', 'p.valid_from']) }},
       p.sku, v.product_id, 0, coalesce(v.product_name, 'Unknown ' || p.sku),
       coalesce(v.category, '(not yet in catalogue)'), coalesce(v.subcategory, '(not yet in catalogue)'),
       v.brand, v.is_chilled, v.supplier_id,
       p.valid_from, p.valid_to, false,
       coalesce(v.current_category, '(not yet in catalogue)'), coalesce(v.current_subcategory, '(not yet in catalogue)'),
       true
from placeholders p
left join versions v on v.sku = p.sku and v.version = 1
