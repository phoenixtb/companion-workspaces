{{ config(tags=['portable']) }}
-- Type 2 versions of each product, derived from the daily catalogue extracts (Part 4). The extracts keep every day,
-- so this history can be rebuilt from raw at any time: a model, not a snapshot.
with daily as (
    select *,
           {{ dbt_utils.generate_surrogate_key(['product_name', 'category', 'subcategory', 'brand', 'is_chilled',
                                                'supplier_id']) }} as attrs          -- NULLs hash too
    from {{ ref('stg_catalog_products') }}
),
flagged as (
    select *, case when attrs = lag(attrs) over (partition by sku order by snapshot_date) then 0 else 1 end as changed
    from daily
),
numbered as (
    select *, sum(changed) over (partition by sku order by snapshot_date
                                 rows between unbounded preceding and current row) as version
    from flagged
),
versions as (
    select sku, version, max(product_id) as product_id, max(product_name) as product_name,
           max(category) as category, max(subcategory) as subcategory, max(brand) as brand,
           bool_or(is_chilled) as is_chilled, max(supplier_id) as supplier_id, min(snapshot_date) as valid_from
    from numbered
    group by sku, version
)
select sku, product_id, version, product_name, category, subcategory, brand, is_chilled, supplier_id,
       valid_from,                                                                     -- inclusive
       coalesce(lead(valid_from) over (partition by sku order by version), date '9999-12-31') as valid_to,  -- exclusive
       lead(valid_from) over (partition by sku order by version) is null              as is_current,
       last_value(category) over (partition by sku order by version
                                  rows between unbounded preceding and unbounded following) as current_category,
       last_value(subcategory) over (partition by sku order by version
                                     rows between unbounded preceding and unbounded following) as current_subcategory
from versions
