-- Transaction fact. Grain: one row per stock adjustment (damage, expiry, unknown loss).
select cast({{ date_key('x.business_date') }} as integer) as date_key, s.store_key, p.product_key, x.reason, x.qty_change
from {{ ref('stg_stock_adjustments') }} x
join {{ ref('dim_store') }} s on s.store_id = x.store_id
join {{ ref('dim_product') }} p
    on p.sku = x.sku and x.business_date >= p.valid_from and x.business_date < p.valid_to
