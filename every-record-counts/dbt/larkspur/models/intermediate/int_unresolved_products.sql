-- Early-arriving facts (Part 4): rows naming a product on a day the catalogue doesn't list it yet.
with facts as (
    select 'sale line' as source, sku, business_date from {{ ref('int_sales_lines') }}
    union all select 'stock snapshot', sku, business_date from {{ ref('stg_stock_snapshots') }}
    union all select 'stock adjustment', sku, business_date from {{ ref('stg_stock_adjustments') }}
    union all select 'purchase order line', sku, po_date from {{ ref('stg_po_lines') }}
)
select f.source, f.sku, f.business_date
from facts f
where not exists (
    select 1 from {{ ref('int_product_versions') }} p
    where p.sku = f.sku and f.business_date >= p.valid_from and f.business_date < p.valid_to
)
