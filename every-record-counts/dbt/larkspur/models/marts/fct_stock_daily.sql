-- Periodic snapshot fact. Grain: one row per store, product and business day. on_hand_qty is semi-additive: add it
-- across stores and products, never across days. closing_on_hand_qty is stock at the close of the business day
-- (Part 5): a count taken after the day ended already includes the next morning's delivery, so take it back out.
select cast({{ date_key('x.business_date') }} as integer) as date_key,
       s.store_key,
       p.product_key,
       x.snapshot_ts,
       x.on_hand_qty,
       x.received_qty,
       case when {{ local_date('x.snapshot_ts', 's.timezone') }} > x.business_date
            then x.on_hand_qty - x.received_qty
            else x.on_hand_qty end                        as closing_on_hand_qty
from {{ ref('stg_stock_snapshots') }} x
join {{ ref('dim_store') }} s on s.store_id = x.store_id
join {{ ref('dim_product') }} p
    on p.sku = x.sku and x.business_date >= p.valid_from and x.business_date < p.valid_to
