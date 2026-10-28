-- Online order lines. Orders come from the CDC replica, so cancelled (hard-deleted) orders are already gone.
select o.channel,                -- 'web' or 'app'
       o.order_id                 as transaction_id,
       l.line_no,
       o.country || '-ONLINE'     as store_id,
       o.country,
       o.currency,
       o.created_at               as event_ts,
       o.tender_type,
       false                      as has_loyalty_card,
       l.sku,
       l.qty,
       l.unit_price_minor,
       l.discount_minor,
       l.promo_id,
       l.amount_minor
from {{ source('raw', 'orders') }} o
join {{ source('raw', 'order_lines') }} l on l.order_id = o.order_id
