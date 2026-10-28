-- Online order lines. Orders come from the CDC replica, so cancelled (hard-deleted) orders are already gone.
select o.channel,                -- 'web' or 'app'
       o.order_id                 as transaction_id,
       l.line_no,
       o.country || '-ONLINE'     as store_id,
       o.country,
       o.currency,
       o.created_at               as event_ts,
       cast(null as timestamp(6) with time zone) as uploaded_at,   -- online orders arrive by CDC (Part 16), not uploads
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
{% if var('replay_now', none) %}
-- Lab only (Part 18's replay): the CDC replica already holds every order to 5 October; hide orders not yet placed at
-- the simulated run time, as the live replica would.
where o.created_at < timestamp '{{ var("replay_now") }} UTC'
{% endif %}
