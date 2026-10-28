-- Sale lines from both channels, each with the local business date of its store (or, online, of its country).
select p.channel, p.transaction_id, p.line_no, p.store_id, s.country, p.currency, p.event_ts,
       {{ local_date('p.event_ts', 's.timezone') }} as business_date,
       p.tender_type, p.has_loyalty_card, p.sku, p.qty, p.unit_price_minor, p.discount_minor, p.promo_id,
       p.amount_minor
from {{ ref('stg_pos_sale_lines') }} p
join {{ ref('stg_stores') }} s on s.store_id = p.store_id
union all
select o.channel, o.transaction_id, o.line_no, o.store_id, o.country, o.currency, o.event_ts,
       {{ local_date('o.event_ts', 'c.timezone') }},
       o.tender_type, o.has_loyalty_card, o.sku, o.qty, o.unit_price_minor, o.discount_minor, o.promo_id,
       o.amount_minor
from {{ ref('stg_online_sale_lines') }} o
join {{ ref('stg_countries') }} c on c.country = o.country
