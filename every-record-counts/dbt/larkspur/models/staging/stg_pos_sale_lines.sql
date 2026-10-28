-- POS sale lines, one per line of each basket. A till that reconnects re-sends a basket under a new event_id
-- (Part 6): keep the first upload of each basket_id, then unnest its lines. Rank the keys alone, then join back:
-- ranking whole events (nested lines and all) needs more memory than the lab's Trino node allows a query.
with first_upload as (
    select event_id
    from (
        select event_id,
               row_number() over (partition by basket_id order by uploaded_at, event_id) as upload_rank
        from {{ source('raw', 'pos_uploads') }}
    ) ranked
    where upload_rank = 1
)
select 'pos'                      as channel,
       b.basket_id                as transaction_id,
       l.line_no, b.store_id, b.currency, b.event_ts,
       b.tenders[1]."type"        as tender_type,
       b.loyalty_id is not null   as has_loyalty_card,
       l.sku, l.qty, l.unit_price_minor, l.discount_minor, l.promo_id,
       l.qty * l.unit_price_minor - l.discount_minor as amount_minor
from {{ source('raw', 'pos_uploads') }} b
join first_upload f on f.event_id = b.event_id
cross join unnest(b.lines) as l (line_no, sku, qty, unit_price_minor, discount_minor, promo_id)
