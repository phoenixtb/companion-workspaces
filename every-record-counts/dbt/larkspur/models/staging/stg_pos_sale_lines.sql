-- POS sale lines, one per line of each basket. A till that reconnects re-sends a basket under a new event_id
-- (Part 6): keep the first upload of each basket, drop the later copies, then unnest the lines. Only re-sent baskets
-- are ranked: ranking every event needs more memory than the lab's Trino node allows a query. A re-send repeats the
-- payload, event_ts included, so baskets are keyed by (basket_id, event_ts): a filter on event_ts (an incremental
-- model's batch, Part 18) then reaches every scan of the raw table, not just the outer one.
with resent as (
    select basket_id, event_ts
    from {{ source('raw', 'pos_uploads') }}
    group by basket_id, event_ts
    having count(*) > 1
),
later_copies as (
    select event_id, event_ts
    from (
        select r.event_id, r.event_ts,
               row_number() over (partition by r.basket_id, r.event_ts order by r.uploaded_at, r.event_id) as upload_rank
        from {{ source('raw', 'pos_uploads') }} r
        join resent d on d.basket_id = r.basket_id and d.event_ts = r.event_ts
    ) ranked
    where upload_rank > 1
)
select 'pos'                      as channel,
       b.basket_id                as transaction_id,
       l.line_no, b.store_id, b.currency, b.event_ts,
       b.uploaded_at,             -- ingestion time, kept apart from event time: when the till's upload arrived
       b.tenders[1]."type"        as tender_type,
       b.loyalty_id is not null   as has_loyalty_card,
       l.sku, l.qty, l.unit_price_minor, l.discount_minor, l.promo_id,
       l.qty * l.unit_price_minor - l.discount_minor as amount_minor
from {{ source('raw', 'pos_uploads') }} b
left join later_copies c on c.event_id = b.event_id and c.event_ts = b.event_ts
cross join unnest(b.lines) as l (line_no, sku, qty, unit_price_minor, discount_minor, promo_id)
where c.event_id is null
