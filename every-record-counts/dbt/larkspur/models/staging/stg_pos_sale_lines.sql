-- POS sale lines, one per line of each basket. A till that reconnects re-sends a basket under a new event_id
-- (Part 6). Since Part 21, which events count is decided once, where data enters: `scripts.part21_reconcile accept`
-- gives every uploaded event a disposition in silver.pos_event_dispositions (accepted, resend, collision), and this
-- view keeps the accepted ones. Both scans are joined on event_ts too, so an incremental model's batch filter on
-- event_ts (Part 18) reaches the raw table and the dispositions alike.
with accepted as (
    select event_id, event_ts
    from {{ source('silver', 'pos_event_dispositions') }}
    where disposition = 'accepted'
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
join accepted c on c.event_id = b.event_id and c.event_ts = b.event_ts
cross join unnest(b.lines) as l (line_no, sku, qty, unit_price_minor, discount_minor, promo_id)
