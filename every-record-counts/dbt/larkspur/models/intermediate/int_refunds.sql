-- Refunds dated by the refund (Part 4), with the timestamp of the purchase each one refunds.
with purchases as (
    select transaction_id, min(event_ts) as purchased_at
    from {{ ref('int_sales_lines') }}
    group by transaction_id
)
select r.refund_id, r.original_channel, r.original_reference, r.store_id, r.country, r.currency,
       r.tender_type, r.reason, r.amount_minor, r.refund_ts,
       {{ local_date('r.refund_ts', 'c.timezone') }}      as refund_date,
       p.purchased_at,
       {{ local_date('p.purchased_at', 'c.timezone') }}   as purchase_date
from {{ ref('stg_refunds') }} r
join {{ ref('stg_countries') }} c on c.country = r.country
join purchases p on p.transaction_id = r.original_reference
