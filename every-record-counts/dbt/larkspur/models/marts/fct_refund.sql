-- Transaction fact. Grain: one row per refund, dated by the refund; the purchase date is a role-playing date key.
select cast({{ date_key('r.refund_date') }} as integer)   as refund_date_key,
       cast({{ date_key('r.purchase_date') }} as integer) as purchase_date_key,
       s.store_key,
       r.refund_id,
       r.original_reference as transaction_id,
       r.original_channel,
       r.reason,
       r.refund_ts,
       r.purchased_at,
       r.currency,
       r.amount_minor       as refund_minor
from {{ ref('int_refunds') }} r
join {{ ref('dim_store') }} s on s.store_id = r.store_id
