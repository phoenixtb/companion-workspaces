select refund_id, original_channel, original_reference,
       coalesce(store_id, country || '-ONLINE') as store_id,
       country, currency, tender_type, reason, amount_minor, refund_ts
from {{ source('raw', 'refunds') }}
