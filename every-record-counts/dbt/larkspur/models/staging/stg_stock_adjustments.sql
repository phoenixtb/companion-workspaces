select business_date, store_id, sku, reason, qty_change
from {{ source('raw', 'stock_adjustments') }}
