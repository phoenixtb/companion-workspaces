select business_date, store_id, sku, snapshot_ts, on_hand_qty, received_qty
from {{ source('raw', 'stock_snapshots') }}
