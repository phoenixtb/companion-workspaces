select supplier_id, supplier_name, feed_format
from {{ source('raw', 'suppliers') }}
