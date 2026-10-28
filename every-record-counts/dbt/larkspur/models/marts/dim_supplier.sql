select {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_key, supplier_id, supplier_name, feed_format
from {{ ref('stg_suppliers') }}
