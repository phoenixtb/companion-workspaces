select po_number, po_line, supplier_id, dc_id, sku, po_date, expected_date, ordered_qty
from {{ source('raw', 'po_lines') }}
