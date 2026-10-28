-- One delivery per purchase-order line: the first file that reported it (Part 15 already removed bad rows).
with ranked as (
    select *, row_number() over (partition by po_number, po_line order by file_date, source_file, source_row) as rn
    from {{ source('silver', 'supplier_deliveries') }}
)
select po_number, po_line, supplier_id, sku, delivered_qty, delivery_date, dc_id, file_date
from ranked
where rn = 1
