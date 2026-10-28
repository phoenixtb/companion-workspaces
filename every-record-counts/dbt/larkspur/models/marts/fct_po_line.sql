-- Accumulating snapshot fact. Grain: one row per purchase-order line, with three role-playing dates. Part 4 updated
-- the row in place when the delivery milestone arrived; rebuilt from raw, the row is simply computed with it.
select po.po_number, po.po_line,
       sup.supplier_key, p.product_key, po.dc_id,
       cast({{ date_key('po.po_date') }} as integer)        as ordered_date_key,
       cast({{ date_key('po.expected_date') }} as integer)  as expected_date_key,
       coalesce(cast({{ date_key('d.delivery_date') }} as integer), -1) as delivered_date_key,  -- -1: not yet
       po.ordered_qty,
       d.delivered_qty,
       date_diff('day', po.expected_date, d.delivery_date)  as days_late,
       case when d.po_number is null then 'ordered' else 'delivered' end as status
from {{ ref('stg_po_lines') }} po
join {{ ref('dim_supplier') }} sup on sup.supplier_id = po.supplier_id
join {{ ref('dim_product') }} p on p.sku = po.sku and po.po_date >= p.valid_from and po.po_date < p.valid_to
left join {{ ref('stg_supplier_deliveries') }} d on d.po_number = po.po_number and d.po_line = po.po_line
