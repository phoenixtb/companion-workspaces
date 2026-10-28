{#- The sales fact's SELECT, shared by fct_sales_line and Part 18's lab variants (models/lab18), which differ only in
    how they load. Dimension keys are looked up as of the event: the product version valid that business date, the
    promotion version in force at that instant. -#}
{% macro sales_line_fact() %}
select cast({{ date_key('l.business_date') }} as integer) as date_key,
       s.store_key,
       p.product_key,
       coalesce(pr.promo_key, 'none')       as promo_key,
       sp.profile_key,
       l.transaction_id,                    -- degenerate dimension: basket or order number
       l.line_no,
       l.event_ts,                          -- event time: when the till rang it up
       l.uploaded_at,                       -- ingestion time: when the upload arrived (POS only)
       l.currency,
       l.qty,                               -- additive
       l.unit_price_minor,                  -- non-additive: the price the till charged
       l.discount_minor,                    -- additive
       l.amount_minor                       -- additive
from {{ ref('int_sales_lines') }} l
join {{ ref('dim_store') }} s on s.store_id = l.store_id
join {{ ref('dim_product') }} p on p.sku = l.sku and l.business_date >= p.valid_from and l.business_date < p.valid_to
join {{ ref('dim_sale_profile') }} sp
    on sp.channel = l.channel and sp.tender_type = l.tender_type and sp.has_loyalty_card = l.has_loyalty_card
left join {{ ref('dim_promotion') }} pr
    on pr.promo_id = l.promo_id and l.event_ts >= pr.version_from and l.event_ts < pr.version_to
{% endmacro %}
