-- Version 2 (Part 18): `currency` is renamed `report_currency`, and `is_provisional` marks the business days still
-- inside the lateness window: the last three days with sales, which a late till upload can still change (Part 2).
with sales as (
    select date_key, store_key, currency, sum(amount_minor) as amount_minor
    from {{ ref('fct_sales_line') }}
    group by transaction_id, date_key, store_key, currency
),
refunds as (
    select refund_date_key as date_key, store_key, currency, -refund_minor as amount_minor
    from {{ ref('fct_refund') }}
),
money as (
    select 'sale' as kind, * from sales
    union all
    select 'refund', * from refunds
),
latest as (
    select max(date_key) as date_key from sales
)
select s.country,
       d."date"                                                                        as business_date,
       sum(case when m.kind = 'sale' then {{ convert_minor('m.amount_minor', 'fx.rate') }} else 0 end)   as sales_minor,
       sum(case when m.kind = 'refund' then {{ convert_minor('m.amount_minor', 'fx.rate') }} else 0 end) as refunds_minor,
       sum({{ convert_minor('m.amount_minor', 'fx.rate') }})                            as net_minor,
       '{{ var("report_currency") }}'                                                  as report_currency,
       d."date" > (select date_add('day', -3, l."date") from latest x join {{ ref('dim_date') }} l on l.date_key = x.date_key)
                                                                                       as is_provisional
from money m
join {{ ref('dim_store') }} s on s.store_key = m.store_key
join {{ ref('dim_date') }} d on d.date_key = m.date_key
join {{ ref('dim_fx_rate') }} fx
    on fx."date" = d."date" and fx.base_currency = m.currency and fx.quote_currency = '{{ var("report_currency") }}'
group by s.country, d."date"
