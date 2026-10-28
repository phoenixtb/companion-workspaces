-- Version 1 (Part 17), kept under its old table name until its deprecation date (see _marts.yml).
-- Net revenue per country and local business date, in the reporting currency at the rate of that date (Part 5):
-- each basket's or order's total converted once, as the payment provider does; refunds on their own date.
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
)
select s.country,
       d."date"                                                                        as business_date,
       sum(case when m.kind = 'sale' then {{ convert_minor('m.amount_minor', 'fx.rate') }} else 0 end)   as sales_minor,
       sum(case when m.kind = 'refund' then {{ convert_minor('m.amount_minor', 'fx.rate') }} else 0 end) as refunds_minor,
       sum({{ convert_minor('m.amount_minor', 'fx.rate') }})                            as net_minor,
       '{{ var("report_currency") }}'                                                  as currency
from money m
join {{ ref('dim_store') }} s on s.store_key = m.store_key
join {{ ref('dim_date') }} d on d.date_key = m.date_key
join {{ ref('dim_fx_rate') }} fx
    on fx."date" = d."date" and fx.base_currency = m.currency and fx.quote_currency = '{{ var("report_currency") }}'
group by s.country, d."date"
