-- Part 24: rpt_daily_net_revenue_v2 built incrementally (enabled with --vars '{part24_lab: true, ...}', dev only).
-- The daily run rebuilds the sales fact for UTC days run_start .. run_end - 1 (Part 19); in London, Mumbai and
-- Singapore those are business dates run_start .. run_end. Refunds are dated by the refund (Part 4), so a run's new
-- refunds land on those dates too. So a run re-derives business dates from run_start - 3 days (the three days
-- `is_provisional` can flip) to run_end, delete+insert by (country, business_date); every other row stays as it was.
{{ config(
    enabled=var('part24_lab', false),
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['country', 'business_date'],
) }}
with window_ as (
    select cast(format_datetime(date_add('day', -3, date '{{ var("run_start") }}'), 'yyyyMMdd') as integer) as lo,
           cast(format_datetime(date '{{ var("run_end") }}', 'yyyyMMdd') as integer)                         as hi
),
sales as (
    select date_key, store_key, currency, sum(amount_minor) as amount_minor
    from {{ ref('fct_sales_line') }}
    {% if is_incremental() %}where date_key between (select lo from window_) and (select hi from window_){% endif %}
    group by transaction_id, date_key, store_key, currency
),
refunds as (
    select refund_date_key as date_key, store_key, currency, -refund_minor as amount_minor
    from {{ ref('fct_refund') }}
    {% if is_incremental() %}where refund_date_key between (select lo from window_) and (select hi from window_){% endif %}
),
money as (
    select 'sale' as kind, * from sales
    union all
    select 'refund', * from refunds
),
latest as (  -- the newest business date with sales; in a run, also what the table already holds (a backfill of an
              -- older day must not mark its rows provisional)
    select max(d) as "date" from (
        select max(dd."date") as d from sales x join {{ ref('dim_date') }} dd on dd.date_key = x.date_key
        {% if is_incremental() %}union all select max(business_date) from {{ this }}{% endif %}
    )
)
select s.country,
       d."date"                                                                        as business_date,
       sum(case when m.kind = 'sale' then {{ convert_minor('m.amount_minor', 'fx.rate') }} else 0 end)   as sales_minor,
       sum(case when m.kind = 'refund' then {{ convert_minor('m.amount_minor', 'fx.rate') }} else 0 end) as refunds_minor,
       sum({{ convert_minor('m.amount_minor', 'fx.rate') }})                            as net_minor,
       '{{ var("report_currency") }}'                                                  as report_currency,
       d."date" > (select date_add('day', -3, "date") from latest)                  as is_provisional
from money m
join {{ ref('dim_store') }} s on s.store_key = m.store_key
join {{ ref('dim_date') }} d on d.date_key = m.date_key
join {{ ref('dim_fx_rate') }} fx
    on fx."date" = d."date" and fx.base_currency = m.currency and fx.quote_currency = '{{ var("report_currency") }}'
group by s.country, d."date"
