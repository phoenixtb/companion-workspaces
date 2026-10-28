{{ config(tags=['portable']) }}
-- An FX rate for every calendar day the source has spoken for (Part 5): a weekend or holiday takes the latest
-- rate published on or before it. Facts then join on a plain date.
with rates as (
    select * from {{ ref('stg_fx_rates') }}
),
days as (
    select "date" from {{ ref('dim_date') }}
    where "date" between (select min(rate_date) from rates) and (select max(rate_date) from rates)
),
latest as (
    select d."date", r.base_currency, r.quote_currency, max(r.rate_date) as rate_date
    from days d
    join rates r on r.rate_date <= d."date"
    group by d."date", r.base_currency, r.quote_currency
)
select l."date", l.base_currency, l.quote_currency, l.rate_date, r.rate, l."date" <> l.rate_date as carried_forward
from latest l
join rates r
    on r.base_currency = l.base_currency and r.quote_currency = l.quote_currency and r.rate_date = l.rate_date
union all                                 -- the reporting currency converts to itself
select "date", '{{ var("report_currency") }}', '{{ var("report_currency") }}', "date",
       cast(1 as decimal(18, 8)), false
from days
