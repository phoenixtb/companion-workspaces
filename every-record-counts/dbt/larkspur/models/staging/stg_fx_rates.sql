{{ config(tags=['portable']) }}
select rate_date, base_currency, quote_currency, cast(rate as decimal(18, 8)) as rate
from {{ source('raw', 'fx_rates') }}
