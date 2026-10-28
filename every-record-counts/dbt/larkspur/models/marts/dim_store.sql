{{ config(tags=['portable']) }}
-- Type 1 (the source keeps no history), plus one row per country for online sales.
select {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key, *
from (
    select store_id, 'store' as store_type, country, timezone, city, format, wms_snapshot_schedule
    from {{ ref('stg_stores') }}
    union all
    select country || '-ONLINE', 'online', country, timezone, null, 'online', null
    from {{ ref('stg_countries') }}
) s
