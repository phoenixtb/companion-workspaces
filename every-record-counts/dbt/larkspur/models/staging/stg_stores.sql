{{ config(tags=['portable']) }}
select store_id, country, currency, timezone, city, format, wms_snapshot_schedule
from {{ source('raw', 'stores') }}
