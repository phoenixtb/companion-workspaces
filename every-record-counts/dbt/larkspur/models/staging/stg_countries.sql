{{ config(tags=['portable']) }}
-- Each country's time zone, for online sales and refunds (they have no store).
select distinct country, timezone
from {{ ref('stg_stores') }}
