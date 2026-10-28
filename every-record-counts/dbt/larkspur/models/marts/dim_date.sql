{{ config(tags=['portable']) }}
-- One row per calendar day of 2026; date_key is yyyymmdd, and -1 is the "hasn't happened yet" row.
with days as (
    {{ dbt_utils.date_spine(datepart="day", start_date="cast('2026-01-01' as date)",
                            end_date="cast('2027-01-01' as date)") }}
),
d as (select cast(date_day as date) as d from days)
select cast({{ date_key('d') }} as integer)                                  as date_key,
       d                                                                     as "date",
       case {{ iso_day_of_week('d') }} when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday'
            when 4 then 'Thursday' when 5 then 'Friday' when 6 then 'Saturday' else 'Sunday' end as day_name,
       {{ iso_day_of_week('d') }} >= 6                                       as is_weekend,
       cast(date_trunc('week', d) as date)                                   as week_start,   -- ISO: Monday
       cast(extract(week from d) as integer)                                 as iso_week,
       cast(extract(year from d) as varchar) || '-' || lpad(cast(extract(month from d) as varchar), 2, '0') as "month",
       cast(extract(year from d) as varchar) || '-Q' || cast(extract(quarter from d) as varchar)          as quarter
from d
union all
select -1, null, 'Not yet', null, null, null, null, null
