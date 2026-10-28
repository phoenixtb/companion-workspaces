-- Every sale line that reached the intermediate layer must be in the fact, with the same money: an inner join to a
-- dimension with a gap (a product version, a store) would drop rows without any error, and so would an incremental
-- run that never reread a late upload (Part 18). Compared over the event times the fact holds, so a CI build of a
-- few days is checked too. Returns a row if anything is missing.
with bounds as (
    select min(event_ts) as lo, max(event_ts) as hi from {{ ref('fct_sales_line') }}
),
source_side as (
    select count(*) as line_rows, sum(amount_minor) as amount_minor
    from {{ ref('int_sales_lines') }} l cross join bounds b
    where l.event_ts between b.lo and b.hi
),
fact_side as (
    select count(*) as line_rows, sum(amount_minor) as amount_minor from {{ ref('fct_sales_line') }}
)
select s.line_rows as source_rows, f.line_rows as fact_rows, s.amount_minor as source_minor, f.amount_minor as fact_minor
from source_side s cross join fact_side f
where s.line_rows <> f.line_rows or s.amount_minor <> f.amount_minor
