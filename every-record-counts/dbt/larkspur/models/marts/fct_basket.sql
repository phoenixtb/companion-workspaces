-- The receipt. Grain: one row per basket or order. Never join it back to lines: its measures would repeat.
-- Totals are aggregated by transaction alone and the descriptive keys taken from line 1: grouping by four 32-character
-- keys as well held every key in the hash table and ran past the lab Trino node's per-query memory.
with totals as (
    select transaction_id, count(*) as lines, sum(qty) as units, sum(discount_minor) as discount_minor,
           sum(amount_minor) as amount_minor
    from {{ ref('fct_sales_line') }}
    group by transaction_id
)
select t.transaction_id, f.date_key, f.store_key, f.profile_key, f.currency,
       t.lines, t.units,
       t.discount_minor,                    -- "You saved" on the receipt
       t.amount_minor
from totals t
join {{ ref('fct_sales_line') }} f on f.transaction_id = t.transaction_id and f.line_no = 1
