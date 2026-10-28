-- Junk dimension: the low-cardinality flags of a sale, one row per combination that occurs.
select {{ dbt_utils.generate_surrogate_key(['channel', 'tender_type', 'has_loyalty_card']) }} as profile_key, *
from (select distinct channel, tender_type, has_loyalty_card from {{ ref('int_sales_lines') }}) p
