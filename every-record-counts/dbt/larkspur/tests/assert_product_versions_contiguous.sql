-- A Type 2 dimension's versions of one product must tile time: each version ends exactly where the next begins, so
-- every fact date finds exactly one version. Returns the versions that leave a gap or overlap.
with ordered as (
    select sku, version, valid_from, valid_to,
           lead(valid_from) over (partition by sku order by valid_from) as next_from
    from {{ ref('dim_product') }}
)
select *
from ordered
where valid_from >= valid_to
   or (next_from is not null and next_from <> valid_to)
