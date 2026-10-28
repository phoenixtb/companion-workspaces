-- One row per promotion version, from the snapshot: the admin tool overwrites a promotion when it's edited, so the
-- snapshot is the only place its history exists. A version is in force from its edit until the next one.
select {{ dbt_utils.generate_surrogate_key(['promo_id', 'version']) }} as promo_key,
       promo_id, version, country, promo_name, discount_pct,
       valid_from     as promo_starts_at,
       valid_to       as promo_ends_at,
       dbt_valid_from as version_from,
       dbt_valid_to   as version_to
from {{ ref('snp_promotion') }}
union all
select 'none', null, null, null, '(no promotion)', 0, null, null, null, null
