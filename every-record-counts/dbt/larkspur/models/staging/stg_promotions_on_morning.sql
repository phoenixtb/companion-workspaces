-- What the promotions admin tool shows at 06:00 UTC on var('as_of'): the latest version of each promotion.
-- The tool keeps no history; the generator does (raw.promotion_versions), so the lab can play any morning back.
with known as (
    select *, row_number() over (partition by promo_id order by version desc) as newest
    from {{ ref('stg_promotion_versions') }}
    where updated_at <= timestamp '{{ var("as_of") }} 06:00:00 UTC'
)
select promo_id, version, country, promo_name, discount_pct, valid_from, valid_to, updated_at
from known
where newest = 1
