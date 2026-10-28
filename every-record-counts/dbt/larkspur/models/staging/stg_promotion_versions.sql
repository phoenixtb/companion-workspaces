select promo_id, version, country, promo_name, discount_pct, valid_from, valid_to, updated_at
from {{ source('raw', 'promotion_versions') }}
