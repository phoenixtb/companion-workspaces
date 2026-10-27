-- A promotion edited after launch: two versions, each in force from its updated_at (DuckDB).
SELECT promo_key, promo_id, version, promo_name, discount_pct, version_from, version_to
FROM dim_promotion
WHERE promo_id = 'PR-0001';

-- Every promoted sale line, three ways to find "the promotion" it was sold under. Does the discount that
-- version implies equal what the till charged? (qty × unit price × pct, rounded half up, in minor units)
WITH promoted AS (
    SELECT f.transaction_id, f.line_no, f.qty, f.unit_price_minor, f.discount_minor, f.promo_key, p.promo_id
    FROM fct_sales_line f JOIN dim_promotion p USING (promo_key)
    WHERE f.promo_key <> 0
),
as_of AS (                    -- the version in force when the line was sold (the fact's promo_key)
    SELECT pr.*, d.discount_pct FROM promoted pr JOIN dim_promotion d USING (promo_key)
),
latest AS (                   -- "the" promotion: its latest version
    SELECT pr.*, d.discount_pct FROM promoted pr
    JOIN dim_promotion d ON d.promo_id = pr.promo_id AND d.version_to = 'infinity'
),
natural_key AS (              -- the natural key alone: one row per version
    SELECT pr.*, d.discount_pct FROM promoted pr JOIN dim_promotion d ON d.promo_id = pr.promo_id
)
SELECT join_on, count(*) AS rows, count(*) FILTER (WHERE (qty * unit_price_minor * discount_pct + 50) // 100
                                                         = discount_minor) AS rows_matching_till
FROM (SELECT 'promo_key, version as of the sale' AS join_on, * FROM as_of
      UNION ALL SELECT 'promo_id, latest version', * FROM latest
      UNION ALL SELECT 'promo_id alone', * FROM natural_key)
GROUP BY join_on
ORDER BY rows_matching_till DESC;
