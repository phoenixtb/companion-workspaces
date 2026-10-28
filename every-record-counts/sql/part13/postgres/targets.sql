-- Part 13: the synthetic targets, computed by DuckDB from Part 10's lines (`lake` is Polaris, sql/part01/polaris_attach.sql).
-- One row per store and month for Jul-Dec 2026: 104% of the same month in 2025, rounded to 1,000 major units.
SELECT
    store_id,
    CAST(date_trunc('month', business_date) + INTERVAL 1 YEAR AS DATE) AS month,
    any_value(currency) AS currency,
    CAST(round(sum(amount_minor) * 1.04 / 100000) * 100000 AS BIGINT) AS target_minor,
    'rm-' || lower(any_value(country)) AS set_by,
    '2026-06-15 09:00:00+00' AS updated_at          -- text: PostgreSQL parses it as timestamptz
FROM lake.part10.pos_monthly
WHERE business_date BETWEEN DATE '2025-07-01' AND DATE '2025-12-31'
GROUP BY store_id, date_trunc('month', business_date)
ORDER BY store_id, month
