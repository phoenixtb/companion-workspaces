-- Part 5: two clocks on one number (DuckDB). UK till sales for business date 30 September (valid time: when the
-- sale happened), as the vault knew them at 06:00 UTC on 1 October, when the daily report ran, and as it knows
-- them now (transaction time: load_ts). The vault never overwrites, so both answers stay reproducible.
WITH gb_pos AS (
    SELECT sp.sale_hk, sp.load_ts, sp.amount_minor,
           (sp.event_ts AT TIME ZONE 'Europe/London')::DATE AS business_date
    FROM sat_sale_pos sp
    JOIN lnk_sale_store ss USING (sale_hk)
    JOIN hub_store st USING (store_hk)
    WHERE st.store_id LIKE 'GB-%'
),
known_at AS (
    SELECT 'as filed, 1 Oct 06:00 UTC' AS version, TIMESTAMPTZ '2026-10-01 06:00:00+00' AS asked_at
    UNION ALL
    SELECT 'as of now', TIMESTAMPTZ 'infinity'
),
known AS (                                     -- each basket's latest row loaded by then
    SELECT k.version, k.asked_at, g.sale_hk, g.amount_minor
    FROM known_at k
    JOIN gb_pos g ON g.load_ts <= k.asked_at
    WHERE g.business_date = DATE '2026-09-30'
    QUALIFY row_number() OVER (PARTITION BY k.version, g.sale_hk ORDER BY g.load_ts DESC) = 1
)
SELECT version, count(*) AS baskets, major(sum(amount_minor)) AS revenue_gbp
FROM known
GROUP BY version, asked_at
ORDER BY asked_at;
