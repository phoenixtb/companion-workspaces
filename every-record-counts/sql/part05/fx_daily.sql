-- Part 5: an FX rate for every calendar day (DuckDB).
-- The rate source publishes on working days only. A Saturday, Sunday or holiday uses the latest rate published
-- on or before it, so densify once (an as-of join) and let every fact join on a plain date.

CREATE OR REPLACE TABLE dim_fx_rate AS
WITH days AS (                             -- only days the source has spoken for: no rate for tomorrow
    SELECT unnest(generate_series(min(rate_date), max(rate_date), INTERVAL 1 DAY))::DATE AS date
    FROM raw_fx_rates
),
pairs AS (
    SELECT DISTINCT base_currency, quote_currency FROM raw_fx_rates
),
rates AS (
    SELECT * FROM raw_fx_rates ORDER BY base_currency, rate_date
)
SELECT d.date, p.base_currency, p.quote_currency, r.rate_date, r.rate,
       d.date <> r.rate_date AS carried_forward
FROM days d CROSS JOIN pairs p
ASOF JOIN rates r
    ON r.base_currency = p.base_currency AND r.quote_currency = p.quote_currency AND d.date >= r.rate_date
UNION ALL                                  -- the reporting currency converts to itself
SELECT date, 'GBP', 'GBP', date, 1, false FROM days;

-- Minor units in, minor units out. Round half away from zero on the absolute amount, so converting -x gives
-- exactly minus converting x at the same rate. The rate is a DECIMAL(18, 8): no float touches money.
CREATE OR REPLACE MACRO convert_minor(amount_minor, rate) AS
    CAST(sign(amount_minor) * round(abs(amount_minor) * rate) AS BIGINT);
