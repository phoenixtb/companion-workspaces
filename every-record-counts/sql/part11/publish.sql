-- Part 11: DuckDB publishes the four-year revenue view into the catalog as an Iceberg table (merge-on-read writer).
-- DuckDB's sum(BIGINT) is a HUGEINT (128-bit); Iceberg has no such type, so an uncast sum lands as DECIMAL(38,0).
CREATE SCHEMA IF NOT EXISTS lake.part11;
DROP TABLE IF EXISTS lake.part11.revenue_by_month;
CREATE TABLE lake.part11.revenue_by_month AS
SELECT CAST(date_trunc('MONTH', business_date) AS DATE) AS month, country, currency,
       count(*) AS lines,
       CAST(sum(amount_minor) AS BIGINT) AS sales_minor,
       CAST(sum(discount_minor) AS BIGINT) AS discount_minor
FROM lake.part10.pos_monthly
GROUP BY 1, 2, 3;
