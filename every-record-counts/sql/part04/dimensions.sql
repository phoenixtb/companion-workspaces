-- Part 4, step 2: dimensions (DuckDB). Every dimension has an integer surrogate key; natural keys stay as columns.

-- Money is stored in minor units (pence, paise, cents); this shows it in major units as a DECIMAL, never a float.
CREATE OR REPLACE MACRO major(minor) AS CAST(minor AS DECIMAL(18, 0)) * 0.01;

-- Date: one row per calendar day. date_key is yyyymmdd; -1 is the "hasn't happened yet" row.
CREATE OR REPLACE TABLE dim_date AS
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
       d::DATE                                AS date,
       dayname(d)                             AS day_name,
       isodow(d) >= 6                         AS is_weekend,
       date_trunc('week', d)::DATE            AS week_start,     -- ISO weeks start on Monday
       week(d)                                AS iso_week,
       strftime(d, '%Y-%m')                   AS month,
       year(d) || '-Q' || quarter(d)          AS quarter
FROM range(DATE '2026-01-01', DATE '2027-01-01', INTERVAL 1 DAY) AS t(d)
UNION ALL
SELECT -1, NULL, 'Not yet', NULL, NULL, NULL, NULL, NULL;

-- Store: Type 1 (the source keeps no history). One extra row per country for online sales.
CREATE OR REPLACE TABLE dim_store AS
SELECT row_number() OVER (ORDER BY store_type DESC, store_id) AS store_key, *
FROM (SELECT store_id, 'store' AS store_type, country, timezone, city, format, wms_snapshot_schedule
      FROM raw_stores
      UNION ALL
      SELECT country || '-ONLINE', 'online', country, timezone, NULL, 'online', NULL
      FROM stg_country);

-- Product: Type 2 built from the daily catalogue extracts, plus Type 1 "current_" columns (together: Type 6).
-- A new row starts whenever any tracked attribute differs from the previous day's extract.
CREATE OR REPLACE TABLE dim_product AS
WITH daily AS (
    SELECT *, md5(CAST((product_name, category, subcategory, brand, is_chilled, supplier_id) AS VARCHAR)) AS attrs  -- NULLs hash too
    FROM raw_catalog_products
),
flagged AS (
    SELECT *, attrs IS DISTINCT FROM lag(attrs) OVER (PARTITION BY sku ORDER BY snapshot_date) AS changed
    FROM daily
),
numbered AS (
    SELECT *, sum(changed::INT) OVER (PARTITION BY sku ORDER BY snapshot_date) AS version
    FROM flagged
),
versions AS (
    SELECT sku, version, any_value(product_id) AS product_id, any_value(product_name) AS product_name,
           any_value(category) AS category, any_value(subcategory) AS subcategory, any_value(brand) AS brand,
           any_value(is_chilled) AS is_chilled, any_value(supplier_id) AS supplier_id,
           min(snapshot_date) AS valid_from
    FROM numbered
    GROUP BY sku, version
)
SELECT row_number() OVER (ORDER BY sku, version)                                 AS product_key,
       sku, product_id, version, product_name, category, subcategory, brand, is_chilled, supplier_id,
       valid_from,                                                               -- inclusive
       coalesce(lead(valid_from) OVER w, DATE '9999-12-31')                      AS valid_to,  -- exclusive
       lead(valid_from) OVER w IS NULL                                           AS is_current,
       last_value(category) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)    AS current_category,
       last_value(subcategory) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS current_subcategory,
       false                                                                     AS is_inferred
FROM versions
WINDOW w AS (PARTITION BY sku ORDER BY version);

-- Promotion: one row per version. A version is in force from its updated_at until the next version's.
CREATE OR REPLACE TABLE dim_promotion AS
SELECT row_number() OVER (ORDER BY promo_id, version) AS promo_key,
       promo_id, version, country, promo_name, discount_pct,
       valid_from AS promo_starts_at, valid_to AS promo_ends_at,
       updated_at                                                                                    AS version_from,
       coalesce(lead(updated_at) OVER (PARTITION BY promo_id ORDER BY version), TIMESTAMPTZ 'infinity') AS version_to
FROM raw_promotion_versions
UNION ALL
SELECT 0, NULL, NULL, NULL, '(no promotion)', 0, NULL, NULL, NULL, NULL;

-- Junk dimension: low-cardinality flags of a sale, one row per combination that occurs.
CREATE OR REPLACE TABLE dim_sale_profile AS
SELECT row_number() OVER (ORDER BY channel, tender_type, has_loyalty_card) AS profile_key, *
FROM (SELECT DISTINCT channel, tender_type, has_loyalty_card FROM stg_sales_line);

CREATE OR REPLACE TABLE dim_supplier AS
SELECT row_number() OVER (ORDER BY supplier_id) AS supplier_key, supplier_id, supplier_name, feed_format
FROM raw_suppliers;
