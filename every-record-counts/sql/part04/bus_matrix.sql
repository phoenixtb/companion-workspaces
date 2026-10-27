-- The bus matrix, read from the star itself: which dimension each fact table carries a key to (DuckDB).
SELECT table_name AS fact,
       max(CASE WHEN column_name LIKE '%date_key' THEN 'x' ELSE '' END)
         || CASE WHEN count(*) FILTER (WHERE column_name LIKE '%date_key') > 1
                 THEN ' (' || count(*) FILTER (WHERE column_name LIKE '%date_key') || ' roles)' ELSE '' END AS date,
       max(CASE WHEN column_name = 'store_key' THEN 'x' ELSE '' END)    AS store,
       max(CASE WHEN column_name = 'product_key' THEN 'x' ELSE '' END)  AS product,
       max(CASE WHEN column_name = 'promo_key' THEN 'x' ELSE '' END)    AS promotion,
       max(CASE WHEN column_name = 'profile_key' THEN 'x' ELSE '' END)  AS sale_profile,
       max(CASE WHEN column_name = 'supplier_key' THEN 'x' ELSE '' END) AS supplier,
       max(CASE WHEN column_name = 'audit_key' THEN 'x' ELSE '' END)    AS audit
FROM duckdb_columns()
WHERE table_name LIKE 'fct_%'
GROUP BY table_name
ORDER BY table_name;
