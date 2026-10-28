-- Attach Larkspur's DuckLake as `dl` (DuckDB 2.0, DuckLake 1.1): metadata in the PostgreSQL database `ducklake`,
-- data files in SeaweedFS under s3://larkspur-lake/ducklake/ (read and written with the larkspur_s3 secret).
INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;
ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake user=larkspur password=larkspur' AS dl (
    DATA_PATH 's3://larkspur-lake/ducklake/'
);
