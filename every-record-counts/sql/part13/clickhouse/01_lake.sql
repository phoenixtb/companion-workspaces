-- Part 13: Polaris as a ClickHouse database. DataLakeCatalog is Beta in ClickHouse 26.8: the session needs
-- allow_database_iceberg = 1 (the script sends it). Tables appear as one name, `namespace.table`.
CREATE DATABASE IF NOT EXISTS lake
ENGINE = DataLakeCatalog('http://polaris:8181/api/catalog')
SETTINGS
    catalog_type = 'rest',
    warehouse = 'larkspur',
    catalog_credential = 'root:s3cr3t',                       -- OAuth2 client id:secret, as for Trino and DuckDB
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    oauth_server_uri = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    storage_endpoint = 'http://seaweedfs:8333/larkspur-lake',  -- Polaris returns no endpoint (Part 9), so name it
    aws_access_key_id = 'lake-reader',                         -- the read-only lake key DuckDB uses (Part 9)
    aws_secret_access_key = 'lake-reader-lab-secret';
