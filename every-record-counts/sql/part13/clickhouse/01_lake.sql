-- Part 13: Polaris as a ClickHouse database. DataLakeCatalog is Beta in ClickHouse 26.8: the session needs
-- allow_database_iceberg = 1 (the script sends it). Tables appear as one name, `namespace.table`.
DROP DATABASE IF EXISTS lake;  -- Part 23: re-created with its own principal (a catalog database holds no data)
CREATE DATABASE lake
ENGINE = DataLakeCatalog('http://polaris:8181/api/catalog')
SETTINGS
    catalog_type = 'rest',
    warehouse = 'larkspur',
    catalog_credential = 'olap_reader:olap-reader-lab-secret', -- Part 23: read-only principal (root until then)
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    oauth_server_uri = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    storage_endpoint = 'http://seaweedfs:8333/larkspur-lake',  -- Polaris returns no endpoint (Part 9), so name it
    aws_access_key_id = 'lake-reader',                         -- read-only, two prefixes from Part 23
    aws_secret_access_key = 'lake-reader-lab-secret';
