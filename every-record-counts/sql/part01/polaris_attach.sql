-- Attach the Polaris catalog as `lake` (DuckDB 2.0 iceberg extension).
INSTALL iceberg;
LOAD iceberg;
CREATE OR REPLACE SECRET polaris (
    TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 's3cr3t',
    OAUTH2_SERVER_URI 'http://localhost:8181/api/catalog/v1/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
);
ATTACH 'larkspur' AS lake (
    TYPE iceberg, URI 'http://localhost:8181/api/catalog', SECRET polaris,
    ACCESS_DELEGATION_MODE 'none'   -- no vended credentials: DuckDB uses the S3 secret
);
