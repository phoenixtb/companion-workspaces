-- Part 9: DuckDB attached to Polaris as one principal ({principal} and the rest are filled in by the script).
-- ACCESS_DELEGATION_MODE 'none': DuckDB reads files with the S3 secret below, not vended credentials (see the post).
INSTALL iceberg;
LOAD iceberg;
CREATE OR REPLACE SECRET polaris_{principal} (
    TYPE iceberg, CLIENT_ID '{principal}', CLIENT_SECRET '{client_secret}',
    OAUTH2_SERVER_URI 'http://localhost:8181/api/catalog/v1/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
);
CREATE OR REPLACE SECRET lake_files (
    TYPE s3, KEY_ID '{key}', SECRET '{secret}', REGION 'us-east-1',
    ENDPOINT 'localhost:8333', URL_STYLE 'path', USE_SSL false
);
ATTACH 'larkspur' AS lake (
    TYPE iceberg, URI 'http://localhost:8181/api/catalog', SECRET polaris_{principal},
    ACCESS_DELEGATION_MODE 'none'
);
