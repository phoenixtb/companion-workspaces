-- Lets DuckDB read the larkspur-raw bucket on the local SeaweedFS (lab credentials).
CREATE OR REPLACE SECRET larkspur_s3 (
    TYPE s3, KEY_ID 'larkspur', SECRET 'larkspur-lab-secret', REGION 'us-east-1',
    ENDPOINT 'localhost:8333', URL_STYLE 'path', USE_SSL false
);
