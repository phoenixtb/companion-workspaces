#!/bin/sh
# One-shot: create the `larkspur` Iceberg catalog in Polaris and let the root
# principal's catalog_admin role manage tables in it. Safe to re-run.
# Only endpointInternal is set: Polaris uses it for the metadata files it writes.
# Setting `endpoint` would make Polaris hand that URL to every engine, overriding
# their own S3 endpoint (seaweedfs:8333 in containers, localhost:8333 on the host).
set -eu
POLARIS=http://polaris:8181
TOKEN=$(curl -sf "$POLARIS/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d "client_id=$POLARIS_CLIENT_ID" \
  -d "client_secret=$POLARIS_CLIENT_SECRET" -d scope=PRINCIPAL_ROLE:ALL |
  sed -E 's/.*"access_token":"([^"]+)".*/\1/')

status=$(curl -s -o /tmp/out -w '%{http_code}' -X POST "$POLARIS/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{
  "catalog": {
    "name": "larkspur",
    "type": "INTERNAL",
    "properties": { "default-base-location": "s3://larkspur-lake/warehouse" },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://larkspur-lake/warehouse"],
      "endpointInternal": "http://seaweedfs:8333",
      "pathStyleAccess": true,
      "stsUnavailable": true,
      "region": "us-east-1"
    }
  }
}')
case "$status" in
  201) echo "catalog larkspur created" ;;
  409) echo "catalog larkspur already exists" ;;
  *) echo "create catalog failed: HTTP $status"; cat /tmp/out; exit 1 ;;
esac

curl -sf -X PUT "$POLARIS/api/management/v1/catalogs/larkspur/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
echo "catalog_admin can manage content in larkspur"
