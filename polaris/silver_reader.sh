#!/bin/sh
# Lab 09-2, exercise 2: a catalog role limited to the silver namespace.
# Creates principal "reader" -> principal role "silver_analyst" -> catalog role "silver_reader".
set -e
API="$POLARIS_URI/api/management/v1"
CAT="$POLARIS_CATALOG"
TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials \
  -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
call() { curl -sS -o /dev/null -w "%{http_code} $1 $2\n" -X "$1" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d "$3" "$API$2"; }

if ! grep -q POLARIS_READER_CREDENTIAL /home/onyxia/work/.polaris-credentials.env; then
  curl -fsS -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"principal":{"name":"reader","properties":{}},"credentialRotationRequired":false}' \
    "$API/principals" \
  | jq -r '"POLARIS_READER_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
    >> /home/onyxia/work/.polaris-credentials.env
  echo "principal reader created"
fi
call POST /principal-roles '{"principalRole":{"name":"silver_analyst","properties":{}}}'
call POST "/catalogs/$CAT/catalog-roles" '{"catalogRole":{"name":"silver_reader","properties":{}}}'
call PUT "/catalogs/$CAT/catalog-roles/silver_reader/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_READ_PROPERTIES"}}'
for p in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  call PUT "/catalogs/$CAT/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$p\"}}"
done
call PUT /principals/reader/principal-roles '{"principalRole":{"name":"silver_analyst"}}'
call PUT "/principal-roles/silver_analyst/catalog-roles/$CAT" '{"catalogRole":{"name":"silver_reader"}}'
echo "--- grants of silver_reader"
curl -fsS -H "Authorization: Bearer $TOKEN" "$API/catalogs/$CAT/catalog-roles/silver_reader/grants" \
  | jq -c '.grants[] | {type, namespace, privilege}'
