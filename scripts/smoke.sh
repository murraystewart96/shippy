#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://localhost:8080"
NUM_CONSIGNMENTS=${1:-5}

command -v jq >/dev/null 2>&1 || { echo "jq is required but not installed"; exit 1; }

echo "--- creating user ---"
curl -sf -X POST "$BASE_URL/v1/users" \
  -H "Content-Type: application/json" \
  -d '{"name":"John Doe","email":"seed@example.com","company":"Shippy Inc","password":"secret123"}' \
  > /dev/null || echo "(user may already exist, continuing)"

echo "--- getting token ---"
TOKEN=$(curl -sf -X POST "$BASE_URL/auth" \
  -H "Content-Type: application/json" \
  -d '{"email":"seed@example.com","password":"secret123"}' \
  | jq -r '.token')

if [[ -z "$TOKEN" || "$TOKEN" == "null" ]]; then
  echo "failed to get token — is the stack running?"
  exit 1
fi

echo "token: ${TOKEN:0:20}..."

CONSIGNMENT_IDS=()

echo ""
echo "--- creating $NUM_CONSIGNMENTS consignments ---"
for i in $(seq 1 "$NUM_CONSIGNMENTS"); do
  RESPONSE=$(curl -sf -X POST "$BASE_URL/v1/consignments" \
    -H "Content-Type: application/json" \
    -H "x-token: $TOKEN" \
    -d "{
      \"description\": \"Shipment $i\",
      \"weight\": 500,
      \"containers\": [{\"customer_id\": \"cust-00$i\", \"user_id\": \"user-00$i\"}]
    }")

  ID=$(echo "$RESPONSE" | jq -r '.id')
  if [[ -z "$ID" || "$ID" == "null" ]]; then
    echo "[$i] failed to create consignment — response: $RESPONSE"
    continue
  fi

  CONSIGNMENT_IDS+=("$ID")
  echo "[$i] created $ID"
done

echo ""
echo "--- confirming consignments ---"
for ID in "${CONSIGNMENT_IDS[@]}"; do
  RESPONSE=$(curl -sf -X POST "$BASE_URL/v1/consignments/confirm/$ID" \
    -H "x-token: $TOKEN")

  echo "confirmed $ID — $(echo "$RESPONSE" | jq -r '.confirmed')"

  # small delay so the saga flows through before the next confirm
  # sleep 1
done

echo ""
echo "--- done ---"
echo "search for these consignment IDs in Grafana Tempo (tag: consignment_id=<id>):"
for ID in "${CONSIGNMENT_IDS[@]}"; do
  echo "  $ID"
done
