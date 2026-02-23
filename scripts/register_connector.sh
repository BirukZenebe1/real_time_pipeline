#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-rides-cdc-connector}"
CONFIG_FILE="${CONFIG_FILE:-./connect/rides_cdc_connector.json}"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Connector config not found: ${CONFIG_FILE}" >&2
  exit 1
fi

if curl -fsS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1; then
  echo "Connector exists; deleting before recreate: ${CONNECTOR_NAME}"
  curl -fsS -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null
  sleep 2
fi

echo "Creating connector: ${CONNECTOR_NAME}"
curl -fsS -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}"

echo "Connector is ready at ${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"
