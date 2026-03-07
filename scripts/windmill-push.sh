#!/usr/bin/env bash
set -euo pipefail

# Load local env overrides if present.
if [[ -f ".env.local" ]]; then
  # shellcheck disable=SC1091
  set -a; source ".env.local"; set +a
elif [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  set -a; source ".env"; set +a
fi

: "${WMILL_TOKEN:?WMILL_TOKEN is required}"

WMILL_BASE_URL="${WMILL_BASE_URL:-http://localhost}"
WMILL_WORKSPACE="${WMILL_WORKSPACE:-starter}"
WMILL_CLI_VERSION="${WMILL_CLI_VERSION:-latest}"

npx --yes "windmill-cli@${WMILL_CLI_VERSION}" sync push \
  --yes \
  --workspace "${WMILL_WORKSPACE}" \
  --token "${WMILL_TOKEN}" \
  --base-url "${WMILL_BASE_URL}"
