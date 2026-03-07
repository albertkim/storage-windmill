#!/usr/bin/env bash
set -euo pipefail

# Single source of config: .env
if [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  set -a; source ".env"; set +a
fi

: "${WMILL_TOKEN:?WMILL_TOKEN is required}"

WMILL_BASE_URL="${WMILL_BASE_URL:-http://localhost}"
WMILL_WORKSPACE="${WMILL_WORKSPACE:-starter}"
WMILL_CLI_VERSION="${WMILL_CLI_VERSION:-latest}"

# Upsert env vars prefixed with SECRET_ to global Windmill paths.
# Example: SECRET_APIFY_API_TOKEN=... -> f/secrets/apify_api_token
while IFS='=' read -r key value; do
  [[ "${key}" == SECRET_* ]] || continue
  secret_name="${key#SECRET_}"
  remote_key="$(printf '%s' "${secret_name}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g')"
  remote_var_path="f/secrets/${remote_key}"
  encoded_path="$(printf '%s' "${remote_var_path}" | jq -sRr @uri)"

  exists="$(curl -sS \
    -H "Authorization: Bearer ${WMILL_TOKEN}" \
    "${WMILL_BASE_URL}/api/w/${WMILL_WORKSPACE}/variables/exists/${encoded_path}")"

  payload="$(jq -n \
    --arg path "${remote_var_path}" \
    --arg value "${value}" \
    --arg desc "Synced from .env (${key})" \
    '{path: $path, value: $value, is_secret: false, description: $desc}')"

  if [[ "${exists}" == "true" ]]; then
    curl -fsS -X POST \
      -H "Authorization: Bearer ${WMILL_TOKEN}" \
      -H "Content-Type: application/json" \
      --data "${payload}" \
      "${WMILL_BASE_URL}/api/w/${WMILL_WORKSPACE}/variables/update/${encoded_path}?already_encrypted=false" >/dev/null
  else
    curl -fsS -X POST \
      -H "Authorization: Bearer ${WMILL_TOKEN}" \
      -H "Content-Type: application/json" \
      --data "${payload}" \
      "${WMILL_BASE_URL}/api/w/${WMILL_WORKSPACE}/variables/create?already_encrypted=false" >/dev/null
  fi
done < <(env)

npx --yes "windmill-cli@${WMILL_CLI_VERSION}" sync push \
  --yes \
  --skip-variables \
  --skip-secrets \
  --workspace "${WMILL_WORKSPACE}" \
  --token "${WMILL_TOKEN}" \
  --base-url "${WMILL_BASE_URL}"
