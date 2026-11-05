#!/usr/bin/env bash
# CI only: writes a fixed event ID to a disposable database.
set -euo pipefail

base_url=$1
curl_args=(--noproxy '*' --globoff --silent --show-error --max-time 5)
event_id=0192a3b4-c5d6-7e89-8abc-123456789abc

for ((attempt = 1; attempt <= 30; attempt++)); do
    if curl "${curl_args[@]}" --fail "$base_url/ready" > /dev/null 2>&1; then break; fi
    if ((attempt == 30)); then echo 'Readiness timed out' >&2; exit 1; fi
    sleep 1
done

test "$(curl "${curl_args[@]}" -o /dev/null -w '%{http_code}' "$base_url/v1/events")" = 401
curl "${curl_args[@]}" --fail -X POST "$base_url/v1/streams/ci/events" \
    -H 'X-API-Key: ci-test-key' -H 'Content-Type: application/json' \
    -d "{\"expected_revision\":-1,\"events\":[{\"id\":\"$event_id\",\"type\":\"Created\",\"data\":{}}]}" > /dev/null
curl "${curl_args[@]}" --fail -H 'X-API-Key: ci-test-key' "$base_url/v1/streams/ci/events" | grep -q "$event_id"
