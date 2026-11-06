#!/usr/bin/env bash
# Writes events to a disposable PostgreSQL database and kills only its own server.
set -euo pipefail

: "${RECOVERY_TEST_DATABASE_URL:?Set RECOVERY_TEST_DATABASE_URL to a disposable PostgreSQL database}"
port=${RECOVERY_TEST_PORT:-18083}
binary=${EVENTSTORE_TEST_BINARY:-target/debug/eventstore}
log=$(mktemp)
pid=
cleanup() {
    if [[ -n $pid ]]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -f "$log"
}
trap cleanup EXIT

key=$(openssl rand -hex 24)
random=$(openssl rand -hex 16)
event_id="${random:0:8}-${random:8:4}-7${random:13:3}-8${random:17:3}-${random:20:12}"
second_random=$(openssl rand -hex 16)
second_event_id="${second_random:0:8}-${second_random:8:4}-7${second_random:13:3}-8${second_random:17:3}-${second_random:20:12}"
stream="recovery-${random}"
base="http://127.0.0.1:$port"
body="{\"expected_revision\":-1,\"events\":[{\"id\":\"$event_id\",\"type\":\"Created\",\"data\":{\"test\":true}},{\"id\":\"$second_event_id\",\"type\":\"Updated\",\"data\":{\"test\":false}}]}"
curl_args=(--noproxy '*' --silent --show-error --fail --max-time 5)

start_server() {
    EVENTSTORE_HOST=127.0.0.1 EVENTSTORE_PORT="$port" \
        EVENTSTORE_DATABASE="$RECOVERY_TEST_DATABASE_URL" EVENTSTORE_API_KEY="$key" \
        "$binary" serve > "$log" 2>&1 &
    pid=$!
    for ((attempt = 1; attempt <= 50; attempt++)); do
        if ! kill -0 "$pid" 2>/dev/null; then cat "$log" >&2; return 1; fi
        if curl "${curl_args[@]}" "$base/ready" > /dev/null 2>&1 &&
            curl "${curl_args[@]}" -H "X-API-Key: $key" "$base/v1/events?limit=1" > /dev/null 2>&1; then return; fi
        sleep 0.1
    done
    cat "$log" >&2
    echo 'Readiness timed out' >&2
    return 1
}

append() {
    curl "${curl_args[@]}" -X POST "$base/v1/streams/$stream/events" \
        -H "X-API-Key: $key" -H 'Content-Type: application/json' -d "$body" | jq -S .
}

start_server
if [[ -n ${RECOVERY_TEST_RETRY_FIXTURE:-} ]]; then
    restored_stream=$(jq -er '.stream' "$RECOVERY_TEST_RETRY_FIXTURE")
    restored_body=$(jq -ec '.body' "$RECOVERY_TEST_RETRY_FIXTURE")
    restored_expected=$(jq -eS '.response' "$RECOVERY_TEST_RETRY_FIXTURE")
    restored_response=$(curl "${curl_args[@]}" -X POST "$base/v1/streams/$restored_stream/events" \
        -H "X-API-Key: $key" -H 'Content-Type: application/json' -d "$restored_body" | jq -S .)
    test "$restored_expected" = "$restored_response"
    restored_events=$(curl "${curl_args[@]}" -H "X-API-Key: $key" "$base/v1/streams/$restored_stream/events" | jq -S .)
    jq -e --argjson ids "$(jq -ec '[.body.events[].id]' "$RECOVERY_TEST_RETRY_FIXTURE")" \
        '[.events[].id] == $ids' <<< "$restored_events" > /dev/null

    changed_body=$(jq -c '.events[0].data.test = false' <<< "$restored_body")
    conflict=$(curl --noproxy '*' --silent --show-error --max-time 5 -w '\n%{http_code}' \
        -X POST "$base/v1/streams/$restored_stream/events" -H "X-API-Key: $key" \
        -H 'Content-Type: application/json' -d "$changed_body")
    test "${conflict##*$'\n'}" = 409
    jq -e '.error.code == "idempotency_conflict"' <<< "${conflict%$'\n'*}" > /dev/null
    test "$restored_events" = "$(curl "${curl_args[@]}" -H "X-API-Key: $key" \
        "$base/v1/streams/$restored_stream/events" | jq -S .)"
fi
before=$(append)
if [[ -n ${RECOVERY_TEST_FIXTURE:-} ]]; then
    jq -n --arg stream "$stream" --argjson body "$body" --argjson response "$before" \
        '{stream: $stream, body: $body, response: $response}' > "$RECOVERY_TEST_FIXTURE"
fi
kill -KILL "$pid"
wait "$pid" 2>/dev/null || true
pid=
start_server
after=$(append)
test "$before" = "$after"
curl "${curl_args[@]}" -H "X-API-Key: $key" "$base/v1/streams/$stream/events" |
    jq -e --arg first "$event_id" --arg second "$second_event_id" \
        '[.events[].id] == [$first, $second]' > /dev/null
