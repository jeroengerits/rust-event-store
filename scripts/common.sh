#!/usr/bin/env bash
# Shared settings for the endpoint scripts; source rather than execute this file.

BASE_URL=${EVENTSTORE_URL:-http://127.0.0.1:8080}
BASE_URL=${BASE_URL%/}
CURL_ARGS=(--silent --show-error --fail-with-body)
AUTH_ARGS=()
if [[ -n ${EVENTSTORE_API_KEY:-} ]]; then
    AUTH_ARGS=(-H "X-API-Key: $EVENTSTORE_API_KEY")
fi

# Percent-encode a stream name as one UTF-8 path segment.
encode_segment() {
    local LC_ALL=C input=$1 char hex i
    for ((i = 0; i < ${#input}; i++)); do
        char=${input:i:1}
        case $char in
            [a-zA-Z0-9.~_-]) printf '%s' "$char" ;;
            *) printf -v hex '%d' "'$char"; printf '%%%02X' "$((hex & 255))" ;;
        esac
    done
}
