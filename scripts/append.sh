#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
if (($# < 1 || $# > 2)) || [[ $1 == --help ]]; then
    echo "Usage: $0 STREAM [JSON_FILE|-] (default: read JSON from stdin)" >&2
    exit 2
fi
stream=$(encode_segment "$1")
file=${2:--}
curl "${CURL_ARGS[@]}" ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} -X POST \
    -H 'Content-Type: application/json' --data-binary "@$file" \
    "$BASE_URL/v1/streams/$stream/events"
