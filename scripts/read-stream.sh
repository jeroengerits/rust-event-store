#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
if (($# < 1 || $# > 4)) || [[ $1 == --help ]]; then
    echo "Usage: $0 STREAM [FROM [forward|backward [LIMIT]]]" >&2
    echo "Omit FROM to use the server default; use '-' to omit FROM when setting direction." >&2
    exit 2
fi
stream=$(encode_segment "$1")
query=()
if [[ ${2:--} != - ]]; then query+=(--data-urlencode "from=$2"); fi
if [[ -n ${3:-} ]]; then query+=(--data-urlencode "direction=$3"); fi
if [[ -n ${4:-} ]]; then query+=(--data-urlencode "limit=$4"); fi
curl "${CURL_ARGS[@]}" ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} --get ${query[@]+"${query[@]}"} \
    "$BASE_URL/v1/streams/$stream/events"
