#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
if (($# > 2)) || [[ ${1:-} == --help ]]; then
    echo "Usage: $0 [AFTER [LIMIT]]" >&2
    exit 2
fi
query=()
if [[ -n ${1:-} ]]; then query+=(--data-urlencode "after=$1"); fi
if [[ -n ${2:-} ]]; then query+=(--data-urlencode "limit=$2"); fi
curl "${CURL_ARGS[@]}" ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} --get ${query[@]+"${query[@]}"} "$BASE_URL/v1/events"
