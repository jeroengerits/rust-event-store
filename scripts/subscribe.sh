#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
if (($# > 3)) || [[ ${1:-} == --help ]]; then
    echo "Usage: $0 [AFTER [LIMIT [LAST_EVENT_ID]]]" >&2
    echo "LAST_EVENT_ID takes precedence over AFTER; Ctrl+C stops the subscription." >&2
    exit 2
fi
query=()
if [[ -n ${1:-} ]]; then query+=(--data-urlencode "after=$1"); fi
if [[ -n ${2:-} ]]; then query+=(--data-urlencode "limit=$2"); fi
if [[ -n ${3:-} ]]; then AUTH_ARGS+=(-H "Last-Event-ID: $3"); fi
curl "${CURL_ARGS[@]}" ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} --no-buffer --get ${query[@]+"${query[@]}"} \
    "$BASE_URL/v1/subscriptions"
