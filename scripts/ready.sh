#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
if (($#)); then echo "Usage: $0" >&2; exit 2; fi
curl "${CURL_ARGS[@]}" "$BASE_URL/ready"
