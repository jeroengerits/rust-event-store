#!/usr/bin/env bash
# Creates and destroys its own disposable PostgreSQL container and data.
set -euo pipefail

container="eventstore-crash-test-$(openssl rand -hex 8)"
fixture=$(mktemp)
cleanup() {
    docker rm -f "$container" > /dev/null 2>&1 || true
    rm -f "$fixture"
}
trap cleanup EXIT

docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=eventstore_test -p 127.0.0.1::5432 postgres:16 > /dev/null
port=$(docker port "$container" 5432/tcp)
port=${port##*:}

wait_for_postgres() {
    for ((attempt = 1; attempt <= 60; attempt++)); do
        if docker exec "$container" pg_isready -U postgres -d eventstore_test > /dev/null 2>&1; then return; fi
        sleep 1
    done
    docker logs "$container" >&2
    echo 'PostgreSQL readiness timed out' >&2
    return 1
}

snapshot() {
    docker exec "$container" psql -X -A -t -v ON_ERROR_STOP=1 -U postgres -d "$1" -c \
        "SELECT COALESCE(json_agg(row_to_json(t) ORDER BY t.id), '[]'::json) FROM batches t; SELECT COALESCE(json_agg(row_to_json(t) ORDER BY t.position), '[]'::json) FROM events t;"
}

wait_for_postgres
RECOVERY_TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$port/eventstore_test" \
    RECOVERY_TEST_FIXTURE="$fixture" \
    bash .github/scripts/recovery-test.sh
before=$(snapshot eventstore_test)
docker kill "$container" > /dev/null
docker start "$container" > /dev/null
wait_for_postgres
port=$(docker port "$container" 5432/tcp)
port=${port##*:}
after=$(snapshot eventstore_test)
test "$before" = "$after"

# Restore a logical backup into a separate, disposable database, including sequences
# and migration metadata. The restored application must still be able to append.
docker exec "$container" createdb -U postgres eventstore_restore
docker exec "$container" pg_dump -U postgres -Fc eventstore_test |
    docker exec -i "$container" pg_restore -U postgres --exit-on-error -d eventstore_restore
test "$before" = "$(snapshot eventstore_restore)"
last_position=$(docker exec "$container" psql -X -A -t -v ON_ERROR_STOP=1 -U postgres \
    -d eventstore_restore -c 'SELECT max(position) FROM events')
RECOVERY_TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$port/eventstore_restore" \
    RECOVERY_TEST_RETRY_FIXTURE="$fixture" \
    bash .github/scripts/recovery-test.sh
new_position=$(docker exec "$container" psql -X -A -t -v ON_ERROR_STOP=1 -U postgres \
    -d eventstore_restore -c 'SELECT max(position) FROM events')
test "$new_position" -gt "$last_position"
