#!/usr/bin/env bash
# Check Docker's actual build context without touching local database files.
set -euo pipefail

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/context/nested" "$work/output"
cp .dockerignore "$work/context/.dockerignore"
printf 'FROM scratch\nCOPY . /context/\n' > "$work/context/Dockerfile"
touch "$work/context/keep.txt" "$work/context/nested/keep.txt"

for extension in db sqlite sqlite3; do
    for suffix in '' -shm -wal -journal; do
        name="sample.$extension$suffix"
        touch "$work/context/$name" "$work/context/nested/$name"
        git check-ignore --no-index -q "$name"
        git check-ignore --no-index -q "nested/$name"
    done
done
touch "$work/context/.env" "$work/context/nested/.env.local"
docker build --quiet --output "type=local,dest=$work/output" "$work/context" > /dev/null
test -f "$work/output/context/keep.txt"
test -f "$work/output/context/nested/keep.txt"
if find "$work/output/context" -type f ! -name keep.txt ! -name .dockerignore | grep -q .; then
    echo 'Unexpected files in Docker build context' >&2
    exit 1
fi
