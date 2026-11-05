# HTTP API

This describes the implementation observed during development and may change. All event endpoints use JSON except the SSE response. When configured, send `X-API-Key: <key>` on `/v1` requests; `/health` and `/ready` are public. TLS must be supplied separately.

Stream names are percent-encoded as a single path segment. Decoded names must be nonempty, at most 255 UTF-8 bytes, and contain no control characters. Proxies may reject encoded slashes. Event IDs must be client-generated RFC-variant UUIDv7 values; event types must not be blank, exceed 255 UTF-8 bytes, or contain control characters. Data is JSON and metadata is optional JSON.

## Append

`POST /v1/streams/{stream}/events` accepts an object with `events` and optional `expected_revision`. Use `null` or omit it for no revision precondition, `-1` for a new stream, or a nonnegative integer for the current last revision. Unknown fields in the request or event objects are rejected, rather than silently ignored. The batch must be nonempty and within configured limits. Events in a batch have distinct IDs. The full ordered batch is committed atomically.

On success, HTTP 200 returns `{ "stream": "...", "revision": 0, "first_position": 1, "last_position": 1 }`. Retry an uncertain request with the same stream, IDs in the same order and content. A successful exact retry returns the original result before checking the revision precondition. Reused IDs with different batch membership or content conflict. Revision and ID conflicts return HTTP 409; invalid input returns 400, size violations 413, unavailable storage 503. Error responses typically contain `{"error":{"code":"...","message":"..."}}`; framework-level rejections can differ.

## Read

`GET /v1/streams/{stream}/events` accepts `from` (inclusive revision), `direction` (`forward` or `backward`, default `forward`), and `limit` (1–1000, default 100). Forward reads default to revision zero; backward reads default to the latest revision. Results contain `stream`, `events` and `next_revision`. A nonexistent stream returns an empty page. The continuation cursor is not a guarantee that another event exists.

`GET /v1/events` accepts `after` (exclusive global position, default 0) and `limit` (1–1000, default 100). Results contain `events` and `next_after`; an empty page retains the input cursor. Each event includes `id`, `stream`, `revision`, `position`, `type`, `data`, `metadata` and `timestamp`.

## Subscribe and health

`GET /v1/subscriptions` streams SSE; see [subscriptions](subscriptions.md). `GET /health` returns `{"status":"ok"}`; `GET /ready` checks database access and returns `{"status":"ready"}` when ready. See [OpenAPI](../openapi.yaml) for a machine-readable draft.
