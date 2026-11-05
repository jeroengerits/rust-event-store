# Eventstore

A small HTTP event store backed by SQLite or PostgreSQL. Append events to named streams, read stream or global history, and subscribe to live events over SSE. Events use client-generated UUIDv7 IDs and support optimistic concurrency.

**Not production-ready.** The bundled SQLite version has a WAL corruption bug; Eventstore uses rollback journaling as a workaround, not a fix. Existing databases require a coordinated restart when upgrading. Both backends need further durability and operational testing. See [storage](docs/storage.md) and [limitations](docs/limitations.md).

## Start

```sh
cargo run --release -- serve
```

The server listens on `127.0.0.1:8080` and uses `./data/events.db` by default. Set `DATABASE_URL` to use PostgreSQL; the schema is created at startup, but existing SQLite data is not imported. Non-loopback binding requires an API key and a trusted TLS proxy. See [configuration](docs/configuration.md) and [deployment](docs/deployment.md).

## Example

```sh
curl -X POST http://127.0.0.1:8080/v1/streams/orders-123/events \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision":-1,"events":[{"id":"0192a3b4-c5d6-7e89-8abc-123456789abc","type":"OrderPlaced","data":{"total":42}}]}'

curl http://127.0.0.1:8080/v1/streams/orders-123/events
```

Retry timed-out appends with the same IDs and content. For paginated reads, continue from `next_revision` or `next_after`; a short page does not necessarily mean the end. See the [API](docs/api.md), [OpenAPI specification](openapi.yaml), and [subscriptions](docs/subscriptions.md).

The scripts in `scripts/` wrap the HTTP endpoints. Set `EVENTSTORE_URL` and, if needed, `EVENTSTORE_API_KEY`; scripts accepting arguments provide `--help`.

## Test

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

PostgreSQL integration tests write data and require a **dedicated disposable** database:

```sh
TEST_DATABASE_URL=postgres://user:password@localhost/eventstore_test \
  cargo test --locked --all-features --test postgres_contract -- --ignored
```

CI also smoke-tests Cloud-style PostgreSQL and Docker configurations. These checks do not verify the Cloud proxy, SSE behavior, or your deployment settings. See [deployment](docs/deployment.md) for details.
