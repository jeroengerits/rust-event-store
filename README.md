# Eventstore

A small HTTP event store backed by PostgreSQL. Append events to named streams, read stream or global history, and subscribe to live events over SSE. Events use client-generated UUIDv7 IDs and support optimistic concurrency.

**Not production-ready.** Further durability and operational testing is required. See [storage](docs/storage.md) and [limitations](docs/limitations.md).

## Start

```sh
DATABASE_URL=postgres://user:password@localhost/eventstore cargo run --release -- serve
```

The server listens on `127.0.0.1:8080` and requires a PostgreSQL `DATABASE_URL`. The schema is created at startup. Existing SQLite data is not imported; preserve and back it up before upgrading. Non-loopback binding requires an API key and a trusted TLS proxy. See [configuration](docs/configuration.md) and [deployment](docs/deployment.md).

## Example

```sh
curl -X POST http://127.0.0.1:8080/v1/streams/orders-123/events \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision":-1,"events":[{"id":"0192a3b4-c5d6-7e89-8abc-123456789abc","type":"OrderPlaced","data":{"total":42}}]}'

curl http://127.0.0.1:8080/v1/streams/orders-123/events
```

Retry timed-out appends with the same IDs and content. For paginated reads, continue from `next_revision` or `next_after`; a short page does not necessarily mean the end. See the [API](docs/api.md), [OpenAPI specification](openapi.yaml), and [subscriptions](docs/subscriptions.md).

Subscriptions deliver events at least once. Persist a processing checkpoint and reconnect from it; send at most one `Last-Event-ID` header to avoid an ambiguous replay position.

The scripts in `scripts/` wrap the HTTP endpoints. Set `EVENTSTORE_URL` and, if needed, `EVENTSTORE_API_KEY`; scripts accepting arguments provide `--help`.

## Test

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

PostgreSQL integration tests require a **dedicated disposable** PostgreSQL server. The API and storage contract tests create isolated test databases and need a role with `CREATEDB` privileges; failed tests can leave databases behind. The PostgreSQL contract tests write to the supplied database:

```sh
TEST_DATABASE_URL=postgres://user:password@localhost/eventstore_test \
  cargo test --locked --all-features --test postgres_contract -- --ignored
DATABASE_URL=postgres://user:password@localhost/eventstore_test \
  cargo test --locked --all-features --test api_contract --test storage_contract -- --ignored
```

CI also smoke-tests Cloud-style PostgreSQL and Docker configurations and checks recovery after an application crash. These checks do not verify database failover, backup restoration, the Cloud proxy, or your deployment settings. See [storage](docs/storage.md#durability-validation) and [deployment](docs/deployment.md) for details.

For load testing, see [benchmarking](docs/benchmarking.md). It writes persistent events and should use a disposable database.
