# Eventstore

Eventstore is a small HTTP event store backed by SQLite or PostgreSQL. It appends ordered batches to named streams, reads stream or global history, and offers a live server-sent event feed. Clients supply UUIDv7 event IDs and can use expected revisions for optimistic concurrency.

**Not production-ready:** the bundled SQLite 3.46.0 is affected by the rare [WAL-reset corruption bug](https://www.sqlite.org/wal.html#the_wal_reset_bug). The server uses rollback journaling rather than WAL to avoid the affected code path; upgrading an existing database requires a coordinated restart. Both backends still need production validation and operational testing before relying on them for durable data.

## Start

With a Rust toolchain installed:

```sh
cargo run --release -- serve
```

By default the server binds `127.0.0.1:8080` and stores its database at `./data/events.db`. See [configuration](docs/configuration.md) for limits, environment variables and network binding. A non-loopback bind requires an API key; use TLS at a trusted reverse proxy for remote access.

For PostgreSQL, set `DATABASE_URL` to a PostgreSQL connection URL before starting. The application creates its schema at startup. Use a fresh database; an existing SQLite database is not imported automatically. See [deployment](docs/deployment.md) for Laravel Cloud and testing instructions.

## Testing and CI

Run the same Rust checks as CI:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

The PostgreSQL contract is ignored by the default test run because it creates tables and leaves events behind. To run it, provide a **dedicated disposable** PostgreSQL database:

```sh
TEST_DATABASE_URL=postgres://user:password@localhost/eventstore_test \
  cargo test --locked --all-features --test postgres_contract -- --ignored
```

CI also builds the Docker image and smoke-tests readiness, authentication, append and read operations. See [deployment](docs/deployment.md) for container setup.

## Production readiness

This project is not yet production-ready. The SQLite WAL workaround above does not replace a fixed SQLite release, and passing tests or CI does not establish durability or suitability for a particular workload. Before deployment, validate the selected backend under realistic concurrency and failure conditions, test backup and restore procedures, configure TLS and access controls, and set up monitoring and alerting. See [storage and recovery](docs/storage.md), [deployment](docs/deployment.md), and [limitations](docs/limitations.md).

## Example append

```sh
curl -X POST 'http://127.0.0.1:8080/v1/streams/orders-123/events' \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision":-1,"events":[{"id":"0192a3b4-c5d6-7e89-8abc-123456789abc","type":"OrderPlaced","data":{"total":42},"metadata":null}]}'
```

Retry a timed-out append with the same stream, ordered IDs, and event content. Do not generate new IDs on retry. Read history at `/v1/streams/{stream}/events` or `/v1/events`; subscribe at `/v1/subscriptions`.

Read pages have a soft 4 MiB event-content budget as well as the requested event limit, so a page may contain fewer events than requested. Continue from the last returned revision or global position rather than assuming a short page means the end. To ensure progress, a single event larger than the budget is still returned; the budget is not a hard limit on response size or process memory. It includes conservative framing allowance, but does not bound all HTTP or in-memory overhead.

## Command-line scripts

The Bash scripts in `scripts/` wrap each HTTP endpoint. They require `curl` and default to `http://127.0.0.1:8080`. Set `EVENTSTORE_URL` to change the server and `EVENTSTORE_API_KEY` when authentication is enabled. They print the response body and return a nonzero exit code on HTTP or connection errors. Use `--help` on scripts that take arguments.

```sh
scripts/health.sh
scripts/ready.sh

# Supply the complete append request as a file or on standard input.
scripts/append.sh orders-123 request.json
cat request.json | scripts/append.sh orders-123

scripts/read-stream.sh orders-123                  # first page from revision 0
scripts/read-stream.sh orders-123 1 forward 10     # from revision 1, inclusive
scripts/read-stream.sh orders-123 - backward 10    # latest events first
scripts/read-global.sh 0 10                        # positions after 0
scripts/subscribe.sh 0                             # live SSE; Ctrl+C to stop
scripts/subscribe.sh 0 100 42                      # resume with Last-Event-ID 42
```

Append files use the JSON format shown above. Use a new UUIDv7 for each new event and preserve the same request for an idempotent retry. On reads, use `next_revision` or `next_after` from the response to request the next page. For subscriptions, resume from the last *successfully processed* global position, not merely the last received event.

## Documentation

- [Architecture](docs/architecture.md), [API](docs/api.md), and [OpenAPI](openapi.yaml)
- [Storage and recovery](docs/storage.md), [subscriptions](docs/subscriptions.md)
- [Configuration](docs/configuration.md), [deployment](docs/deployment.md), and [limitations](docs/limitations.md)

The API is under active implementation; verify wire-level details against the running version before relying on this draft.
