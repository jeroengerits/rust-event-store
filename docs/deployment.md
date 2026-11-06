# Deployment

Build with `cargo build --release` and run `target/release/eventstore serve` with `DATABASE_URL` set to a PostgreSQL URL. Supervise the process, monitor readiness, logs and database capacity, and configure tested backups.

For access beyond loopback set `EVENTSTORE_HOST=0.0.0.0` and provide `EVENTSTORE_API_KEY` through a secret manager. Terminate TLS at a trusted reverse proxy, restrict access to the backend, and configure the proxy not to buffer or prematurely time out SSE connections. Protect operational health endpoints according to your network policy.

## Laravel Cloud / PostgreSQL

1. Push the project to a connected Git repository and create a Rust application on Laravel Cloud. Attach a PostgreSQL database; Cloud supplies `DATABASE_URL`.
2. Set the build command to `cargo build --release --locked` and the start command to `eventstore serve`. Set `EVENTSTORE_HOST=::` for Cloud's IPv6 health probes and inject a strong `EVENTSTORE_API_KEY` as a secret. The app uses Cloud's `PORT` unless `EVENTSTORE_PORT` or `--port` is set. Do not set `EVENTSTORE_DATABASE` to a local path: it overrides Cloud's `DATABASE_URL`.
3. The app applies `migrations-postgres/` on startup using the database credentials in `DATABASE_URL`. The database role must be allowed to create and migrate tables, functions and triggers. Startup fails if migration fails; test schema changes with old and new versions in mind during rolling deployments. No separate deploy migration command is currently provided.
4. In Cloud's deployment settings, configure `/health` as the startup and liveness endpoint and `/ready` as the readiness endpoint. Leave the health-check port at its default so requests go through Cloud's proxy. Verify authenticated writes, reads, SSE reconnects, backups and recovery in staging. PostgreSQL appends are serialized across instances for correct global cursor ordering, which limits write throughput. Global positions can contain gaps after rolled-back transactions. Other instances' subscriptions detect writes through polling (normally within two seconds).

After saving health-check settings, confirm they are shown in Cloud and redeploy to apply them. Check both endpoints after deployment. Responses include `Cache-Control: no-store` to discourage caching event data and stale health results; ensure any proxy or CDN respects this header. If an endpoint returns a cached error from before the deployment, purge that path from Cloud's edge cache and check it again. A successful deployment or a healthy public endpoint alone does not confirm that the custom probes are configured.

Connecting PostgreSQL does not migrate existing SQLite data. A safe migration must preserve event IDs, stream revisions, batch retry metadata and global positions, and must be independently verified; an automatic importer is not provided.

To run the integration tests, see the [README](../README.md#test). Use a dedicated disposable PostgreSQL server, never production.

## Docker

The container requires a reachable PostgreSQL server and an API key. For example:

```sh
docker build -t eventstore .
docker run --rm -p 127.0.0.1:8080:8080 \
  -e DATABASE_URL -e EVENTSTORE_API_KEY eventstore
```

Export `DATABASE_URL` and `EVENTSTORE_API_KEY` first. The database hostname must be reachable from inside the container; `localhost` refers to the container itself. Container ports alone do not provide TLS or access control. See [storage](storage.md) for recovery and upgrades.
