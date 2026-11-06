# Configuration

Run `eventstore serve --help` for the options supported by your build. CLI flags take precedence over their matching environment variables.

| Flag | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--host` | `EVENTSTORE_HOST` | `127.0.0.1` | Bind IP address |
| `--port` | `EVENTSTORE_PORT`, then `PORT` | `8080` | Bind port |
| `--database` | `EVENTSTORE_DATABASE`, then `DATABASE_URL` | required | PostgreSQL connection URL |
| `--api-key` | `EVENTSTORE_API_KEY` | unset | API key; required for non-loopback binds |
| `--log-level` | `EVENTSTORE_LOG_LEVEL` | `info` | Log filter |
| `--max-event-size` | `EVENTSTORE_MAX_EVENT_SIZE` | `1048576` | Maximum event size, bytes |
| `--max-batch-size` | `EVENTSTORE_MAX_BATCH_SIZE` | `100` | Maximum events per append |
| `--max-body-size` | `EVENTSTORE_MAX_BODY_SIZE` | `4194304` | Maximum HTTP request body, bytes |

Limits must be positive. Batch size is capped at 10,000 and body size at 256 MiB. The bind host is an IP address, not a hostname. An empty API key is treated as unset. A PostgreSQL connection URL is required. The current implementation measures a serialized event including its ID, type, data, and metadata; check the specific release before relying on an exact byte boundary.

Prefer secret injection through your deployment environment rather than placing a key on the command line, where process listings or shell history may expose it. An API key alone does not provide transport encryption.

When authentication is enabled, protected requests must contain exactly one `X-API-Key` header. Requests with duplicate headers are rejected to avoid ambiguous interpretation by intermediaries.

`DATABASE_URL` and `PORT` are fallback variables for the executable; explicit flags and their `EVENTSTORE_` variables take precedence. PostgreSQL URLs must start with `postgres://` or `postgresql://`. For remote databases use TLS (for example, `?sslmode=require` where appropriate). Never log or commit database credentials.
