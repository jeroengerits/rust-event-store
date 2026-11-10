# Subscriptions

`GET /v1/subscriptions` provides a server-sent events (SSE) stream of committed events in global position order. The database is authoritative: in-process notifications only reduce latency, and timeout polling catches missed notifications and writes from other connections or processes.

Delivery is **at least once**. Persist the last successfully processed global position durably and resume from that checkpoint after disconnection. Process an event and advance its checkpoint atomically where possible; otherwise make handlers idempotent using the event ID. A crash between processing and checkpointing can replay an event. Never advance the checkpoint merely because bytes arrived.

Use `after` as an exclusive position (default 0); `Last-Event-ID`, when present, takes precedence. Send at most one `Last-Event-ID` header containing a nonnegative integer; duplicate or comma-separated values are rejected with HTTP 400 to avoid an ambiguous replay checkpoint. `limit` controls each database fetch (accepted range 1–1000, default 100, currently capped internally at 16), not the total lifetime of the stream. Each event is an SSE `event: event` frame whose `id` is the global position and whose `data` is the JSON event. The implementation polls every two seconds when idle and emits SSE keepalives. These wire details may change while implementation is in progress.

Use an SSE-capable client, reconnect with backoff, and configure intermediaries for long-lived unbuffered responses. Automatic `Last-Event-ID` handling tracks received events, which may be ahead of a durable processing checkpoint: reconnect from the latter to avoid loss. The server does not promise exactly-once processing or durable server-side consumer groups.

The macOS Event Store Workspace Timeline uses paginated `GET /v1/events` reads and periodic polling, not this SSE endpoint. Its in-memory position is for display and is not a durable processing checkpoint.

The feed uses bounded database fetches and transport backpressure rather than an unbounded per-subscriber event queue. There is currently no promised slow-consumer disconnect timeout: a stalled connection may remain open until the client, network, proxy, or server closes it. On shutdown the server allows ten seconds for connections to drain, then exits and closes any remaining connections.

Server logs record subscription starts and closes with the process-local active count, and the start and recovery of a database-read failure with its cursor and retry duration. An open connection is not proof that storage is currently healthy; use `/ready` and monitor these logs. Event payloads and credentials are not included in these subscription lifecycle logs.
