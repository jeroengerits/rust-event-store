# Limitations

- SQLite has one writer at a time; sustained write concurrency and long transactions can cause contention or temporary unavailability.
- PostgreSQL appends are also globally serialized to preserve cursor ordering, limiting write throughput. Direct database writers that bypass the advisory lock can break ordering guarantees.
- This is not a replicated or highly available cluster. Disk loss requires recovery from backup; local persistence and backup testing are operator responsibilities.
- Global positions are ordered but should not be assumed gap-free. A restored older backup can invalidate consumer checkpoints.
- Subscription delivery is at least once, not exactly once. Clients must persist checkpoints, reconnect, and make processing idempotent.
- An API key is a shared credential, not per-user authorization or encryption. Use TLS and network controls. Browser `EventSource` clients cannot generally set arbitrary authentication headers; use a suitable client or trusted proxy.
- No event deletion, retention policy, projection engine, schema registry, consumer-group coordination, or server-managed consumer checkpoints is promised by this interface.
- Request and event size limits apply. Large histories should be paginated rather than loaded in one request.
- Percent-encoded stream names may be affected by proxies that decode or reject encoded path separators; verify the full request path in your deployment.
