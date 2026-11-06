# Storage and recovery

PostgreSQL commits each batch atomically and serializes appends with a transaction advisory lock. Stream revisions start at zero. Global positions increase monotonically; do not assume positions are contiguous. An exact retry of a committed batch returns its original result even if its expected revision has since become stale. Reusing IDs with changed content, order, batch membership, or stream is a conflict.

Append transactions use a five-second PostgreSQL lock timeout. If a lock cannot be acquired, the request returns `503 storage_unavailable`; retry the same batch with bounded backoff. This bounds lock waits, not the total duration of a database operation or an outage.

## Backup and restore

Use your PostgreSQL provider's backup and restore procedures, configure durability appropriate for your data, and regularly test recovery. After restoring, check readiness, expected events and consumer checkpoints. Restoring an older snapshot rolls back history; downstream checkpoints beyond it need reconciliation.

## Upgrades

Back up and test restoration before upgrading. Test the new binary and migrations against a copy first, review release-specific migration and rollback instructions, and consider compatibility between old and new instances. Startup runs embedded PostgreSQL migrations. Do not assume downgrades or simultaneous mixed-version writers are safe. Keep the pre-upgrade backup until validation is complete.

Previous SQLite databases are not imported automatically. Preserve a consistent backup; migration requires a separately designed and verified process that preserves event IDs, stream revisions, batch retry metadata and global positions.

## Durability validation

CI kills the application after an acknowledged multi-event append, starts it again, and checks that the ordered events and exact retry result survive. A separate disposable-container test kills and restarts PostgreSQL after an acknowledged append and compares the stored events and batches. It also restores a `pg_dump` logical backup into a separate database, compares events and batches, retries the original batch, checks that changed content with the same event ID is rejected without changing the stream, and appends and retries a new batch with higher positions. These checks do not simulate power loss, faulty storage, failover, or your provider's backup and restore process.

An isolated PostgreSQL integration test also terminates the writer's database connection during the second event of a batch. It checks that no events or batch records remain and that the same batch can then be appended successfully.

Before production use, validate those scenarios against an isolated environment matching your provider and settings. Review PostgreSQL `synchronous_commit`, replication and failover guarantees against your acceptable data-loss window. Test database interruption and recovery, failover, backup restoration, idempotent retries and subscription checkpoint reconciliation. Run sustained multi-instance load and outage tests, and monitor readiness, errors, database capacity and backup failures. Record the configuration and results; a successful application restart alone does not establish production durability.
