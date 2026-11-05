# Storage and recovery

Both backends commit the complete batch atomically. SQLite uses DELETE rollback journaling, `synchronous=FULL`, and `BEGIN IMMEDIATE`; PostgreSQL serializes appends with a transaction advisory lock. Rollback journaling avoids the bundled SQLite version's WAL-reset corruption bug, at the cost of reduced read/write concurrency. Stream revisions start at zero. Global positions increase monotonically; do not assume positions are contiguous. An exact retry of a committed batch returns its original result even if its expected revision has since become stale. Reusing IDs with changed content, order, batch membership, or stream is a conflict.

For SQLite, durability still depends on the OS, filesystem, storage hardware, and correct locking and sync behavior. Use local persistent storage suitable for SQLite; do not assume arbitrary network filesystems provide safe locking. For PostgreSQL, use your provider's backup and restore procedures, verify recovery, and configure durability appropriate for your data. The SQLite backup instructions below do not apply to PostgreSQL.

## Backup

Use SQLite's online backup API or the SQLite CLI `.backup` command against the live database, for example:

```sh
sqlite3 ./data/events.db ".backup './backup/events.db'"
```

Create the backup directory first. Test the backup with `PRAGMA integrity_check` and practice restoring it. A raw copy of only the main database while WAL is active can omit committed data; do not treat it as a consistent backup.

## Restore

Stop every process using the database before restoring. Preserve the old database and its `-journal`, `-wal` and `-shm` sidecars for investigation if needed. Restore a consistent backup to the database path; do not combine a restored main file with stale sidecars from a different database state. Ensure ownership and permissions, start the service, check readiness and verify expected events and consumer checkpoints. Restoring an older snapshot rolls back history; downstream checkpoints beyond it need reconciliation.

## Upgrades

Back up and test restoration before upgrading. Test the new binary and migrations against a copy first, review release-specific migration and rollback instructions, then stop the old instance and start the new one against the persistent database. Startup runs embedded migrations. Do not assume downgrades or simultaneous mixed-version writers are safe. Keep the pre-upgrade backup until validation is complete.

When upgrading from a version that used SQLite WAL, stop **all** processes accessing the database before starting this version. Startup switches the database to DELETE journaling; another open connection can prevent the switch. Do not manually remove WAL or shared-memory files: SQLite must recover and checkpoint existing WAL data during the transition. Back up first, then verify readiness and the expected history after the switch. External writers must not re-enable WAL with the vulnerable SQLite version.
