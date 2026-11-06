# Architecture

The service exposes an HTTP API over one PostgreSQL database. A stream is an ordered sequence of events with zero-based revisions. Each committed event also receives a database-wide, monotonically increasing position. Events carry a client-generated UUIDv7 ID, type, JSON data, optional JSON metadata, and a server timestamp.

An append is one transaction: the service checks for an existing batch and expected revision, writes all events and their batch record, then commits. PostgreSQL uses a database-wide transaction advisory lock to serialize appends, including across instances, so global positions follow commit order. All writers must use this protocol. Reads query committed database state. Notifications after commits are local hints, not the source of truth: subscribers query the database and periodically poll even when a notification is missed.

The service is a single-node building block, not a distributed consensus system. Consumers own their checkpoints and must tolerate replay. See [storage](storage.md) and [subscriptions](subscriptions.md).
