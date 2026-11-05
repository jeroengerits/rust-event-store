use std::{str::FromStr, sync::Arc, time::Duration};

use chrono::{SecondsFormat, Utc};
use futures::{Stream, TryStreamExt};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::domain::{AppendRequest, AppendResult, Event};

const READ_BUDGET: usize = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("wrong revision: expected {expected}, actual {actual:?}")]
    WrongRevision { expected: i64, actual: Option<i64> },
    #[error("event ID already exists")]
    DuplicateEventId,
    #[error("idempotency conflict")]
    IdempotencyConflict,
    #[error("event store unavailable")]
    Unavailable,
    #[error("internal event store error")]
    Internal,
}

pub(crate) fn db_error(error: sqlx::Error) -> StoreError {
    if matches!(
        error,
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_)
    ) {
        return StoreError::Unavailable;
    }
    if let sqlx::Error::Database(ref db) = error {
        let postgres = db
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .is_some();
        if db
            .code()
            .as_deref()
            .is_some_and(|code| is_transient_database_code(code, postgres))
        {
            return StoreError::Unavailable;
        }
    }
    tracing::error!(%error, "event store database error");
    StoreError::Internal
}

fn is_transient_database_code(code: &str, postgres: bool) -> bool {
    // SQLite extended result codes retain the primary code in their low byte.
    if !postgres {
        return code
            .parse::<i32>()
            .is_ok_and(|code| code & 0xff == 5 || code & 0xff == 6);
    }
    // PostgreSQL SQLSTATE: connection exceptions, resource exhaustion,
    // serialization/deadlock/lock failures, and server shutdown or startup.
    code.starts_with("08")
        || code.starts_with("53")
        || matches!(
            code,
            "40001" | "40P01" | "55P03" | "57P01" | "57P02" | "57P03"
        )
}

#[cfg(test)]
mod error_tests {
    use super::is_transient_database_code;

    #[test]
    fn transient_database_codes_are_classified_as_unavailable() {
        for code in ["5", "6", "517", "262"] {
            assert!(is_transient_database_code(code, false), "{code}");
        }
        for code in [
            "08006", "53300", "40001", "40P01", "55P03", "57P01", "57P02", "57P03",
        ] {
            assert!(is_transient_database_code(code, true), "{code}");
        }
    }

    #[test]
    fn other_database_errors_are_not_classified_as_unavailable() {
        for code in ["1", "19", ""] {
            assert!(!is_transient_database_code(code, false), "{code}");
        }
        for code in ["23505", "22021", "42601", "42P01", "57014", ""] {
            assert!(!is_transient_database_code(code, true), "{code}");
        }
    }
}

#[derive(Clone)]
pub struct Storage {
    pool: Option<SqlitePool>,
    postgres: Option<crate::postgres::PostgresStorage>,
    pub notify: Arc<Notify>,
}

impl Storage {
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        if path.starts_with("postgres://") || path.starts_with("postgresql://") {
            return Ok(Self {
                pool: None,
                postgres: Some(crate::postgres::PostgresStorage::open(path).await?),
                notify: Arc::new(Notify::new()),
            });
        }
        let options = if path.starts_with("sqlite:") {
            SqliteConnectOptions::from_str(path).map_err(db_error)?
        } else {
            SqliteConnectOptions::new().filename(path)
        }
        .create_if_missing(true)
        // The bundled SQLite version is affected by the WAL-reset corruption bug.
        // Rollback journaling avoids the affected WAL code path.
        .journal_mode(SqliteJournalMode::Delete)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5))
        .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(if path == ":memory:" || path.contains(":memory:") {
                1
            } else {
                8
            })
            .connect_with(options)
            .await
            .map_err(db_error)?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|error| {
                tracing::error!(%error, "event store migration failed");
                StoreError::Internal
            })?;
        Ok(Self {
            pool: Some(pool),
            postgres: None,
            notify: Arc::new(Notify::new()),
        })
    }

    pub async fn append(
        &self,
        stream: &str,
        req: &AppendRequest,
    ) -> Result<AppendResult, StoreError> {
        if let Some(postgres) = &self.postgres {
            let result = postgres.append(stream, req).await?;
            self.notify.notify_waiters();
            return Ok(result);
        }
        if req.events.is_empty() {
            return Err(StoreError::Internal);
        }
        let ids: Vec<String> = req
            .events
            .iter()
            .map(|event| event.id.to_string())
            .collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        if unique.len() != ids.len() {
            return Err(StoreError::DuplicateEventId);
        }
        let ids_json = serde_json::to_string(&ids).map_err(|_| StoreError::Internal)?;
        let contents = serde_json::to_string(&req.events).map_err(|_| StoreError::Internal)?;
        let mut tx = self
            .pool
            .as_ref()
            .expect("SQLite pool")
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;

        // A matching batch is returned even if the caller's expected revision is now stale.
        let existing = sqlx::query("SELECT b.stream, b.event_ids, b.contents, b.revision, b.first_position, b.last_position FROM events e JOIN batches b ON b.id = e.batch_id WHERE e.id = ?")
            .bind(&ids[0]).fetch_optional(&mut *tx).await.map_err(db_error)?;
        if let Some(row) = existing {
            let previous_ids: String = row.try_get("event_ids").map_err(db_error)?;
            if previous_ids != ids_json {
                return Err(StoreError::DuplicateEventId);
            }
            let previous_stream: String = row.try_get("stream").map_err(db_error)?;
            let previous_contents: String = row.try_get("contents").map_err(db_error)?;
            if previous_stream != stream || previous_contents != contents {
                return Err(StoreError::IdempotencyConflict);
            }
            return Ok(AppendResult {
                stream: previous_stream,
                revision: row.try_get("revision").map_err(db_error)?,
                first_position: row.try_get("first_position").map_err(db_error)?,
                last_position: row.try_get("last_position").map_err(db_error)?,
            });
        }
        for id in &ids[1..] {
            let exists: i64 =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM events WHERE id = ?)")
                    .bind(id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(db_error)?;
            if exists != 0 {
                return Err(StoreError::DuplicateEventId);
            }
        }
        let actual: Option<i64> =
            sqlx::query_scalar("SELECT MAX(revision) FROM events WHERE stream = ?")
                .bind(stream)
                .fetch_one(&mut *tx)
                .await
                .map_err(db_error)?;
        if let Some(expected) = req.expected_revision {
            // -1 represents a stream that does not yet exist.
            if actual.unwrap_or(-1) != expected {
                return Err(StoreError::WrongRevision { expected, actual });
            }
        }
        let initial = actual.unwrap_or(-1);
        let last_revision = initial
            .checked_add(req.events.len() as i64)
            .ok_or(StoreError::Internal)?;
        let batch_id = sqlx::query("INSERT INTO batches (stream, event_ids, contents, revision, first_position, last_position) VALUES (?, ?, ?, ?, 0, 0)")
            .bind(stream).bind(&ids_json).bind(&contents).bind(last_revision)
            .execute(&mut *tx).await.map_err(db_error)?.last_insert_rowid();
        let mut first_position = 0;
        let mut last_position = 0;
        for (index, event) in req.events.iter().enumerate() {
            let data = serde_json::to_string(&event.data).map_err(|_| StoreError::Internal)?;
            let metadata = event
                .metadata
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|_| StoreError::Internal)?;
            let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            let result = sqlx::query("INSERT INTO events (id, stream, revision, event_type, data, metadata, timestamp, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(&ids[index]).bind(stream).bind(initial + index as i64 + 1).bind(&event.event_type)
                .bind(data).bind(metadata).bind(timestamp).bind(batch_id)
                .execute(&mut *tx).await.map_err(db_error)?;
            last_position = result.last_insert_rowid();
            if index == 0 {
                first_position = last_position;
            }
        }
        sqlx::query("UPDATE batches SET first_position = ?, last_position = ? WHERE id = ?")
            .bind(first_position)
            .bind(last_position)
            .bind(batch_id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        self.notify.notify_waiters();
        Ok(AppendResult {
            stream: stream.to_owned(),
            revision: last_revision,
            first_position,
            last_position,
        })
    }

    pub async fn read_stream(
        &self,
        stream: &str,
        from: Option<i64>,
        limit: i64,
        backward: bool,
    ) -> Result<Vec<Event>, StoreError> {
        if let Some(postgres) = &self.postgres {
            return postgres.read_stream(stream, from, limit, backward).await;
        }
        let sql = if backward {
            "SELECT * FROM events WHERE stream = ? AND revision <= ? ORDER BY revision DESC LIMIT ?"
        } else {
            "SELECT * FROM events WHERE stream = ? AND revision >= ? ORDER BY revision ASC LIMIT ?"
        };
        let rows = sqlx::query(sql)
            .bind(stream)
            .bind(from.unwrap_or(if backward { i64::MAX } else { 0 }))
            .bind(limit)
            .fetch(self.pool.as_ref().expect("SQLite pool"));
        collect_page(rows).await
    }

    pub async fn read_global(&self, after: i64, limit: i64) -> Result<Vec<Event>, StoreError> {
        if let Some(postgres) = &self.postgres {
            return postgres.read_global(after, limit).await;
        }
        let rows =
            sqlx::query("SELECT * FROM events WHERE position > ? ORDER BY position ASC LIMIT ?")
                .bind(after)
                .bind(limit)
                .fetch(self.pool.as_ref().expect("SQLite pool"));
        collect_page(rows).await
    }

    pub async fn ready(&self) -> Result<(), StoreError> {
        if let Some(postgres) = &self.postgres {
            return postgres.ready().await;
        }
        sqlx::query("SELECT position FROM events LIMIT 1")
            .fetch_optional(self.pool.as_ref().expect("SQLite pool"))
            .await
            .map_err(db_error)?;
        Ok(())
    }
}

// Count the actual JSON encoding, including escaping, without allocating another copy.
struct ByteCounter(usize);
impl std::io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn collect_page(
    rows: impl Stream<Item = Result<sqlx::sqlite::SqliteRow, sqlx::Error>>,
) -> Result<Vec<Event>, StoreError> {
    futures::pin_mut!(rows);
    let mut events = Vec::new();
    // Reserve room for the response envelope, separators, and transport framing.
    let mut bytes = 1024usize;
    while let Some(row) = rows.try_next().await.map_err(db_error)? {
        let event = decode_event(row)?;
        let mut size = ByteCounter(0);
        serde_json::to_writer(&mut size, &event).map_err(|_| StoreError::Internal)?;
        let next = bytes.saturating_add(size.0).saturating_add(256);
        if !events.is_empty() && next > READ_BUDGET {
            break;
        }
        events.push(event);
        bytes = next;
    }
    Ok(events)
}

fn decode_event(row: sqlx::sqlite::SqliteRow) -> Result<Event, StoreError> {
    let id: String = row.try_get("id").map_err(db_error)?;
    let data: String = row.try_get("data").map_err(db_error)?;
    let metadata: Option<String> = row.try_get("metadata").map_err(db_error)?;
    Ok(Event {
        id: Uuid::parse_str(&id).map_err(|_| StoreError::Internal)?,
        stream: row.try_get("stream").map_err(db_error)?,
        revision: row.try_get("revision").map_err(db_error)?,
        position: row.try_get("position").map_err(db_error)?,
        event_type: row.try_get("event_type").map_err(db_error)?,
        data: serde_json::from_str(&data).map_err(|_| StoreError::Internal)?,
        metadata: metadata
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(|_| StoreError::Internal)?,
        timestamp: row.try_get("timestamp").map_err(db_error)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::EventInput;
    use serde_json::json;

    fn event(id: u128) -> EventInput {
        EventInput {
            id: Uuid::from_u128(id),
            event_type: "created".into(),
            data: json!({"id": id}),
            metadata: None,
        }
    }

    #[tokio::test]
    async fn append_retry_conflicts_and_ordered_reads() {
        let store = Storage::open(":memory:").await.unwrap();
        let req = AppendRequest {
            expected_revision: Some(-1),
            events: vec![event(1), event(2)],
        };
        let result = store.append("a", &req).await.unwrap();
        assert_eq!(result.revision, 1);
        assert_eq!(store.append("a", &req).await.unwrap(), result);
        let mut changed = req.clone();
        changed.events[0].data = json!({"changed": true});
        assert!(matches!(
            store.append("a", &changed).await,
            Err(StoreError::IdempotencyConflict)
        ));
        assert!(matches!(
            store.append("b", &req).await,
            Err(StoreError::IdempotencyConflict)
        ));
        changed.events.reverse();
        assert!(matches!(
            store.append("a", &changed).await,
            Err(StoreError::DuplicateEventId)
        ));
        let stale = AppendRequest {
            expected_revision: Some(-1),
            events: vec![event(3)],
        };
        assert!(matches!(
            store.append("a", &stale).await,
            Err(StoreError::WrongRevision {
                actual: Some(1),
                ..
            })
        ));
        let next = AppendRequest {
            expected_revision: Some(1),
            events: vec![event(3)],
        };
        store.append("a", &next).await.unwrap();
        let forward = store.read_stream("a", Some(1), 10, false).await.unwrap();
        assert_eq!(
            forward.iter().map(|e| e.revision).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let backward = store.read_stream("a", Some(1), 10, true).await.unwrap();
        assert_eq!(
            backward.iter().map(|e| e.revision).collect::<Vec<_>>(),
            vec![1, 0]
        );
        let global = store.read_global(result.first_position, 10).await.unwrap();
        assert_eq!(global.len(), 2);
        assert_eq!(global[0].id, Uuid::from_u128(2));
        store.ready().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_batch_is_atomic() {
        let store = Storage::open(":memory:").await.unwrap();
        let first = AppendRequest {
            expected_revision: None,
            events: vec![event(1)],
        };
        store.append("a", &first).await.unwrap();
        let duplicate = AppendRequest {
            expected_revision: None,
            events: vec![event(2), event(1)],
        };
        assert!(matches!(
            store.append("a", &duplicate).await,
            Err(StoreError::DuplicateEventId)
        ));
        assert_eq!(store.read_global(0, 10).await.unwrap().len(), 1);
        let valid = AppendRequest {
            expected_revision: Some(0),
            events: vec![event(2)],
        };
        assert_eq!(store.append("a", &valid).await.unwrap().revision, 1);
    }

    #[tokio::test]
    async fn concurrent_expected_revision_only_commits_once() {
        let store = Storage::open(":memory:").await.unwrap();
        let one = AppendRequest {
            expected_revision: Some(-1),
            events: vec![event(1)],
        };
        let two = AppendRequest {
            expected_revision: Some(-1),
            events: vec![event(2)],
        };
        let (a, b) = tokio::join!(store.append("a", &one), store.append("a", &two));
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        assert_eq!(store.read_global(0, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn schema_enforces_append_only_and_valid_json() {
        let store = Storage::open(":memory:").await.unwrap();
        store
            .append(
                "a",
                &AppendRequest {
                    expected_revision: None,
                    events: vec![event(1)],
                },
            )
            .await
            .unwrap();
        assert!(
            sqlx::query("UPDATE events SET revision = 9")
                .execute(store.pool.as_ref().unwrap())
                .await
                .is_err()
        );
        assert!(
            sqlx::query("DELETE FROM events")
                .execute(store.pool.as_ref().unwrap())
                .await
                .is_err()
        );
        assert!(sqlx::query("INSERT INTO events (id, stream, revision, event_type, data, timestamp, batch_id) VALUES ('x', 'a', 1, 'x', 'not json', 'now', 1)").execute(store.pool.as_ref().unwrap()).await.is_err());
        assert!(sqlx::query("INSERT INTO events (id, stream, revision, event_type, data, timestamp, batch_id) VALUES ('x', 'a', -1, 'x', '{}', 'now', 1)").execute(store.pool.as_ref().unwrap()).await.is_err());
        assert_eq!(store.read_global(0, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn page_budget_keeps_cursors_and_allows_oversized_first_event() {
        let store = Storage::open(":memory:").await.unwrap();
        let mut first = event(1);
        first.data = json!("x".repeat(READ_BUDGET / 2 + 1));
        let mut second = event(2);
        second.data = first.data.clone();
        let mut oversized = event(3);
        oversized.data = json!("y".repeat(READ_BUDGET + 1));
        store
            .append(
                "a",
                &AppendRequest {
                    expected_revision: None,
                    events: vec![first, second, oversized],
                },
            )
            .await
            .unwrap();

        let page = store.read_global(0, 10).await.unwrap();
        assert_eq!(page.len(), 1);
        let next = store.read_global(page[0].position, 10).await.unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].revision, 1);
        let last = store.read_global(next[0].position, 10).await.unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].revision, 2);
        let backward = store.read_stream("a", None, 10, true).await.unwrap();
        assert_eq!(backward.len(), 1);
        assert_eq!(backward[0].revision, 2);
        let following = store
            .read_stream("a", Some(backward[0].revision - 1), 10, true)
            .await
            .unwrap();
        assert_eq!(following.len(), 1);
        assert_eq!(following[0].revision, 1);
    }
}
