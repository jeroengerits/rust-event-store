use std::sync::Arc;

use futures::{Stream, TryStreamExt};
use tokio::sync::Notify;

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
    if let sqlx::Error::Database(ref db) = error
        && db.code().as_deref().is_some_and(is_transient_database_code)
    {
        return StoreError::Unavailable;
    }
    tracing::error!(%error, "event store database error");
    StoreError::Internal
}

fn is_transient_database_code(code: &str) -> bool {
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
        for code in [
            "08006", "53300", "40001", "40P01", "55P03", "57P01", "57P02", "57P03",
        ] {
            assert!(is_transient_database_code(code), "{code}");
        }
    }

    #[test]
    fn other_database_errors_are_not_classified_as_unavailable() {
        for code in ["23505", "22021", "42601", "42P01", "57014", ""] {
            assert!(!is_transient_database_code(code), "{code}");
        }
    }
}

#[derive(Clone)]
pub struct Storage {
    postgres: crate::postgres::PostgresStorage,
    pub notify: Arc<Notify>,
}

pub(crate) struct PreparedBatch {
    pub(crate) ids: Vec<String>,
    pub(crate) ids_json: String,
    pub(crate) contents: String,
}

fn prepare_batch(req: &AppendRequest) -> Result<PreparedBatch, StoreError> {
    if req.events.is_empty() {
        return Err(StoreError::Internal);
    }
    let ids: Vec<String> = req
        .events
        .iter()
        .map(|event| event.id.to_string())
        .collect();
    if ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len() {
        return Err(StoreError::DuplicateEventId);
    }
    Ok(PreparedBatch {
        ids_json: serde_json::to_string(&ids).map_err(|_| StoreError::Internal)?,
        contents: serde_json::to_string(&req.events).map_err(|_| StoreError::Internal)?,
        ids,
    })
}

impl Storage {
    pub async fn open(url: &str) -> Result<Self, StoreError> {
        Ok(Self {
            postgres: crate::postgres::PostgresStorage::open(url).await?,
            notify: Arc::new(Notify::new()),
        })
    }

    /// Wrap a PostgreSQL pool whose event store schema is already migrated.
    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self {
            postgres: crate::postgres::PostgresStorage::from_pool(pool),
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn append(
        &self,
        stream: &str,
        req: &AppendRequest,
    ) -> Result<AppendResult, StoreError> {
        let batch = prepare_batch(req)?;
        let result = self.postgres.append(stream, req, &batch).await?;
        self.notify.notify_waiters();
        Ok(result)
    }

    pub async fn read_stream(
        &self,
        stream: &str,
        from: Option<i64>,
        limit: i64,
        backward: bool,
    ) -> Result<Vec<Event>, StoreError> {
        self.postgres
            .read_stream(stream, from, limit, backward)
            .await
    }

    pub async fn read_global(&self, after: i64, limit: i64) -> Result<Vec<Event>, StoreError> {
        self.postgres.read_global(after, limit).await
    }

    pub async fn ready(&self) -> Result<(), StoreError> {
        self.postgres.ready().await
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

pub(crate) async fn collect_page<R>(
    rows: impl Stream<Item = Result<R, sqlx::Error>>,
    decode: impl Fn(R) -> Result<Event, StoreError>,
) -> Result<Vec<Event>, StoreError> {
    futures::pin_mut!(rows);
    let mut events = Vec::new();
    // Reserve room for the response envelope, separators, and transport framing.
    let mut bytes = 1024usize;
    while let Some(row) = rows.try_next().await.map_err(db_error)? {
        let event = decode(row)?;
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
