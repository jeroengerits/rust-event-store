use chrono::{SecondsFormat, Utc};
use futures::{Stream, TryStreamExt};
use sqlx::{
    PgPool, Row,
    postgres::{PgPoolOptions, PgRow},
};
use uuid::Uuid;

use crate::{
    domain::{AppendRequest, AppendResult, Event},
    storage::{StoreError, db_error},
};

#[derive(Clone)]
pub(crate) struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub async fn open(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(url)
            .await
            .map_err(db_error)?;
        sqlx::migrate!("./migrations-postgres")
            .run(&pool)
            .await
            .map_err(|error| {
                tracing::error!(%error, "PostgreSQL migration failed");
                StoreError::Internal
            })?;
        Ok(Self { pool })
    }

    pub async fn append(
        &self,
        stream: &str,
        req: &AppendRequest,
    ) -> Result<AppendResult, StoreError> {
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
        let ids_json = serde_json::to_string(&ids).map_err(|_| StoreError::Internal)?;
        let contents = serde_json::to_string(&req.events).map_err(|_| StoreError::Internal)?;
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        // A global transaction lock makes sequence allocation and commits ordered across
        // every instance. Sequences alone do not guarantee commit order.
        sqlx::query("SELECT pg_advisory_xact_lock(721015812890071)")
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        let existing = sqlx::query("SELECT b.stream, b.event_ids, b.contents, b.revision, b.first_position, b.last_position FROM events e JOIN batches b ON b.id = e.batch_id WHERE e.id = $1")
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
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)")
                    .bind(id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(db_error)?;
            if exists {
                return Err(StoreError::DuplicateEventId);
            }
        }
        let actual: Option<i64> =
            sqlx::query_scalar("SELECT MAX(revision) FROM events WHERE stream = $1")
                .bind(stream)
                .fetch_one(&mut *tx)
                .await
                .map_err(db_error)?;
        if let Some(expected) = req.expected_revision
            && actual.unwrap_or(-1) != expected
        {
            return Err(StoreError::WrongRevision { expected, actual });
        }
        let initial = actual.unwrap_or(-1);
        let last_revision = initial
            .checked_add(req.events.len() as i64)
            .ok_or(StoreError::Internal)?;
        let batch_id: i64 = sqlx::query_scalar("INSERT INTO batches (stream, event_ids, contents, revision, first_position, last_position) VALUES ($1, $2, $3, $4, 0, 0) RETURNING id")
            .bind(stream).bind(&ids_json).bind(&contents).bind(last_revision).fetch_one(&mut *tx).await.map_err(db_error)?;
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
            last_position = sqlx::query_scalar("INSERT INTO events (id, stream, revision, event_type, data, metadata, timestamp, batch_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING position")
                .bind(&ids[index]).bind(stream).bind(initial + index as i64 + 1).bind(&event.event_type)
                .bind(data).bind(metadata).bind(timestamp).bind(batch_id).fetch_one(&mut *tx).await.map_err(db_error)?;
            if index == 0 {
                first_position = last_position;
            }
        }
        sqlx::query("UPDATE batches SET first_position = $1, last_position = $2 WHERE id = $3")
            .bind(first_position)
            .bind(last_position)
            .bind(batch_id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
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
        let sql = if backward {
            "SELECT * FROM events WHERE stream = $1 AND revision <= $2 ORDER BY revision DESC LIMIT $3"
        } else {
            "SELECT * FROM events WHERE stream = $1 AND revision >= $2 ORDER BY revision ASC LIMIT $3"
        };
        collect(
            sqlx::query(sql)
                .bind(stream)
                .bind(from.unwrap_or(if backward { i64::MAX } else { 0 }))
                .bind(limit)
                .fetch(&self.pool),
        )
        .await
    }

    pub async fn read_global(&self, after: i64, limit: i64) -> Result<Vec<Event>, StoreError> {
        collect(
            sqlx::query("SELECT * FROM events WHERE position > $1 ORDER BY position ASC LIMIT $2")
                .bind(after)
                .bind(limit)
                .fetch(&self.pool),
        )
        .await
    }

    pub async fn ready(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT position FROM events LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(db_error)?;
        Ok(())
    }
}

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

async fn collect(
    rows: impl Stream<Item = Result<PgRow, sqlx::Error>>,
) -> Result<Vec<Event>, StoreError> {
    futures::pin_mut!(rows);
    let mut events = Vec::new();
    let mut bytes = 1024usize;
    while let Some(row) = rows.try_next().await.map_err(db_error)? {
        let id: String = row.try_get("id").map_err(db_error)?;
        let data: String = row.try_get("data").map_err(db_error)?;
        let metadata: Option<String> = row.try_get("metadata").map_err(db_error)?;
        let event = Event {
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
        };
        let mut size = ByteCounter(0);
        serde_json::to_writer(&mut size, &event).map_err(|_| StoreError::Internal)?;
        let next = bytes.saturating_add(size.0).saturating_add(256);
        if !events.is_empty() && next > 4 * 1024 * 1024 {
            break;
        }
        events.push(event);
        bytes = next;
    }
    Ok(events)
}
