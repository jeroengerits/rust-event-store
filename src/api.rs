use std::{convert::Infallible, sync::Arc, time::Duration};

use async_stream::stream;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, Request, State, rejection::BytesRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event as SseEvent, KeepAlive, Sse},
    },
    routing::get,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    domain::AppendRequest,
    storage::{Storage, StoreError},
};

struct AppState {
    storage: Storage,
    config: Config,
    shutdown: CancellationToken,
}

type Shared = Arc<AppState>;

pub fn router(storage: Storage, config: Config, shutdown: CancellationToken) -> Router {
    let max_body_size = config.max_body_size;
    let state = Arc::new(AppState {
        storage,
        config,
        shutdown,
    });
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/streams/{stream}/events", get(read_stream).post(append))
        .route("/v1/events", get(read_global))
        .route("/v1/subscriptions", get(subscribe))
        .fallback(|| async {
            api_error(StatusCode::NOT_FOUND, "invalid_request", "Route not found")
        })
        .method_not_allowed_fallback(|| async {
            api_error(
                StatusCode::METHOD_NOT_ALLOWED,
                "invalid_request",
                "Method not allowed",
            )
        })
        .layer(DefaultBodyLimit::max(max_body_size))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authenticate_and_log,
        ))
        .with_state(state)
}

async fn authenticate_and_log(
    State(state): State<Shared>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let public = request.uri().path() == "/health" || request.uri().path() == "/ready";
    let authorized = public
        || match &state.config.api_key {
            None => true,
            Some(key) => {
                let supplied = request
                    .headers()
                    .get("x-api-key")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let supplied_hash = Sha256::digest(supplied.as_bytes());
                let key_hash = Sha256::digest(key.as_bytes());
                bool::from(supplied_hash.as_slice().ct_eq(key_hash.as_slice()))
            }
        };
    let mut response = if authorized {
        next.run(request).await
    } else {
        api_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Invalid or missing API key",
        )
    };
    // Event data, authorization failures, and probe results must not be cached
    // by browsers or intermediaries, including the Cloud edge cache.
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    tracing::info!(%method, status = response.status().as_u16(), "HTTP request");
    response
}

fn api_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error": {"code": code, "message": message}})),
    )
        .into_response()
}

fn store_error(error: StoreError) -> Response {
    match error {
        StoreError::WrongRevision { expected, actual } => (StatusCode::CONFLICT, Json(json!({"error": {"code": "wrong_expected_revision", "message": "Stream revision does not match", "details": {"expected": expected, "actual": actual}}}))).into_response(),
        StoreError::DuplicateEventId => api_error(StatusCode::CONFLICT, "duplicate_event_id", "Event ID already exists"),
        StoreError::IdempotencyConflict => api_error(StatusCode::CONFLICT, "idempotency_conflict", "Event ID conflicts with an existing event"),
        StoreError::Unavailable => api_error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable", "Storage is unavailable"),
        StoreError::Internal => api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", "Internal storage error"),
    }
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn ready(State(state): State<Shared>) -> Response {
    match state.storage.ready().await {
        Ok(_) => Json(json!({"status": "ready"})).into_response(),
        Err(error) => store_error(error),
    }
}

fn valid_stream(stream: &str) -> bool {
    !stream.is_empty() && stream.len() <= 255 && !stream.chars().any(char::is_control)
}

async fn append(
    State(state): State<Shared>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let Path(stream) = match path {
        Ok(path) => path,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid stream path",
            );
        }
    };
    if !valid_stream(&stream) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Invalid stream name",
        );
    }
    let body = match body {
        Ok(body) => body,
        Err(error) if error.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return api_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "Request body exceeds the configured limit",
            );
        }
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Could not read request body",
            );
        }
    };
    let request: AppendRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid JSON or append request",
            );
        }
    };
    if request
        .expected_revision
        .is_some_and(|revision| revision < -1)
        || request.events.is_empty()
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Expected revision must be at least -1 and the batch must not be empty",
        );
    }
    if request.events.len() > state.config.max_batch_size {
        return api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "batch_too_large",
            "Batch exceeds the configured limit",
        );
    }
    for event in &request.events {
        if event.id.get_version_num() != 7
            || event.id.get_variant() != uuid::Variant::RFC4122
            || event.event_type.trim().is_empty()
            || event.event_type.len() > 255
            || event.event_type.chars().any(char::is_control)
        {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_event",
                "Each event requires a UUIDv7 ID and nonempty event type",
            );
        }
        // Measure the complete event as supplied, including metadata.
        let size = serde_json::to_vec(event)
            .map(|v| v.len())
            .unwrap_or(usize::MAX);
        if size > state.config.max_event_size {
            return api_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "event_too_large",
                "Event exceeds the configured limit",
            );
        }
    }
    match state.storage.append(&stream, &request).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(error) => store_error(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamQuery {
    from: Option<i64>,
    limit: Option<i64>,
    direction: Option<Direction>,
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Direction {
    Forward,
    Backward,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalQuery {
    after: Option<i64>,
    limit: Option<i64>,
}

fn page_limit(limit: Option<i64>) -> Option<i64> {
    let limit = limit.unwrap_or(100);
    (1..=1000).contains(&limit).then_some(limit)
}

async fn read_stream(
    State(state): State<Shared>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    query: Result<Query<StreamQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Path(stream) = match path {
        Ok(path) => path,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid stream path",
            );
        }
    };
    if !valid_stream(&stream) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Invalid stream name",
        );
    }
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid query parameters",
            );
        }
    };
    let limit = match page_limit(query.limit) {
        Some(limit) => limit,
        None => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Limit must be between 1 and 1000",
            );
        }
    };
    if query.from.is_some_and(|from| from < 0) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Cursor must be nonnegative",
        );
    }
    let backward = matches!(query.direction, Some(Direction::Backward));
    match state
        .storage
        .read_stream(&stream, query.from, limit, backward)
        .await
    {
        Ok(events) => {
            let next = events.last().and_then(|event| {
                if backward {
                    event.revision.checked_sub(1).filter(|v| *v >= 0)
                } else {
                    event.revision.checked_add(1)
                }
            });
            Json(json!({"stream": stream, "events": events, "next_revision": next})).into_response()
        }
        Err(error) => store_error(error),
    }
}

async fn read_global(
    State(state): State<Shared>,
    query: Result<Query<GlobalQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid query parameters",
            );
        }
    };
    let limit = match page_limit(query.limit) {
        Some(limit) => limit,
        None => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Limit must be between 1 and 1000",
            );
        }
    };
    let after = query.after.unwrap_or(0);
    if after < 0 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Cursor must be nonnegative",
        );
    }
    match state.storage.read_global(after, limit).await {
        Ok(events) => {
            let next = events.last().map(|event| event.position).unwrap_or(after);
            Json(json!({"events": events, "next_after": next})).into_response()
        }
        Err(error) => store_error(error),
    }
}

async fn subscribe(
    State(state): State<Shared>,
    headers: HeaderMap,
    query: Result<Query<GlobalQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid query parameters",
            );
        }
    };
    let limit = match page_limit(query.limit) {
        Some(limit) => limit.min(16),
        None => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Limit must be between 1 and 1000",
            );
        }
    };
    let mut cursor = match headers.get("last-event-id") {
        Some(value) => match value.to_str().ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(value) if value >= 0 => value,
            _ => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "Invalid Last-Event-ID",
                );
            }
        },
        None => query.after.unwrap_or(0),
    };
    if cursor < 0 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Cursor must be nonnegative",
        );
    }
    let output = stream! {
        'subscription: loop {
            // Register before querying to avoid losing a wakeup between the query and wait.
            let notified = state.storage.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let result = tokio::select! {
                _ = state.shutdown.cancelled() => break,
                result = state.storage.read_global(cursor, limit) => result,
            };
            match result {
                Ok(events) if !events.is_empty() => {
                    for event in events {
                        let position = event.position;
                        if position <= cursor { break 'subscription; }
                        let value = match serde_json::to_string(&event) { Ok(value) => value, Err(_) => break 'subscription };
                        cursor = position;
                        yield Ok::<_, Infallible>(SseEvent::default().id(position.to_string()).event("event").data(value));
                    }
                }
                Ok(_) => {
                    tokio::select! {
                        _ = state.shutdown.cancelled() => break,
                        _ = &mut notified => {},
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                    }
                }
                Err(_) => {
                    // Avoid exposing storage internals and retry with a bounded delay.
                    tokio::select! {
                        _ = state.shutdown.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                    }
                }
            }
        }
    };
    Sse::new(output)
        .keep_alive(KeepAlive::default())
        .into_response()
}
