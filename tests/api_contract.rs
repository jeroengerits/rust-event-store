use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use eventstore::{api, config::Config, storage::Storage};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

fn config() -> Config {
    Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 8080,
        database: "unused".into(),
        api_key: Some("secret".into()),
        log_level: "info".into(),
        max_event_size: 4096,
        max_batch_size: 10,
        max_body_size: 8192,
    }
}

async fn request(
    app: &axum::Router,
    method: &str,
    path: &str,
    key: Option<&str>,
    body: String,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(key) = key {
        builder = builder.header("x-api-key", key);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn responses_disallow_caching_even_for_errors_and_subscriptions() {
    use axum::http::header::CACHE_CONTROL;

    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(dir.path().join("cache.db").to_str().unwrap())
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    let app = api::router(store, config(), shutdown.clone());

    for (path, key, expected_status) in [
        ("/health", None, StatusCode::OK),
        ("/ready", None, StatusCode::OK),
        ("/v1/events", Some("secret"), StatusCode::OK),
        ("/v1/events", None, StatusCode::UNAUTHORIZED),
        ("/missing", Some("secret"), StatusCode::NOT_FOUND),
        ("/v1/subscriptions", Some("secret"), StatusCode::OK),
    ] {
        let mut builder = Request::builder().uri(path);
        if let Some(key) = key {
            builder = builder.header("x-api-key", key);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status, "{path}");
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store", "{path}");
    }
    shutdown.cancel();
}

#[tokio::test]
async fn duplicate_api_key_headers_are_rejected() {
    let store = Storage::open(":memory:").await.unwrap();
    let app = api::router(store, config(), CancellationToken::new());
    for keys in [
        ["secret", "wrong"],
        ["wrong", "secret"],
        ["secret", "secret"],
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("x-api-key", keys[0])
                    .header("x-api-key", keys[1])
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{keys:?}");
    }
    assert_eq!(
        request(&app, "GET", "/v1/events", Some("secret"), String::new())
            .await
            .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn append_rejects_unknown_fields_without_writing_events() {
    let store = Storage::open(":memory:").await.unwrap();
    let app = api::router(store, config(), CancellationToken::new());
    let event = json!({"id": Uuid::now_v7(), "type": "created", "data": {"arbitrary": true}});

    for body in [
        json!({"expected_revison": -1, "events": [event.clone()]}),
        json!({"events": [event.clone(), {"id": Uuid::now_v7(), "type": "created", "data": {}, "metdata": {"actor": "test"}}]}),
    ] {
        let (status, response) = request(
            &app,
            "POST",
            "/v1/streams/a/events",
            Some("secret"),
            body.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(response["error"]["code"], "invalid_request");
    }

    let (_, page) = request(&app, "GET", "/v1/events", Some("secret"), String::new()).await;
    assert_eq!(page["events"], json!([]));
    let (status, _) = request(
        &app,
        "POST",
        "/v1/streams/a/events",
        Some("secret"),
        json!({"events": [event]}).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn json_routes_enforce_auth_validation_conflicts_and_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(dir.path().join("api.db").to_str().unwrap())
        .await
        .unwrap();
    let app = api::router(store, config(), CancellationToken::new());
    assert_eq!(
        request(&app, "GET", "/health", None, String::new()).await.0,
        StatusCode::OK
    );
    assert_eq!(
        request(&app, "GET", "/ready", None, String::new()).await.0,
        StatusCode::OK
    );
    let unauthorized = request(&app, "GET", "/v1/events", None, String::new()).await;
    assert_eq!(unauthorized.0, StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized.1["error"]["code"], "unauthorized");
    let invalid = request(
        &app,
        "POST",
        "/v1/streams/a/events",
        Some("secret"),
        "{".into(),
    )
    .await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST);
    assert_eq!(invalid.1["error"]["code"], "invalid_request");
    let id = Uuid::now_v7();
    let body = json!({"expected_revision": -1, "events": [{"id": id, "type": "created", "data": {"nested": [1, null]}, "metadata": {"actor": "test"}}]}).to_string();
    let (status, result) = request(
        &app,
        "POST",
        "/v1/streams/a/events",
        Some("secret"),
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["revision"], 0);
    assert_eq!(
        request(&app, "POST", "/v1/streams/a/events", Some("secret"), body)
            .await
            .1,
        result
    );
    let stale = json!({"expected_revision": -1, "events": [{"id": Uuid::now_v7(), "type": "created", "data": {}}]}).to_string();
    let conflict = request(&app, "POST", "/v1/streams/a/events", Some("secret"), stale).await;
    assert_eq!(conflict.0, StatusCode::CONFLICT);
    assert_eq!(conflict.1["error"]["code"], "wrong_expected_revision");
    assert_eq!(
        conflict.1["error"]["details"],
        json!({"expected": -1, "actual": 0})
    );
    let (_, read) = request(
        &app,
        "GET",
        "/v1/streams/a/events",
        Some("secret"),
        String::new(),
    )
    .await;
    assert_eq!(read["events"][0]["id"], id.to_string());
    assert_eq!(read["events"][0]["type"], "created");
    assert_eq!(read["events"][0]["data"], json!({"nested": [1, null]}));
    assert_eq!(read["events"][0]["metadata"], json!({"actor": "test"}));
    assert_eq!(read["next_revision"], 1);
    let (_, backward) = request(
        &app,
        "GET",
        "/v1/streams/a/events?direction=backward",
        Some("secret"),
        String::new(),
    )
    .await;
    assert_eq!(backward["events"][0]["revision"], 0);
    assert!(backward["next_revision"].is_null());
    let (_, page) = request(
        &app,
        "GET",
        "/v1/events?limit=1",
        Some("secret"),
        String::new(),
    )
    .await;
    assert_eq!(page["next_after"], page["events"][0]["position"]);
    let (_, empty) = request(
        &app,
        "GET",
        &format!("/v1/events?after={}", page["next_after"]),
        Some("secret"),
        String::new(),
    )
    .await;
    assert_eq!(empty["events"], json!([]));
    assert_eq!(empty["next_after"], page["next_after"]);
    assert_eq!(
        request(
            &app,
            "GET",
            "/v1/events?limit=0",
            Some("secret"),
            String::new()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn subscription_replays_after_last_event_id_and_closes_on_shutdown() {
    use eventstore::domain::{AppendRequest, EventInput};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(dir.path().join("sse.db").to_str().unwrap())
        .await
        .unwrap();
    let events = (0..2)
        .map(|n| EventInput {
            id: Uuid::now_v7(),
            event_type: "created".into(),
            data: json!(n),
            metadata: None,
        })
        .collect();
    let result = store
        .append(
            "a",
            &AppendRequest {
                expected_revision: Some(-1),
                events,
            },
        )
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    let app = api::router(store, config(), shutdown.clone());
    let request = Request::builder()
        .uri("/v1/subscriptions?after=0")
        .header("x-api-key", "secret")
        .header("last-event-id", result.first_position.to_string())
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let mut body = response.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
    assert!(
        text.contains(&format!("id: {}", result.last_position)),
        "{text}"
    );
    assert!(text.contains("event: event"), "{text}");
    assert!(
        !text.contains(&format!("id: {}\n", result.first_position)),
        "{text}"
    );
    shutdown.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .is_none()
    );
}
