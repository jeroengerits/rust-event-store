use eventstore::{
    domain::{AppendRequest, EventInput},
    storage::{Storage, StoreError},
};
use serde_json::json;
use uuid::Uuid;

fn request(expected_revision: Option<i64>) -> AppendRequest {
    AppendRequest {
        expected_revision,
        events: vec![EventInput {
            id: Uuid::now_v7(),
            event_type: "created".into(),
            data: json!({"value": 42}),
            metadata: None,
        }],
    }
}

// Use a dedicated test database: the test creates schema and persists events.
#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing to a dedicated PostgreSQL database"]
async fn postgres_multi_instance_contract() {
    let url = std::env::var("TEST_DATABASE_URL").expect("set TEST_DATABASE_URL");
    let a = Storage::open(&url).await.unwrap();
    let b = Storage::open(&url).await.unwrap();
    let stream = format!("test-{}", Uuid::now_v7());
    let one = request(Some(-1));
    let two = request(Some(-1));
    let (first, second) = tokio::join!(a.append(&stream, &one), b.append(&stream, &two));
    let (winner, original) = match (first, second) {
        (
            Ok(result),
            Err(StoreError::WrongRevision {
                actual: Some(0), ..
            }),
        ) => (result, one),
        (
            Err(StoreError::WrongRevision {
                actual: Some(0), ..
            }),
            Ok(result),
        ) => (result, two),
        other => panic!("unexpected race outcome: {other:?}"),
    };
    assert_eq!(b.append(&stream, &original).await.unwrap(), winner);
    let mut conflict = original.clone();
    conflict.events[0].data = json!({"changed": true});
    assert!(matches!(
        a.append(&stream, &conflict).await,
        Err(StoreError::IdempotencyConflict)
    ));
    let mut duplicate = request(Some(0));
    duplicate.events.push(original.events[0].clone());
    assert!(matches!(
        a.append(&stream, &duplicate).await,
        Err(StoreError::DuplicateEventId)
    ));
    let next = request(Some(0));
    let second_result = b.append(&stream, &next).await.unwrap();
    assert!(second_result.first_position > winner.last_position);
    let events = a.read_stream(&stream, None, 10, false).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].id, original.events[0].id);
    assert_eq!(events[1].id, next.events[0].id);
    assert_eq!(
        b.read_global(winner.last_position, 100)
            .await
            .unwrap()
            .iter()
            .find(|event| event.id == next.events[0].id)
            .unwrap()
            .position,
        second_result.first_position
    );
    a.ready().await.unwrap();
    drop(a);
    assert_eq!(
        Storage::open(&url)
            .await
            .unwrap()
            .append(&stream, &original)
            .await
            .unwrap(),
        winner
    );
}
