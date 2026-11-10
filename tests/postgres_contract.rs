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

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing to a dedicated PostgreSQL database"]
async fn postgres_read_pages_respect_byte_budget_and_make_progress() {
    let url = std::env::var("TEST_DATABASE_URL").expect("set TEST_DATABASE_URL");
    let store = Storage::open(&url).await.unwrap();
    let stream = format!("budget-{}", Uuid::now_v7());
    let mut batch = request(Some(-1));
    batch.events[0].data = json!("a".repeat(2_200_000));
    let mut second = request(None).events.remove(0);
    second.data = json!("b".repeat(2_200_000));
    let mut oversized = request(None).events.remove(0);
    oversized.data = json!("c".repeat(4_300_000));
    batch.events.extend([second, oversized]);
    let result = store.append(&stream, &batch).await.unwrap();

    let mut after = result.first_position - 1;
    for event in &batch.events {
        let page = store.read_global(after, 10).await.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, event.id);
        after = page[0].position;
    }

    for revision in 0..3 {
        let page = store
            .read_stream(&stream, Some(revision), 10, false)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].revision, revision);
        assert_eq!(page[0].id, batch.events[revision as usize].id);
    }
    let backwards = store.read_stream(&stream, None, 10, true).await.unwrap();
    assert_eq!(backwards.len(), 1);
    assert_eq!(backwards[0].revision, 2);
}

// Use a dedicated test database: the test creates schema and persists events.
#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing to a dedicated PostgreSQL database"]
async fn postgres_multi_instance_contract() {
    let url = std::env::var("TEST_DATABASE_URL").expect("set TEST_DATABASE_URL");
    let a = Storage::open(&url).await.unwrap();
    let b = Storage::open(&url).await.unwrap();
    let stream = format!("test-{}", Uuid::now_v7());
    let mut invalid = AppendRequest {
        expected_revision: None,
        events: vec![],
    };
    assert!(matches!(
        a.append(&stream, &invalid).await,
        Err(StoreError::Internal)
    ));
    let duplicate_event = request(None).events.remove(0);
    invalid.events = vec![duplicate_event.clone(), duplicate_event];
    assert!(matches!(
        a.append(&stream, &invalid).await,
        Err(StoreError::DuplicateEventId)
    ));
    assert!(
        a.read_stream(&stream, None, 10, false)
            .await
            .unwrap()
            .is_empty()
    );
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
        b.read_global(second_result.first_position - 1, 100)
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
