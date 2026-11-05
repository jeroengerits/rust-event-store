use eventstore::{
    domain::{AppendRequest, EventInput},
    storage::{Storage, StoreError},
};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Barrier;
use uuid::Uuid;

fn event(id: u128) -> EventInput {
    EventInput {
        id: Uuid::from_u128(id),
        event_type: "created".into(),
        data: json!({"number": id, "nested": [true, null]}),
        metadata: Some(json!({"source": "test"})),
    }
}

fn batch(expected_revision: Option<i64>, ids: &[u128]) -> AppendRequest {
    AppendRequest {
        expected_revision,
        events: ids.iter().copied().map(event).collect(),
    }
}

fn database() -> (TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("events.sqlite")
        .to_str()
        .unwrap()
        .to_owned();
    (dir, path)
}

#[tokio::test]
async fn concurrent_compare_and_append_has_one_winner_across_pools() {
    let (_dir, path) = database();
    let a = Storage::open(&path).await.unwrap();
    let b = Storage::open(&path).await.unwrap();
    let barrier = Arc::new(Barrier::new(9));
    let mut tasks = Vec::new();
    for i in 0..8 {
        let store = if i % 2 == 0 { a.clone() } else { b.clone() };
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.append("race", &batch(Some(-1), &[i + 1])).await
        }));
    }
    barrier.wait().await;
    let mut successes = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(result) => {
                successes += 1;
                assert_eq!(result.revision, 0);
            }
            Err(StoreError::WrongRevision {
                expected: -1,
                actual: Some(0),
            }) => (),
            other => panic!("unexpected concurrent result: {other:?}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(
        a.read_stream("race", None, 100, false).await.unwrap().len(),
        1
    );
    assert_eq!(b.read_global(0, 100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn insert_failure_rolls_back_the_whole_batch_and_leaves_ids_reusable() {
    let (_dir, path) = database();
    let store = Storage::open(&path).await.unwrap();
    let pool = sqlx::SqlitePool::connect(&format!("sqlite:{path}"))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_second BEFORE INSERT ON events WHEN NEW.event_type = 'fail' BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        .execute(&pool).await.unwrap();
    let mut request = batch(Some(-1), &[1, 2, 3]);
    request.events[1].event_type = "fail".into();
    assert!(store.append("atomic", &request).await.is_err());
    assert!(store.read_global(0, 100).await.unwrap().is_empty());
    assert!(
        store
            .read_stream("atomic", None, 100, false)
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query("DROP TRIGGER fail_second")
        .execute(&pool)
        .await
        .unwrap();
    let result = store.append("atomic", &request).await.unwrap();
    assert_eq!(result.revision, 2);
    assert_eq!(store.read_global(0, 100).await.unwrap().len(), 3);
}

#[tokio::test]
async fn retry_after_advance_is_exact_and_conflicting_reuse_never_writes() {
    let (_dir, path) = database();
    let store = Storage::open(&path).await.unwrap();
    let original = batch(Some(-1), &[1, 2, 3]);
    let first = store.append("a", &original).await.unwrap();
    store.append("a", &batch(Some(2), &[4])).await.unwrap();
    assert_eq!(store.append("a", &original).await.unwrap(), first);
    let mut changed = original.clone();
    changed.events[2].data = json!({"different": true});
    assert!(matches!(
        store.append("a", &changed).await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.append("other", &original).await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.append("a", &batch(None, &[5, 2])).await,
        Err(StoreError::DuplicateEventId)
    ));
    assert!(matches!(
        store.append("a", &batch(None, &[6, 6])).await,
        Err(StoreError::DuplicateEventId)
    ));
    assert_eq!(store.read_global(0, 100).await.unwrap().len(), 4);
    assert_eq!(
        store
            .append("a", &batch(Some(3), &[5, 6]))
            .await
            .unwrap()
            .revision,
        5
    );
}

#[tokio::test]
async fn restart_preserves_json_batches_and_paginated_global_order() {
    let (_dir, path) = database();
    let store = Storage::open(&path).await.unwrap();
    let request = batch(Some(-1), &[11, 12]);
    let first = store.append("alpha", &request).await.unwrap();
    store.append("beta", &batch(Some(-1), &[13])).await.unwrap();
    store.append("alpha", &batch(Some(1), &[14])).await.unwrap();
    let before = store.read_global(0, 100).await.unwrap();
    drop(store);
    let reopened = Storage::open(&path).await.unwrap();
    reopened.ready().await.unwrap();
    assert_eq!(reopened.append("alpha", &request).await.unwrap(), first);
    let page1 = reopened.read_global(0, 2).await.unwrap();
    let page2 = reopened
        .read_global(page1.last().unwrap().position, 2)
        .await
        .unwrap();
    assert_eq!([page1.clone(), page2.clone()].concat(), before);
    assert!(
        before
            .windows(2)
            .all(|pair| pair[0].position < pair[1].position)
    );
    assert_eq!(
        before.iter().map(|e| e.stream.as_str()).collect::<Vec<_>>(),
        ["alpha", "alpha", "beta", "alpha"]
    );
    assert_eq!(before[0].data, request.events[0].data);
    assert_eq!(before[0].metadata, request.events[0].metadata);
    assert!(
        reopened
            .read_global(page2.last().unwrap().position, 2)
            .await
            .unwrap()
            .is_empty()
    );
    let backward = reopened
        .read_stream("alpha", Some(1), 2, true)
        .await
        .unwrap();
    assert_eq!(
        backward.iter().map(|e| e.revision).collect::<Vec<_>>(),
        [1, 0]
    );
    let forward = reopened
        .read_stream("alpha", Some(1), 1, false)
        .await
        .unwrap();
    assert_eq!(forward[0].revision, 1);
}
