use eventstore::{
    domain::{AppendRequest, EventInput},
    storage::{Storage, StoreError},
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Barrier;
use uuid::Uuid;

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn blocked_append_times_out_without_writing_and_can_retry(pool: sqlx::PgPool) {
    let store = Storage::from_pool(pool.clone());
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(721015812890071)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let request = batch(Some(-1), &[100]);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        store.append("blocked", &request),
    )
    .await
    .expect("append must not wait indefinitely");
    assert!(matches!(result, Err(StoreError::Unavailable)));
    blocker.rollback().await.unwrap();
    assert!(store.read_global(0, 10).await.unwrap().is_empty());
    assert_eq!(store.append("blocked", &request).await.unwrap().revision, 0);
}

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

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn concurrent_compare_and_append_has_one_winner_across_pools(pool: sqlx::PgPool) {
    let second_pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let a = Storage::from_pool(pool.clone());
    let b = Storage::from_pool(second_pool);
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

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn insert_failure_rolls_back_the_whole_batch_and_leaves_ids_reusable(pool: sqlx::PgPool) {
    let store = Storage::from_pool(pool.clone());
    sqlx::query("CREATE FUNCTION fail_second_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type = 'fail' THEN RAISE EXCEPTION 'injected failure'; END IF; RETURN NEW; END $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_second BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION fail_second_fn()")
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
    sqlx::query("DROP TRIGGER fail_second ON events")
        .execute(&pool)
        .await
        .unwrap();
    let result = store.append("atomic", &request).await.unwrap();
    assert_eq!(result.revision, 2);
    assert_eq!(store.read_global(0, 100).await.unwrap().len(), 3);
}

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn terminated_connection_rolls_back_partial_batch_and_allows_retry(pool: sqlx::PgPool) {
    use std::time::Duration;

    sqlx::query("CREATE FUNCTION pause_second_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type = 'pause' THEN PERFORM pg_sleep(30); END IF; RETURN NEW; END $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER pause_second BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION pause_second_fn()")
        .execute(&pool).await.unwrap();

    let store = Storage::from_pool(pool.clone());
    let mut request = batch(Some(-1), &[101, 102, 103]);
    request.events[1].event_type = "pause".into();
    let writer = store.clone();
    let pending = request.clone();
    let task = tokio::spawn(async move { writer.append("interrupted", &pending).await });

    // The trigger sleeps only on the second insert, so observing PgSleep proves
    // the transaction has already inserted the first event before interruption.
    let pid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND wait_event = 'PgSleep' AND query LIKE '%INSERT INTO events %'")
                .fetch_optional(&pool).await.unwrap();
            if let Some(pid) = pid {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("writer did not reach the second event");
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(terminated);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(store.read_global(0, 10).await.unwrap().is_empty());
    let batches: i64 = sqlx::query_scalar("SELECT count(*) FROM batches")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(batches, 0);

    sqlx::query("DROP TRIGGER pause_second ON events")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .append("interrupted", &request)
            .await
            .unwrap()
            .revision,
        2
    );
    assert_eq!(store.read_global(0, 10).await.unwrap().len(), 3);
}

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn retry_after_advance_is_exact_and_conflicting_reuse_never_writes(pool: sqlx::PgPool) {
    let store = Storage::from_pool(pool.clone());
    let original = batch(Some(-1), &[1, 2, 3]);
    let first = store.append("a", &original).await.unwrap();
    store.append("a", &batch(Some(2), &[4])).await.unwrap();
    assert_eq!(store.append("a", &original).await.unwrap(), first);
    let mut changed_precondition = original.clone();
    changed_precondition.expected_revision = Some(999);
    assert_eq!(
        store.append("a", &changed_precondition).await.unwrap(),
        first
    );
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

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn bulk_append_preserves_order_content_and_retry_for_large_batch(pool: sqlx::PgPool) {
    let store = Storage::from_pool(pool);
    let ids: Vec<u128> = (1..=512).collect();
    let mut request = batch(Some(-1), &ids);
    for event in request.events.iter_mut().step_by(2) {
        event.metadata = None;
    }
    let result = store.append("large", &request).await.unwrap();
    let events = store.read_global(0, 1000).await.unwrap();
    assert_eq!(events.len(), request.events.len());
    for (index, (actual, expected)) in events.iter().zip(&request.events).enumerate() {
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.revision, index as i64);
        assert_eq!(actual.event_type, expected.event_type);
        assert_eq!(actual.data, expected.data);
        assert_eq!(actual.metadata, expected.metadata);
    }
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].position < pair[1].position)
    );
    assert_eq!(result.first_position, events.first().unwrap().position);
    assert_eq!(result.last_position, events.last().unwrap().position);
    assert_eq!(store.append("large", &request).await.unwrap(), result);
    assert_eq!(store.read_global(0, 1000).await.unwrap().len(), 512);
}

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn restart_preserves_json_batches_and_paginated_global_order(pool: sqlx::PgPool) {
    let store = Storage::from_pool(pool.clone());
    let request = batch(Some(-1), &[11, 12]);
    let first = store.append("alpha", &request).await.unwrap();
    store.append("beta", &batch(Some(-1), &[13])).await.unwrap();
    store.append("alpha", &batch(Some(1), &[14])).await.unwrap();
    let before = store.read_global(0, 100).await.unwrap();
    drop(store);
    let reopened_pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let reopened = Storage::from_pool(reopened_pool);
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

#[sqlx::test(migrations = "./migrations-postgres")]
#[ignore = "requires DATABASE_URL pointing to a disposable PostgreSQL server with CREATE DATABASE privileges"]
async fn schema_enforces_append_only_and_valid_json(pool: sqlx::PgPool) {
    let store = Storage::from_pool(pool.clone());
    store.append("a", &batch(Some(-1), &[1])).await.unwrap();
    assert!(
        sqlx::query("UPDATE events SET revision = 9")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM events")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT INTO events (id, stream, revision, event_type, data, timestamp, batch_id) VALUES ('x', 'a', 1, 'x', 'not json', 'now', 1)").execute(&pool).await.is_err());
    assert!(sqlx::query("INSERT INTO events (id, stream, revision, event_type, data, timestamp, batch_id) VALUES ('x', 'a', -1, 'x', '{}', 'now', 1)").execute(&pool).await.is_err());
    assert_eq!(store.read_global(0, 10).await.unwrap().len(), 1);
}
