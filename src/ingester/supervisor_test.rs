use super::*;
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
use tokio::sync::oneshot;

#[rstest::rstest]
#[tokio::test]
async fn worker_failure_restarts_from_the_committed_checkpoint(#[values(false, true)] panic: bool) {
    let db = Arc::new(Database::connect("sqlite::memory:").await.unwrap());
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "CREATE TABLE blocks (slot INTEGER PRIMARY KEY)".into(),
    ))
    .await
    .unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let (done_tx, done_rx) = oneshot::channel();
    let test_observed = observed.clone();
    let task = tokio::spawn(async move {
        let attempt_db = db.clone();
        let mut done_tx = Some(done_tx);
        supervise(db.as_ref(), 7, move |slot| {
            let observed = test_observed.clone();
            let db = attempt_db.clone();
            let done_tx = if slot == 11 { done_tx.take() } else { None };
            async move {
                observed.lock().unwrap().push(slot);
                if slot == 7 {
                    db.execute(Statement::from_string(
                        DatabaseBackend::Sqlite,
                        "INSERT INTO blocks VALUES (11)".into(),
                    ))
                    .await
                    .unwrap();
                    if panic {
                        panic!("injected worker panic after commit");
                    }
                    return Ok(());
                }
                if let Some(done_tx) = done_tx {
                    let _ = done_tx.send(());
                }
                futures::future::pending::<Result<(), IngesterError>>().await
            }
        })
        .await;
    });
    tokio::time::timeout(Duration::from_secs(5), done_rx)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        *observed.lock().unwrap(),
        vec![7, 11],
        "restart must read the durable checkpoint, not the last fetched slot"
    );
}

#[test]
fn quiet_caught_up_indexing_is_healthy_but_unresolved_work_is_not() {
    let mut progress = Progress::default();
    assert_eq!(progress.health_error(false), None);
    progress.source_failure = Some((
        Instant::now() - Duration::from_secs(11),
        "bad credentials".into(),
    ));
    assert_eq!(
        progress.health_error(false),
        None,
        "no work is waiting on a quiet chain"
    );
    assert_eq!(progress.health_error(true), Some("bad credentials".into()));
    progress.failure = Some("worker exited".into());
    assert_eq!(progress.health_error(false), Some("worker exited".into()));
}
