use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::FutureExt;
use once_cell::sync::Lazy;
use sea_orm::DatabaseConnection;

use super::{
    error::IngesterError,
    fetchers::BlockStreamConfig,
    indexer::{fetch_last_indexed_slot_with_infinite_retry, index_block_stream},
    retry::Backoff,
};

#[derive(Default)]
struct Progress {
    failure: Option<String>,
    source_failure: Option<(Instant, String)>,
}

impl Progress {
    fn health_error(&self, behind: bool) -> Option<String> {
        self.failure.clone().or_else(|| {
            self.source_failure
                .as_ref()
                .filter(|(since, _)| behind && since.elapsed() > Duration::from_secs(10))
                .map(|(_, message)| message.clone())
        })
    }
}

static PROGRESS: Lazy<Mutex<Progress>> = Lazy::new(|| Mutex::new(Progress::default()));

pub fn record_commit() {
    *PROGRESS.lock().unwrap_or_else(|error| error.into_inner()) = Progress::default();
}

pub fn health_error(behind: bool) -> Option<String> {
    PROGRESS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .health_error(behind)
}

pub fn record_source_failure(error: &IngesterError) {
    if !error.is_retryable() {
        PROGRESS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .source_failure
            .get_or_insert_with(|| (Instant::now(), error.to_string()));
    }
}

/// A restart discards speculative fetch state and reloads the durable database
/// checkpoint. Catching a task panic here also keeps shutdown cancellation
/// attached to the actual indexing future, without detached child workers.
pub fn spawn_indexer(
    config: BlockStreamConfig,
    db: Arc<DatabaseConnection>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let initial = config.last_indexed_slot;
        let attempt_db = db.clone();
        supervise(db.as_ref(), initial, move |slot| {
            let mut config = config.clone();
            config.last_indexed_slot = slot;
            let db = attempt_db.clone();
            async move {
                let stream = config.load_block_stream();
                index_block_stream(stream, db, config.rpc_client.clone(), slot, None).await
            }
        })
        .await;
    })
}

async fn supervise<F, Fut>(db: &DatabaseConnection, initial: u64, mut attempt: F)
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<(), IngesterError>>,
{
    let mut backoff = Backoff::default();
    let mut previous_slot = initial;
    loop {
        let slot = fetch_last_indexed_slot_with_infinite_retry(db)
            .await
            .and_then(|slot| u64::try_from(slot).ok())
            .unwrap_or(initial);
        if slot > previous_slot {
            backoff = Backoff::default();
        }
        previous_slot = slot;
        let outcome = AssertUnwindSafe(async { attempt(slot).await })
            .catch_unwind()
            .await;
        let permanent = matches!(&outcome, Ok(Err(error)) if !error.is_retryable());
        let message = match outcome {
            Ok(Err(error)) => error.to_string(),
            Ok(Ok(())) => "Live block stream ended unexpectedly".to_string(),
            Err(panic) => {
                let detail = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic");
                format!("Indexing task panicked: {}", detail)
            }
        };
        PROGRESS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .failure = Some(message.clone());
        let delay = backoff.next_delay().max(if permanent {
            Duration::from_secs(30)
        } else {
            Duration::ZERO
        });
        log::error!(
            "Indexer stopped at checkpoint {}: {}. Restarting in {:?} (attempt {})",
            slot,
            message,
            delay,
            backoff.attempts()
        );
        crate::metric! { cadence_macros::statsd_count!("indexer_restarts", 1); }
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
#[path = "supervisor_test.rs"]
mod tests;
