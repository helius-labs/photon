use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use cadence_macros::{statsd_count, statsd_gauge};
use log::error;
use once_cell::sync::Lazy;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::time::{interval, sleep};

use crate::{
    api::method::{get_indexer_health::HEALTH_CHECK_SLOT_DISTANCE, utils::Context},
    common::{
        fetch_current_slot_with_infinite_retry, typedefs::rpc_client_with_uri::RpcClientWithUri,
    },
    dao::generated::state_trees,
    metric,
};

pub static LATEST_SLOT: Lazy<Arc<AtomicU64>> = Lazy::new(|| Arc::new(AtomicU64::new(0)));

async fn fetch_last_indexed_slot_with_infinite_retry(db: &DatabaseConnection) -> u64 {
    loop {
        if let Ok(context) = Context::extract(db).await {
            return context.slot;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

// Return a tokio join handle for the monitoring task
pub async fn continously_monitor_photon(
    db: &DatabaseConnection,
    rpc_client: Arc<RpcClientWithUri>,
) -> JoinHandle<()> {
    let mut has_been_healthy = false;
    start_latest_slot_updater(rpc_client).await;
    loop {
        let latest_slot = LATEST_SLOT.load(Ordering::SeqCst);
        let last_indexed_slot = fetch_last_indexed_slot_with_infinite_retry(db).await;
        let lag = if last_indexed_slot > latest_slot {
            last_indexed_slot - latest_slot
        } else {
            0
        };
        metric! {
            statsd_gauge!("indexing_lag", lag);
        }
        if lag < HEALTH_CHECK_SLOT_DISTANCE as u64 {
            has_been_healthy = true;
        }
        if has_been_healthy && lag > HEALTH_CHECK_SLOT_DISTANCE as u64 {
            error!("Indexing lag is too high: {}", lag);
            continue;
        }

        let state_tree_roots = state_trees::Entity::find()
            .filter(state_trees::Column::NodeIdx.eq(1))
            .all(db)
            .await
            .unwrap();

        let state_tree_root = state_tree_roots[0].clone();

        sleep(Duration::from_millis(1000)).await;
    }
}

pub async fn update_latest_slot(rpc_client: &RpcClientWithUri) {
    let slot = fetch_current_slot_with_infinite_retry(&rpc_client.client).await;
    LATEST_SLOT.fetch_max(slot, Ordering::SeqCst);
}

pub async fn start_latest_slot_updater(rpc_client: Arc<RpcClientWithUri>) {
    if LATEST_SLOT.load(Ordering::SeqCst) != 0 {
        return;
    }
    update_latest_slot(&rpc_client).await;
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            update_latest_slot(&rpc_client).await;
        }
    });
}
