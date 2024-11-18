use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use cadence_macros::{statsd_count, statsd_gauge};
use log::{error, info};
use once_cell::sync::Lazy;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::{
    task::JoinHandle,
    time::{interval, sleep},
};

use crate::{common::fetch_current_slot_with_infinite_retry, metric};

use solana_sdk::pubkey::Pubkey;
use std::mem;
const CHUNK_SIZE: usize = 100;

pub static LATEST_SLOT: Lazy<Arc<AtomicU64>> = Lazy::new(|| Arc::new(AtomicU64::new(0)));
pub const HEALTH_CHECK_SLOT_DISTANCE: u64 = 20;

// Return a tokio join handle for the monitoring task
pub async fn update_latest_slot(rpc_client: &RpcClient) {
    let slot = fetch_current_slot_with_infinite_retry(&rpc_client).await;
    LATEST_SLOT.fetch_max(slot, Ordering::SeqCst);
}

pub async fn start_latest_slot_updater(rpc_client: Arc<RpcClient>) {
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
