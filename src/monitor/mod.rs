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

use crate::{
    api::method::{get_indexer_health::HEALTH_CHECK_SLOT_DISTANCE, utils::Context},
    common::fetch_current_slot_with_infinite_retry,
    dao::generated::state_trees,
    metric,
};
use light_concurrent_merkle_tree::copy::ConcurrentMerkleTreeCopy;
use light_concurrent_merkle_tree::light_hasher::Poseidon;
use light_sdk::state::MerkleTreeMetadata;

use crate::common::typedefs::hash::Hash;

use solana_sdk::account::Account as SolanaAccount;

use solana_sdk::pubkey::Pubkey;
use std::mem;
const CHUNK_SIZE: usize = 100;

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
pub fn continously_monitor_photon(
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut has_been_healthy = false;
        start_latest_slot_updater(rpc_client.clone()).await;

        loop {
            let latest_slot = LATEST_SLOT.load(Ordering::SeqCst);
            let last_indexed_slot = fetch_last_indexed_slot_with_infinite_retry(db.as_ref()).await;
            let lag = if latest_slot > last_indexed_slot {
                latest_slot - last_indexed_slot
            } else {
                0
            };
            metric! {
                statsd_gauge!("latest_slot", latest_slot);
                statsd_gauge!("last_indexed_slot", last_indexed_slot);
                statsd_gauge!("indexing_lag", lag);
            }
            if lag < HEALTH_CHECK_SLOT_DISTANCE as u64 {
                has_been_healthy = true;
            }
            info!("Indexing lag: {}", lag);
            if lag > HEALTH_CHECK_SLOT_DISTANCE as u64 {
                if has_been_healthy {
                    error!("Indexing lag is too high: {}", lag);
                }
            } else {
                let tree_roots = load_db_tree_roots_with_infinite_retry(db.as_ref()).await;
                validate_tree_roots(rpc_client.as_ref(), tree_roots).await;
            }
            sleep(Duration::from_millis(5000)).await;
        }
    })
}

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

fn parse_historical_roots(account: SolanaAccount) -> Vec<Hash> {
    let roots = ConcurrentMerkleTreeCopy::<Poseidon, 26>::from_bytes_copy(
        &account.data[8 + mem::size_of::<MerkleTreeMetadata>()..],
    )
    .unwrap()
    .roots
    .iter()
    .map(|root| Hash::from(*root))
    .collect();

    roots
}

async fn load_db_tree_roots_with_infinite_retry(db: &DatabaseConnection) -> Vec<(Pubkey, Hash)> {
    loop {
        let models = state_trees::Entity::find()
            .filter(state_trees::Column::NodeIdx.eq(1))
            .all(db)
            .await;
        match models {
            Ok(models) => {
                return models
                    .iter()
                    .map(|model| {
                        (
                            Pubkey::try_from(model.tree.clone()).unwrap(),
                            Hash::try_from(model.hash.clone()).unwrap(),
                        )
                    })
                    .collect()
            }
            Err(e) => {
                log::error!("Error loading tree roots: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn load_accounts_with_infinite_retry(
    rpc_client: &RpcClient,
    pubkeys: Vec<Pubkey>,
) -> Vec<SolanaAccount> {
    loop {
        let accounts = rpc_client.get_multiple_accounts(&pubkeys).await;
        match accounts {
            Ok(accounts) => {
                let mut parsed_accounts = Vec::new();
                let mut found_null_account = false;
                for account in accounts {
                    match account {
                        Some(account) => parsed_accounts.push(account),
                        None => {
                            log::error!("Found null tree account when fetching historical roots. Retrying...");
                            found_null_account = true;
                            break;
                        }
                    }
                }
                if found_null_account {
                    continue;
                }
                return parsed_accounts;
            }
            Err(e) => {
                log::error!("Error loading accounts: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn validate_tree_roots(rpc_client: &RpcClient, db_roots: Vec<(Pubkey, Hash)>) {
    let mut root_validation_errors = 0;
    for chunk in db_roots.chunks(CHUNK_SIZE) {
        let pubkeys = chunk.iter().map(|(pubkey, _)| pubkey.clone()).collect();
        let accounts = load_accounts_with_infinite_retry(rpc_client, pubkeys).await;
        for ((pubkey, db_hash), account) in chunk.iter().zip(accounts) {
            let account_roots = parse_historical_roots(account);
            if !account_roots.contains(db_hash) {
                log::error!(
                    "Root mismatch for pubkey {:?}. db_hash: {}, account_roots: {:?}",
                    pubkey,
                    db_hash,
                    account_roots
                );
                root_validation_errors += 1;
                statsd_count!("root_validation_failure", 1, "pubkey" => pubkey);
            }
        }
    }

    if root_validation_errors > 0 {
        metric! {
            statsd_count!("root_validation_failures", root_validation_errors);
            statsd_gauge!("root_validation_success", 0);
        }
    } else {
        metric! {
            statsd_gauge!("root_validation_success", 1);
            statsd_count!("root_validation_failures", 0);
        }
    }
}
