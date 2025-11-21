use super::{indexer_events::RawIndexedElement, merkle_tree_events_parser::BatchMerkleTreeEvents};
use crate::api::error::PhotonApiError;
use crate::common::typedefs::account::AccountWithContext;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::parser::tree_info::TreeInfo;
use borsh::{BorshDeserialize, BorshSerialize};
use jsonrpsee_core::Serialize;
use light_compressed_account::TreeType;
use light_event::event::{BatchNullifyContext, NewAddress};
use log::debug;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use std::collections::{HashMap, HashSet};
use utoipa::ToSchema;

#[derive(BorshDeserialize, BorshSerialize, Debug, Clone, PartialEq, Eq)]
pub struct PathNode {
    pub node: [u8; 32],
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichedPathNode {
    pub node: PathNode,
    pub slot: u64,
    pub tree: [u8; 32],
    pub seq: u64,
    pub level: usize,
    pub tree_depth: usize,
    pub leaf_index: Option<u32>,
}

pub struct PathUpdate {
    pub tree: [u8; 32],
    pub path: Vec<PathNode>,
    pub seq: u64,
}

#[derive(Hash, Eq, Clone, PartialEq, Debug)]
pub struct Transaction {
    pub signature: Signature,
    pub slot: u64,
    pub uses_compression: bool,
    pub error: Option<String>,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct AccountTransaction {
    pub hash: Hash,
    pub signature: Signature,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct LeafNullification {
    pub tree: Pubkey,
    pub leaf_index: u64,
    pub seq: u64,
    pub signature: Signature,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct IndexedTreeLeafUpdate {
    pub tree: Pubkey,
    pub tree_type: TreeType,
    pub leaf: RawIndexedElement,
    pub hash: [u8; 32],
    pub seq: u64,
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AddressQueueUpdate {
    pub tree: SerializablePubkey,
    pub address: [u8; 32],
    pub queue_index: u64,
}

impl From<NewAddress> for AddressQueueUpdate {
    fn from(new_address: NewAddress) -> Self {
        AddressQueueUpdate {
            tree: SerializablePubkey::from(new_address.mt_pubkey),
            address: new_address.address,
            queue_index: new_address.queue_index,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
/// Representation of state update of the compression system that is optimal for simple persistence.
pub struct StateUpdate {
    // v1 and v2 tree accounts
    pub in_accounts: HashSet<Hash>,
    // v1 and v2 tree accounts
    pub out_accounts: Vec<AccountWithContext>,
    pub account_transactions: HashSet<AccountTransaction>,
    pub transactions: HashSet<Transaction>,
    pub leaf_nullifications: HashSet<LeafNullification>,
    pub indexed_merkle_tree_updates: HashMap<(Pubkey, u64), IndexedTreeLeafUpdate>,
    // v2 state and address Merkle tree updates
    pub batch_merkle_tree_events: BatchMerkleTreeEvents,
    // v2 input accounts that are inserted into the input queue
    pub batch_nullify_context: Vec<BatchNullifyContext>,
    pub batch_new_addresses: Vec<AddressQueueUpdate>,
}

/// Result of filtering a StateUpdate by known trees
pub struct FilteredStateUpdate {
    pub state_update: StateUpdate,
    pub tree_info_cache: HashMap<Pubkey, TreeInfo>,
}

impl StateUpdate {
    pub fn new() -> Self {
        StateUpdate::default()
    }

    /// Filters the StateUpdate to only include data from trees that exist in the database
    /// and match the expected owner. This prevents processing events from external/unauthorized trees.
    ///
    /// A "known tree" is one that:
    /// 1. Exists in the `tree_metadata` table
    /// 2. Has an owner matching `EXPECTED_TREE_OWNER` (if configured)
    ///
    /// All data structures referencing unknown trees are filtered out, and account_transactions
    /// are filtered to match the kept accounts.
    ///
    /// Returns the filtered StateUpdate and a cache of tree metadata for the known trees.
    pub async fn filter_by_known_trees<C>(
        self,
        txn: &C,
    ) -> Result<FilteredStateUpdate, PhotonApiError>
    where
        C: sea_orm::ConnectionTrait + sea_orm::TransactionTrait,
    {
        // Collect all tree pubkeys referenced in this state update
        let mut all_tree_pubkeys: HashSet<Pubkey> = self
            .indexed_merkle_tree_updates
            .keys()
            .map(|(pubkey, _)| *pubkey)
            .collect();

        for account in self.out_accounts.iter() {
            if let Ok(tree_pubkey) =
                Pubkey::try_from(account.account.tree.to_bytes_vec().as_slice())
            {
                all_tree_pubkeys.insert(tree_pubkey);
            }
        }

        for leaf_nullification in self.leaf_nullifications.iter() {
            all_tree_pubkeys.insert(leaf_nullification.tree);
        }

        for tree_pubkey in self.batch_merkle_tree_events.keys() {
            all_tree_pubkeys.insert(Pubkey::from(*tree_pubkey));
        }

        for address in self.batch_new_addresses.iter() {
            if let Ok(tree_pubkey) = Pubkey::try_from(address.tree.to_bytes_vec().as_slice()) {
                all_tree_pubkeys.insert(tree_pubkey);
            }
        }

        // Query database for tree metadata - only trees with correct owner will be in cache
        let tree_info_cache = if !all_tree_pubkeys.is_empty() {
            let pubkeys_vec: Vec<Pubkey> = all_tree_pubkeys.into_iter().collect();
            TreeInfo::get_tree_info_batch(txn, &pubkeys_vec).await?
        } else {
            std::collections::HashMap::new()
        };

        let known_trees: HashSet<_> = tree_info_cache.keys().cloned().collect();

        let is_tree_known = |tree_pubkey: &Pubkey, context: &str| -> bool {
            if !known_trees.contains(tree_pubkey) {
                debug!("Skipping {} for unknown tree {}", context, tree_pubkey);
                false
            } else {
                true
            }
        };

        // Track which account hashes we're keeping for filtering account_transactions later
        let mut kept_account_hashes = HashSet::new();

        // Add input (spent) account hashes - these don't have tree info but should be kept
        // for account_transactions tracking
        kept_account_hashes.extend(self.in_accounts.iter().cloned());

        // Filter out_accounts
        let out_accounts: Vec<_> = self
            .out_accounts
            .into_iter()
            .filter(|account| {
                match Pubkey::try_from(account.account.tree.to_bytes_vec().as_slice()) {
                    Ok(tree_pubkey) => {
                        if !is_tree_known(&tree_pubkey, "output account") {
                            return false;
                        }
                        kept_account_hashes.insert(account.account.hash.clone());
                        true
                    }
                    Err(_) => {
                        debug!("Skipping output account with invalid tree pubkey");
                        false
                    }
                }
            })
            .collect();

        // Filter leaf_nullifications
        let leaf_nullifications: HashSet<_> = self
            .leaf_nullifications
            .into_iter()
            .filter(|nullification| is_tree_known(&nullification.tree, "leaf nullification"))
            .collect();

        // Filter indexed_merkle_tree_updates
        let indexed_merkle_tree_updates: HashMap<_, _> = self
            .indexed_merkle_tree_updates
            .into_iter()
            .filter(|((tree_pubkey, _), _)| {
                is_tree_known(tree_pubkey, "indexed merkle tree update")
            })
            .collect();

        // Filter batch_merkle_tree_events
        let batch_merkle_tree_events: crate::ingester::parser::merkle_tree_events_parser::BatchMerkleTreeEvents = self
            .batch_merkle_tree_events
            .into_iter()
            .filter(|(tree_bytes, _)| {
                let tree_pubkey = Pubkey::from(*tree_bytes);
                is_tree_known(&tree_pubkey, "batch merkle tree events")
            })
            .collect();

        // Filter batch_new_addresses
        let batch_new_addresses: Vec<_> = self
            .batch_new_addresses
            .into_iter()
            .filter(
                |address| match Pubkey::try_from(address.tree.to_bytes_vec().as_slice()) {
                    Ok(tree_pubkey) => is_tree_known(&tree_pubkey, "address"),
                    Err(_) => {
                        debug!("Skipping address with invalid tree pubkey");
                        false
                    }
                },
            )
            .collect();

        // Filter account_transactions to only include transactions for accounts we kept
        let account_transactions: HashSet<_> = self
            .account_transactions
            .into_iter()
            .filter(|account_tx| {
                if !kept_account_hashes.contains(&account_tx.hash) {
                    debug!(
                        "Skipping account transaction for filtered account {}",
                        account_tx.hash
                    );
                    false
                } else {
                    true
                }
            })
            .collect();

        //   Some fields are not explicitly filtered by tree:
        // - transactions: Contains no tree references (only signature, slot, error)
        // - in_accounts: Just account hashes with no tree information
        // - batch_nullify_context: References accounts by hash only. If the account doesn't
        //   exist in the DB (filtered during creation), the subsequent UPDATE will affect 0 rows.
        //   This achieves implicit filtering without an extra DB query.

        let state_update = StateUpdate {
            in_accounts: self.in_accounts,
            out_accounts,
            account_transactions,
            transactions: self.transactions,
            leaf_nullifications,
            indexed_merkle_tree_updates,
            batch_merkle_tree_events,
            batch_nullify_context: self.batch_nullify_context,
            batch_new_addresses,
        };

        Ok(FilteredStateUpdate {
            state_update,
            tree_info_cache,
        })
    }

    pub fn merge_updates(updates: Vec<StateUpdate>) -> StateUpdate {
        let mut merged = StateUpdate::default();

        for update in updates {
            merged.in_accounts.extend(update.in_accounts);
            merged.out_accounts.extend(update.out_accounts);
            merged
                .account_transactions
                .extend(update.account_transactions);
            merged.transactions.extend(update.transactions);
            merged
                .leaf_nullifications
                .extend(update.leaf_nullifications);

            for (key, value) in update.indexed_merkle_tree_updates {
                // Insert only if the seq is higher.
                if let Some(existing) = merged.indexed_merkle_tree_updates.get_mut(&key) {
                    if value.seq > existing.seq {
                        *existing = value;
                    }
                } else {
                    merged.indexed_merkle_tree_updates.insert(key, value);
                }
            }

            for (key, events) in update.batch_merkle_tree_events {
                if let Some(existing_events) = merged.batch_merkle_tree_events.get_mut(&key) {
                    existing_events.extend(events);
                } else {
                    merged.batch_merkle_tree_events.insert(key, events);
                }
            }

            merged
                .batch_new_addresses
                .extend(update.batch_new_addresses);
            merged
                .batch_nullify_context
                .extend(update.batch_nullify_context);
        }

        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::tree_metadata_sync::{upsert_tree_metadata, TreeAccountData};
    use light_compressed_account::TreeType;
    use sea_orm::DatabaseConnection;
    use sea_orm_migration::MigratorTrait;
    use solana_pubkey::Pubkey as SdkPubkey;

    async fn setup_test_db() -> DatabaseConnection {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();

        crate::migration::MigractorWithCustomMigrations::up(&db, None)
            .await
            .unwrap();

        db
    }

    async fn insert_test_tree(
        db: &DatabaseConnection,
        tree_pubkey: &str,
        queue_pubkey: &str,
        tree_type: TreeType,
    ) -> Pubkey {
        let tree_pk = tree_pubkey.parse::<Pubkey>().unwrap();
        let queue_pk = queue_pubkey.parse::<Pubkey>().unwrap();

        let owner = crate::ingester::parser::EXPECTED_TREE_OWNER
            .expect("EXPECTED_TREE_OWNER must be set for tests");

        let data = TreeAccountData {
            queue_pubkey: SdkPubkey::from(queue_pk.to_bytes()),
            root_history_capacity: 2400,
            height: 26,
            sequence_number: 0,
            next_index: 0,
            owner: SdkPubkey::from(owner.to_bytes()),
        };

        upsert_tree_metadata(db, SdkPubkey::from(tree_pk.to_bytes()), tree_type, &data, 0)
            .await
            .unwrap();

        tree_pk
    }

    #[tokio::test]
    async fn test_filter_by_known_trees_filters_unknown_trees() {
        let db = setup_test_db().await;

        // Insert one known tree
        let known_tree = insert_test_tree(
            &db,
            "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT",
            "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148",
            TreeType::StateV1,
        )
        .await;

        let unknown_tree = "smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho"
            .parse::<Pubkey>()
            .unwrap();

        // Create state update with data from both known and unknown trees
        let mut state_update = StateUpdate::new();

        // Add leaf nullifications for both trees
        state_update.leaf_nullifications.insert(LeafNullification {
            tree: known_tree,
            leaf_index: 0,
            signature: Signature::default(),
            seq: 0,
        });

        state_update.leaf_nullifications.insert(LeafNullification {
            tree: unknown_tree,
            leaf_index: 1,
            signature: Signature::default(),
            seq: 1,
        });

        // Filter the state update
        let result = state_update.filter_by_known_trees(&db).await.unwrap();

        // Assert: only the known tree's nullification remains
        assert_eq!(result.state_update.leaf_nullifications.len(), 1);
        assert_eq!(
            result
                .state_update
                .leaf_nullifications
                .iter()
                .next()
                .unwrap()
                .tree,
            known_tree
        );
    }

    #[tokio::test]
    async fn test_filter_by_known_trees_keeps_all_when_all_known() {
        let db = setup_test_db().await;

        // Insert two known trees
        let tree1 = insert_test_tree(
            &db,
            "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT",
            "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148",
            TreeType::StateV1,
        )
        .await;

        let tree2 = insert_test_tree(
            &db,
            "smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho",
            "nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X",
            TreeType::StateV1,
        )
        .await;

        // Create state update with data from both known trees
        let mut state_update = StateUpdate::new();

        state_update.leaf_nullifications.insert(LeafNullification {
            tree: tree1,
            leaf_index: 0,
            signature: Signature::default(),
            seq: 0,
        });

        state_update.leaf_nullifications.insert(LeafNullification {
            tree: tree2,
            leaf_index: 1,
            signature: Signature::default(),
            seq: 1,
        });

        // Filter the state update
        let result = state_update.filter_by_known_trees(&db).await.unwrap();

        // Assert: both nullifications remain and cache contains both trees
        assert_eq!(result.state_update.leaf_nullifications.len(), 2);
        assert_eq!(result.tree_info_cache.len(), 2);
        assert!(result.tree_info_cache.contains_key(&tree1));
        assert!(result.tree_info_cache.contains_key(&tree2));
    }

    #[tokio::test]
    async fn test_filter_by_known_trees_empty_state_update() {
        let db = setup_test_db().await;

        let state_update = StateUpdate::new();

        // Filter empty state update
        let result = state_update.filter_by_known_trees(&db).await.unwrap();

        // Assert: everything is empty
        assert_eq!(result.state_update, StateUpdate::new());
        assert_eq!(result.tree_info_cache.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_by_known_trees_filters_account_transactions() {
        let db = setup_test_db().await;

        // Insert one known tree
        let known_tree = insert_test_tree(
            &db,
            "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT",
            "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148",
            TreeType::StateV1,
        )
        .await;

        let unknown_tree = "smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho"
            .parse::<Pubkey>()
            .unwrap();

        let mut state_update = StateUpdate::new();

        // Create accounts with hashes
        let known_account = AccountWithContext {
            account: crate::common::typedefs::account::Account {
                hash: crate::common::typedefs::hash::Hash::new_unique(),
                tree: SerializablePubkey::from(known_tree.to_bytes()),
                ..Default::default()
            },
            context: Default::default(),
        };

        let unknown_account = AccountWithContext {
            account: crate::common::typedefs::account::Account {
                hash: crate::common::typedefs::hash::Hash::new_unique(),
                tree: SerializablePubkey::from(unknown_tree.to_bytes()),
                ..Default::default()
            },
            context: Default::default(),
        };

        let known_hash = known_account.account.hash.clone();
        let unknown_hash = unknown_account.account.hash.clone();

        state_update.out_accounts.push(known_account);
        state_update.out_accounts.push(unknown_account);

        // Add account transactions for both
        state_update
            .account_transactions
            .insert(AccountTransaction {
                hash: known_hash.clone(),
                signature: Signature::default(),
            });

        state_update
            .account_transactions
            .insert(AccountTransaction {
                hash: unknown_hash.clone(),
                signature: Signature::default(),
            });

        // Filter the state update
        let result = state_update.filter_by_known_trees(&db).await.unwrap();

        // Assert: only account from known tree and its transaction remain
        assert_eq!(result.state_update.out_accounts.len(), 1);
        assert_eq!(result.state_update.out_accounts[0].account.hash, known_hash);
        assert_eq!(result.state_update.account_transactions.len(), 1);
        assert!(result
            .state_update
            .account_transactions
            .iter()
            .any(|tx| tx.hash == known_hash));
        assert!(!result
            .state_update
            .account_transactions
            .iter()
            .any(|tx| tx.hash == unknown_hash));
    }
}
