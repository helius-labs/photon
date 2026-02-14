use super::{error, parser::state_update::AccountTransaction};
use crate::ingester::parser::state_update::{AddressQueueUpdate, StateUpdate};
use crate::{
    api::method::utils::PAGE_LIMIT,
    common::{
        token_layout::{
            ACCOUNT_TYPE_MINT, COMPRESSED_MINT_PDA_END, COMPRESSED_MINT_PDA_OFFSET,
            TOKEN_ACCOUNT_TYPE_OFFSET,
        },
        typedefs::{hash::Hash, token_data::TokenData},
    },
    dao::generated::{
        account_transactions, accounts, state_tree_histories, state_trees, token_accounts,
        transactions,
    },
    ingester::parser::state_update::Transaction,
    metric,
};
use itertools::Itertools;
use light_poseidon::{Poseidon, PoseidonBytesHasher};
use persisted_batch_event::persist_batch_events;

use crate::common::typedefs::account::AccountWithContext;
use crate::dao::generated::address_queues;
use crate::ingester::persist::spend::{spend_input_accounts, spend_input_accounts_batched};
use ark_bn254::Fr;
use cadence_macros::statsd_count;
use error::IngesterError;
use light_compressed_account::TreeType;
use log::debug;
use persisted_indexed_merkle_tree::persist_indexed_tree_updates;
use sea_orm::{
    sea_query::OnConflict, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction,
    EntityTrait, Order, QueryFilter, QueryOrder, QuerySelect, QueryTrait, Set, Statement,
};
use solana_pubkey::{pubkey, Pubkey};
use solana_signature::Signature;
use sqlx::types::Decimal;
use std::{cmp::max, collections::HashMap};

pub mod indexed_merkle_tree;
pub mod persisted_indexed_merkle_tree;
pub mod persisted_state_tree;

pub use merkle_proof_with_context::MerkleProofWithContext;

mod leaf_node;
mod leaf_node_proof;
mod merkle_proof_with_context;
mod persisted_batch_event;
mod spend;

pub use self::leaf_node::{persist_leaf_nodes, LeafNode};
pub use self::leaf_node_proof::{
    get_multiple_compressed_leaf_proofs, get_multiple_compressed_leaf_proofs_by_indices,
    get_multiple_compressed_leaf_proofs_from_full_leaf_info,
};

pub const LIGHT_TOKEN_PROGRAM_ID: Pubkey = pubkey!("cTokenmWW8bLPjZEBAUgYy3zKxQZW6VKi7bqNFEVv3m");

/// Discriminator for decompressed PDA accounts: [255, 255, 255, 255, 255, 255, 255, 0]
/// This matches DECOMPRESSED_PDA_DISCRIMINATOR from light_compressible.
pub const DECOMPRESSED_ACCOUNT_DISCRIMINATOR: u64 = 0x00FFFFFFFFFFFFFF;

// To avoid exceeding the 64k total parameter limit
pub const MAX_SQL_INSERTS: usize = 500;

pub async fn persist_state_update(
    txn: &DatabaseTransaction,
    state_update: StateUpdate,
) -> Result<(), IngesterError> {
    if state_update == StateUpdate::default() {
        return Ok(());
    }

    let filtered = state_update.filter_by_known_trees(txn).await.map_err(|e| {
        IngesterError::ParserError(format!("Failed to filter by known trees: {}", e))
    })?;

    let state_update = filtered.state_update;
    let tree_info_cache = filtered.tree_info_cache;

    let StateUpdate {
        // input accounts that will be nullified
        in_accounts,
        // Output accounts that will be created
        out_accounts,
        account_transactions,
        transactions,
        leaf_nullifications,
        indexed_merkle_tree_updates,
        batch_merkle_tree_events,
        batch_nullify_context,
        batch_new_addresses,
        ata_owners,
    } = state_update;

    let input_accounts_len = in_accounts.len();
    let output_accounts_len = out_accounts.len();
    let leaf_nullifications_len = leaf_nullifications.len();
    let indexed_merkle_tree_updates_len = indexed_merkle_tree_updates.len();

    debug!(
        "Persisting state update with {} input accounts, {} output accounts, {} addresses",
        in_accounts.len(),
        out_accounts.len(),
        batch_new_addresses.len()
    );

    debug!("Persisting addresses...");
    for chunk in batch_new_addresses.chunks(MAX_SQL_INSERTS) {
        insert_addresses_into_queues(txn, chunk).await?;
    }

    debug!("Persisting output accounts...");
    for chunk in out_accounts.chunks(MAX_SQL_INSERTS) {
        append_output_accounts(txn, chunk, &ata_owners).await?;
    }

    debug!("Persisting spent accounts...");
    for chunk in in_accounts
        .into_iter()
        .collect::<Vec<_>>()
        .chunks(MAX_SQL_INSERTS)
    {
        spend_input_accounts(txn, chunk).await?;
    }

    spend_input_accounts_batched(txn, &batch_nullify_context).await?;

    let account_to_transaction = account_transactions
        .iter()
        .map(|account_transaction| {
            (
                account_transaction.hash.clone(),
                account_transaction.signature,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut leaf_nodes_with_signatures: Vec<(LeafNode, Signature)> = out_accounts
        .iter()
        .filter(|account| account.context.tree_type == TreeType::StateV1 as u16)
        .map(|account| {
            (
                LeafNode::from(account.clone()),
                account_to_transaction
                    .get(&account.account.hash)
                    .copied()
                    // HACK: We should always have a signature for account transactions, but sometimes
                    //       we don't generate it for mock tests.
                    .unwrap_or(Signature::from([0; 64])),
            )
        })
        .chain(leaf_nullifications.iter().map(|leaf_nullification| {
            (
                LeafNode::from(leaf_nullification.clone()),
                leaf_nullification.signature,
            )
        }))
        .collect();

    leaf_nodes_with_signatures.sort_by_key(|x| x.0.seq);

    debug!("Persisting index tree updates...");
    let converted_updates = indexed_merkle_tree_updates
        .into_iter()
        .map(|((pubkey, u64_val), update)| {
            let sdk_pubkey = Pubkey::new_from_array(pubkey.to_bytes());
            ((sdk_pubkey, u64_val), update)
        })
        .collect();

    // IMPORTANT: Persist indexed tree updates BEFORE state tree nodes to ensure consistency
    persist_indexed_tree_updates(txn, converted_updates, &tree_info_cache).await?;

    debug!("Persisting state nodes...");
    // Group leaf nodes by tree to get correct heights
    let mut nodes_by_tree: HashMap<solana_pubkey::Pubkey, Vec<(LeafNode, Signature)>> =
        HashMap::new();
    for (leaf_node, signature) in leaf_nodes_with_signatures {
        let tree_pubkey = solana_pubkey::Pubkey::try_from(leaf_node.tree.to_bytes_vec().as_slice())
            .map_err(|e| IngesterError::ParserError(format!("Invalid tree pubkey: {}", e)))?;
        nodes_by_tree
            .entry(tree_pubkey)
            .or_default()
            .push((leaf_node, signature));
    }

    // Process each tree's nodes with the correct height
    for (tree_pubkey, tree_nodes) in nodes_by_tree {
        let tree_info = tree_info_cache.get(&tree_pubkey)
            .ok_or_else(|| IngesterError::ParserError(format!(
                "Tree metadata not found for tree {}. Tree metadata must be synced before indexing.",
                tree_pubkey
            )))?;
        let tree_height = tree_info.height + 1; // +1 for indexed trees

        // Process in chunks
        for chunk in tree_nodes.chunks(MAX_SQL_INSERTS) {
            let chunk_vec = chunk.iter().cloned().collect_vec();
            persist_state_tree_history(txn, chunk_vec.clone()).await?;
            let leaf_nodes_chunk = chunk_vec
                .iter()
                .map(|(leaf_node, _)| leaf_node.clone())
                .collect_vec();

            persist_leaf_nodes(txn, leaf_nodes_chunk, tree_height).await?;
        }
    }

    let transactions_vec = transactions.into_iter().collect::<Vec<_>>();

    debug!("Persisting transaction metadatas...");
    let (compression_transactions, non_compression_transactions): (Vec<_>, Vec<_>) =
        transactions_vec
            .into_iter()
            .partition(|tx| tx.uses_compression);

    let non_compression_transactions_to_keep =
        max(0, PAGE_LIMIT as i64 - compression_transactions.len() as i64);
    let transactions_to_persist = compression_transactions
        .into_iter()
        .chain(
            non_compression_transactions
                .into_iter()
                .rev()
                .take(non_compression_transactions_to_keep as usize),
        )
        .collect_vec();
    for chunk in transactions_to_persist.chunks(MAX_SQL_INSERTS) {
        persist_transactions(txn, chunk).await?;
    }

    debug!("Persisting account transactions...");
    let account_transactions = account_transactions.into_iter().collect::<Vec<_>>();
    for chunk in account_transactions.chunks(MAX_SQL_INSERTS) {
        persist_account_transactions(txn, chunk).await?;
    }

    persist_batch_events(txn, batch_merkle_tree_events, &tree_info_cache).await?;

    metric! {
        statsd_count!("state_update.input_accounts", input_accounts_len as u64);
        statsd_count!("state_update.output_accounts", output_accounts_len as u64);
        statsd_count!("state_update.leaf_nullifications", leaf_nullifications_len as u64);
        statsd_count!("state_update.indexed_merkle_tree_updates", indexed_merkle_tree_updates_len as u64);
    }

    Ok(())
}

async fn persist_state_tree_history(
    txn: &DatabaseTransaction,
    chunk: Vec<(LeafNode, Signature)>,
) -> Result<(), IngesterError> {
    let state_tree_history = chunk
        .into_iter()
        .filter_map(|(leaf_node, signature)| {
            if leaf_node.seq.is_none() {
                None
            } else {
                Some((leaf_node, signature))
            }
        })
        .map(|(leaf_node, signature)| state_tree_histories::ActiveModel {
            tree: Set(leaf_node.tree.to_bytes_vec()),
            seq: Set(leaf_node.seq.unwrap() as i64),
            leaf_idx: Set(leaf_node.leaf_index as i64),
            transaction_signature: Set(Into::<[u8; 64]>::into(signature).to_vec()),
        })
        .collect_vec();
    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let query = state_tree_histories::Entity::insert_many(state_tree_history)
        .on_conflict(
            OnConflict::columns([state_trees::Column::Tree, state_trees::Column::Seq])
                .do_nothing()
                .to_owned(),
        )
        .build(txn.get_database_backend());
    txn.execute(query).await?;
    Ok(())
}

pub struct EnrichedTokenAccount {
    pub token_data: TokenData,
    pub hash: Hash,
    /// The wallet owner pubkey that the ATA is derived from (for compressed ATAs)
    pub ata_owner: Option<solana_pubkey::Pubkey>,
}

#[derive(Debug)]
enum AccountType {
    Account,
    TokenAccount,
}

#[derive(Debug)]
enum ModificationType {
    Append,
    Spend,
}

pub fn bytes_to_sql_format(database_backend: DatabaseBackend, bytes: Vec<u8>) -> String {
    match database_backend {
        DatabaseBackend::Postgres => bytes_to_postgres_sql_format(bytes),
        DatabaseBackend::Sqlite => bytes_to_sqlite_sql_format(bytes),
        _ => panic!("Unsupported database backend"),
    }
}

fn bytes_to_postgres_sql_format(bytes: Vec<u8>) -> String {
    let hex_string = bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>();
    format!("'\\x{}'", hex_string) // Properly formatted for PostgreSQL BYTEA
}

fn bytes_to_sqlite_sql_format(bytes: Vec<u8>) -> String {
    let hex_string = bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>();
    format!("X'{}'", hex_string) // Properly formatted for SQLite BLOB
}

async fn execute_account_update_query_and_update_balances(
    txn: &DatabaseTransaction,
    mut query: Statement,
    account_type: AccountType,
    modification_type: ModificationType,
) -> Result<(), IngesterError> {
    let (owner_table_name, balance_column, additional_columns) = match account_type {
        AccountType::Account => ("owner_balances", "lamports", ""),
        AccountType::TokenAccount => ("token_owner_balances", "amount", ", mint"),
    };

    query.sql = format!(
        "{} RETURNING owner,prev_spent,{}{}",
        query.sql, balance_column, additional_columns
    );
    let result = txn.query_all(query.clone()).await.map_err(|e| {
        IngesterError::DatabaseError(format!(
            "Got error appending {:?} accounts {}. Query {}",
            account_type, e, query.sql
        ))
    })?;
    let multiplier = Decimal::from(match &modification_type {
        ModificationType::Append => 1,
        ModificationType::Spend => -1,
    });
    let mut balance_modifications = HashMap::new();
    let db_backend = txn.get_database_backend();
    for row in result {
        let prev_spent: Option<bool> = row.try_get("", "prev_spent")?;
        match (prev_spent, &modification_type) {
            (_, ModificationType::Append) | (Some(false), ModificationType::Spend) => {
                let mut amount_of_interest = match db_backend {
                    DatabaseBackend::Postgres => row.try_get("", balance_column)?,
                    DatabaseBackend::Sqlite => {
                        let amount: i64 = row.try_get("", balance_column)?;
                        Decimal::from(amount)
                    }
                    _ => panic!("Unsupported database backend"),
                };
                amount_of_interest *= multiplier;
                let owner = bytes_to_sql_format(db_backend, row.try_get("", "owner")?);
                let key = match account_type {
                    AccountType::Account => owner,
                    AccountType::TokenAccount => {
                        format!(
                            "{},{}",
                            owner,
                            bytes_to_sql_format(db_backend, row.try_get("", "mint")?)
                        )
                    }
                };
                balance_modifications
                    .entry(key)
                    .and_modify(|amount| *amount += amount_of_interest)
                    .or_insert(amount_of_interest);
            }
            _ => {}
        }
    }
    let values = balance_modifications
        .into_iter()
        .filter(|(_, value)| *value != Decimal::from(0))
        .map(|(key, value)| format!("({}, {})", key, value))
        .collect::<Vec<String>>();

    if !values.is_empty() {
        let values_string = values.join(", ");
        let raw_sql = format!(
            "INSERT INTO {owner_table_name} (owner {additional_columns}, {balance_column})
            VALUES {values_string} ON CONFLICT (owner{additional_columns})
            DO UPDATE SET {balance_column} = {owner_table_name}.{balance_column} + excluded.{balance_column}",
        );
        txn.execute(Statement::from_string(db_backend, raw_sql))
            .await?;
    }

    Ok(())
}

async fn insert_addresses_into_queues(
    txn: &DatabaseTransaction,
    addresses: &[AddressQueueUpdate],
) -> Result<(), IngesterError> {
    let mut address_models = Vec::new();

    for address in addresses {
        address_models.push(address_queues::ActiveModel {
            address: Set(address.address.to_vec()),
            tree: Set(address.tree.to_bytes_vec()),
            queue_index: Set(address.queue_index as i64),
        });
    }

    let query = address_queues::Entity::insert_many(address_models)
        .on_conflict(
            OnConflict::column(address_queues::Column::Address)
                .do_nothing()
                .to_owned(),
        )
        .build(txn.get_database_backend());
    txn.execute(query).await?;

    Ok(())
}

async fn append_output_accounts(
    txn: &DatabaseTransaction,
    out_accounts: &[AccountWithContext],
    ata_owners: &HashMap<crate::common::typedefs::hash::Hash, solana_pubkey::Pubkey>,
) -> Result<(), IngesterError> {
    let mut account_models = Vec::new();
    let mut token_accounts = Vec::new();

    for account in out_accounts {
        let onchain_pubkey = extract_onchain_pubkey(account);

        account_models.push(accounts::ActiveModel {
            hash: Set(account.account.hash.to_vec()),
            address: Set(account.account.address.map(|x| x.to_bytes_vec())),
            onchain_pubkey: Set(onchain_pubkey),
            discriminator: Set(account
                .account
                .data
                .as_ref()
                .map(|x| x.discriminator.0.to_le_bytes().to_vec())),
            data: Set(account.account.data.as_ref().map(|x| x.data.clone().0)),
            data_hash: Set(account.account.data.as_ref().map(|x| x.data_hash.to_vec())),
            tree: Set(account.account.tree.to_bytes_vec()),
            queue: Set(Some(account.context.queue.to_bytes_vec())),
            leaf_index: Set(account.account.leaf_index.0 as i64),
            in_output_queue: Set(account.context.in_output_queue),
            nullifier_queue_index: Set(account.context.nullifier_queue_index.map(|x| x.0 as i64)),
            nullified_in_tree: Set(false),
            tree_type: Set(Some(account.context.tree_type as i32)),
            nullifier: Set(account.context.nullifier.as_ref().map(|x| x.to_vec())),
            owner: Set(account.account.owner.to_bytes_vec()),
            lamports: Set(Decimal::from(account.account.lamports.0)),
            spent: Set(false),
            slot_created: Set(account.account.slot_created.0 as i64),
            seq: Set(account.account.seq.map(|x| x.0 as i64)),
            prev_spent: Set(None),
            tx_hash: Default::default(), // tx hashes are only set when an account is an input
        });

        if let Some(token_data) = account.account.parse_token_data()? {
            let ata_owner = ata_owners.get(&account.account.hash).copied();
            token_accounts.push(EnrichedTokenAccount {
                token_data,
                hash: account.account.hash.clone(),
                ata_owner,
            });
        }
    }

    if !out_accounts.is_empty() {
        let query = accounts::Entity::insert_many(account_models)
            .on_conflict(
                OnConflict::column(accounts::Column::Hash)
                    .do_nothing()
                    .to_owned(),
            )
            .build(txn.get_database_backend());
        execute_account_update_query_and_update_balances(
            txn,
            query,
            AccountType::Account,
            ModificationType::Append,
        )
        .await?;

        if !token_accounts.is_empty() {
            debug!("Persisting {} token accounts...", token_accounts.len());
            persist_token_accounts(txn, token_accounts).await?;
        }
    }

    Ok(())
}

fn extract_onchain_pubkey(account: &AccountWithContext) -> Option<Vec<u8>> {
    let data = account.account.data.as_ref()?;

    // Decompressed PDA accounts store the on-chain pubkey in the first 32 bytes.
    if data.discriminator.0 == DECOMPRESSED_ACCOUNT_DISCRIMINATOR && data.data.0.len() >= 32 {
        return Some(data.data.0[..32].to_vec());
    }

    // Compressed mints store the mint PDA at [84..116], and have account_type marker
    // ACCOUNT_TYPE_MINT at byte TOKEN_ACCOUNT_TYPE_OFFSET.
    if account.account.owner.0 == LIGHT_TOKEN_PROGRAM_ID
        && !data.is_c_token_discriminator()
        && data.data.0.len() > TOKEN_ACCOUNT_TYPE_OFFSET
        && data.data.0[TOKEN_ACCOUNT_TYPE_OFFSET] == ACCOUNT_TYPE_MINT
    {
        return Some(data.data.0[COMPRESSED_MINT_PDA_OFFSET..COMPRESSED_MINT_PDA_END].to_vec());
    }

    None
}

pub async fn persist_token_accounts(
    txn: &DatabaseTransaction,
    token_accounts: Vec<EnrichedTokenAccount>,
) -> Result<(), IngesterError> {
    let token_models = token_accounts
        .into_iter()
        .map(
            |EnrichedTokenAccount {
                 token_data,
                 hash,
                 ata_owner,
             }| token_accounts::ActiveModel {
                hash: Set(hash.into()),
                mint: Set(token_data.mint.to_bytes_vec()),
                owner: Set(token_data.owner.to_bytes_vec()),
                amount: Set(Decimal::from(token_data.amount.0)),
                delegate: Set(token_data.delegate.map(|d| d.to_bytes_vec())),
                state: Set(token_data.state as i32),
                spent: Set(false),
                prev_spent: Set(None),
                tlv: Set(token_data.tlv.map(|t| t.0)),
                ata_owner: Set(ata_owner.map(|o| o.to_bytes().to_vec())),
            },
        )
        .collect::<Vec<_>>();

    let query = token_accounts::Entity::insert_many(token_models)
        .on_conflict(
            OnConflict::column(token_accounts::Column::Hash)
                .do_nothing()
                .to_owned(),
        )
        .build(txn.get_database_backend());

    execute_account_update_query_and_update_balances(
        txn,
        query,
        AccountType::TokenAccount,
        ModificationType::Append,
    )
    .await?;

    Ok(())
}

pub(crate) fn get_node_direct_ancestors(leaf_index: i64) -> Vec<i64> {
    let mut path: Vec<i64> = Vec::new();
    let mut current_index = leaf_index;
    while current_index > 1 {
        current_index >>= 1;
        path.push(current_index);
    }
    path
}

pub fn compute_parent_hash(left: Vec<u8>, right: Vec<u8>) -> Result<Vec<u8>, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();
    poseidon
        .hash_bytes_be(&[&left, &right])
        .map_err(|e| IngesterError::ParserError(format!("Failed to compute parent hash: {}", e)))
        .map(|x| x.to_vec())
}

async fn persist_transactions(
    txn: &DatabaseTransaction,
    transactions: &[Transaction],
) -> Result<(), IngesterError> {
    let transaction_models = transactions
        .iter()
        .map(|transaction| transactions::ActiveModel {
            signature: Set(Into::<[u8; 64]>::into(transaction.signature).to_vec()),
            slot: Set(transaction.slot as i64),
            uses_compression: Set(transaction.uses_compression),
            error: Set(transaction.error.clone()),
        })
        .collect::<Vec<_>>();

    if !transaction_models.is_empty() {
        // We first build the query and then execute it because SeaORM has a bug where it always throws
        // an error if we do not insert a record in an insert statement. However, in this case, it's
        // expected not to insert anything if the key already exists.
        let query = transactions::Entity::insert_many(transaction_models)
            .on_conflict(
                OnConflict::columns([transactions::Column::Signature])
                    .do_nothing()
                    .to_owned(),
            )
            .build(txn.get_database_backend());
        txn.execute(query).await?;
    }

    let result = transactions::Entity::find()
        .filter(transactions::Column::UsesCompression.eq(false))
        .order_by(transactions::Column::Slot, Order::Desc)
        .order_by(transactions::Column::Signature, Order::Desc)
        .offset(PAGE_LIMIT)
        .limit(1)
        .all(txn)
        .await?;

    if let Some(transaction) = result.first() {
        let query = transactions::Entity::delete_many()
            .filter(transactions::Column::Slot.lte(transaction.slot))
            .filter(transactions::Column::Signature.lt(transaction.signature.to_vec()))
            .filter(transactions::Column::UsesCompression.eq(false))
            .build(txn.get_database_backend());
        txn.execute(query).await?;
    }

    Ok(())
}

async fn persist_account_transactions(
    txn: &DatabaseTransaction,
    account_transactions: &[AccountTransaction],
) -> Result<(), IngesterError> {
    let account_transaction_models = account_transactions
        .iter()
        .map(|transaction| account_transactions::ActiveModel {
            hash: Set(transaction.hash.to_vec()),
            signature: Set(Into::<[u8; 64]>::into(transaction.signature).to_vec()),
        })
        .collect::<Vec<_>>();

    if !account_transaction_models.is_empty() {
        // We first build the query and then execute it because SeaORM has a bug where it always throws
        // an error if we do not insert a record in an insert statement. However, in this case, it's
        // expected not to insert anything if the key already exists.
        let query = account_transactions::Entity::insert_many(account_transaction_models)
            .on_conflict(
                OnConflict::columns([
                    account_transactions::Column::Hash,
                    account_transactions::Column::Signature,
                ])
                .do_nothing()
                .to_owned(),
            )
            .build(txn.get_database_backend());
        txn.execute(query).await.map_err(|e| {
            IngesterError::DatabaseError(format!(
                "Failed to persist account transactions: {:?}. Error {}",
                account_transactions, e
            ))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::typedefs::account::{
        Account, AccountContext, AccountData, C_TOKEN_DISCRIMINATOR_V2,
    };
    use crate::common::typedefs::bs64_string::Base64String;
    use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
    use crate::common::typedefs::unsigned_integer::UnsignedInteger;

    fn sample_account_with_context(
        owner: Pubkey,
        discriminator: u64,
        data: Vec<u8>,
    ) -> AccountWithContext {
        AccountWithContext {
            account: Account {
                hash: Hash::default(),
                address: None,
                data: Some(AccountData {
                    discriminator: UnsignedInteger(discriminator),
                    data: Base64String(data),
                    data_hash: Hash::default(),
                }),
                owner: SerializablePubkey::from(owner),
                lamports: UnsignedInteger(0),
                tree: SerializablePubkey::default(),
                leaf_index: UnsignedInteger(0),
                seq: None,
                slot_created: UnsignedInteger(0),
            },
            context: AccountContext {
                queue: SerializablePubkey::default(),
                in_output_queue: true,
                spent: false,
                nullified_in_tree: false,
                nullifier_queue_index: None,
                nullifier: None,
                tx_hash: None,
                tree_type: 0,
            },
        }
    }

    #[test]
    fn test_extract_onchain_pubkey_for_decompressed_pda() {
        let expected = (0u8..32).collect::<Vec<u8>>();
        let mut data = expected.clone();
        data.extend_from_slice(&[9u8; 8]);

        let account = sample_account_with_context(
            Pubkey::new_unique(),
            DECOMPRESSED_ACCOUNT_DISCRIMINATOR,
            data,
        );

        assert_eq!(extract_onchain_pubkey(&account), Some(expected));
    }

    #[test]
    fn test_extract_onchain_pubkey_for_compressed_mint() {
        let mut data = vec![0u8; TOKEN_ACCOUNT_TYPE_OFFSET + 1];
        let expected = [7u8; 32];
        data[COMPRESSED_MINT_PDA_OFFSET..COMPRESSED_MINT_PDA_END].copy_from_slice(&expected);
        data[TOKEN_ACCOUNT_TYPE_OFFSET] = ACCOUNT_TYPE_MINT;

        let account = sample_account_with_context(LIGHT_TOKEN_PROGRAM_ID, 0, data);

        assert_eq!(extract_onchain_pubkey(&account), Some(expected.to_vec()));
    }

    #[test]
    fn test_extract_onchain_pubkey_skips_ctoken_discriminator() {
        let mut data = vec![0u8; TOKEN_ACCOUNT_TYPE_OFFSET + 1];
        data[COMPRESSED_MINT_PDA_OFFSET..COMPRESSED_MINT_PDA_END].copy_from_slice(&[8u8; 32]);
        data[TOKEN_ACCOUNT_TYPE_OFFSET] = ACCOUNT_TYPE_MINT;

        let account = sample_account_with_context(
            LIGHT_TOKEN_PROGRAM_ID,
            u64::from_le_bytes(C_TOKEN_DISCRIMINATOR_V2),
            data,
        );

        assert_eq!(extract_onchain_pubkey(&account), None);
    }
}
