use super::{error, parser::state_update::AccountTransaction};
use crate::{
    api::method::get_multiple_new_address_proofs::ADDRESS_TREE_HEIGHT,
    common::typedefs::{
        account::Account,
        hash::Hash,
        token_data::TokenData,
    },
    dao::generated::{account_transactions, state_tree_histories, state_trees, transactions},
    ingester::parser::state_update::Transaction,
};
use crate::{
    dao::generated::{accounts, token_accounts},
    ingester::parser::state_update::StateUpdate,
};
use itertools::Itertools;
use light_poseidon::{Poseidon, PoseidonBytesHasher};

use ark_bn254::Fr;
use borsh::BorshDeserialize;
use log::debug;
use persisted_indexed_merkle_tree::multi_append_fully_specified;
use persisted_state_tree::{persist_leaf_nodes, LeafNode};
use sea_orm::{
    sea_query::{Expr, OnConflict},
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait, Order,
    QueryFilter, QueryOrder, QuerySelect, QueryTrait, Set, Statement,
};
use std::collections::HashMap;

use error::IngesterError;
use solana_program::pubkey;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use sqlx::types::Decimal;
pub mod persisted_indexed_merkle_tree;
pub mod persisted_state_tree;

const COMPRESSED_TOKEN_PROGRAM: Pubkey = pubkey!("cTokenmWW8bLPjZEBAUgYy3zKxQZW6VKi7bqNFEVv3m");
const TREE_HEIGHT: u32 = 27;
// To avoid exceeding the 64k total parameter limit
pub const MAX_SQL_INSERTS: usize = 5000;

pub async fn persist_state_update(
    txn: &DatabaseTransaction,
    state_update: StateUpdate,
) -> Result<(), IngesterError> {
    if state_update == StateUpdate::default() {
        return Ok(());
    }
    let StateUpdate {
        in_accounts,
        out_accounts,
        account_transactions,
        transactions,
        leaf_nullifications,
        indexed_merkle_tree_updates,
    } = state_update;

    debug!(
        "Persisting state update with {} input accounts, {} output accounts",
        in_accounts.len(),
        out_accounts.len()
    );
    debug!("Persisting output accounts...");
    for chunk in out_accounts.chunks(MAX_SQL_INSERTS) {
        append_output_accounts(txn, chunk).await?;
    }

    debug!("Persisting spent accounts...");
    for chunk in in_accounts
        .into_iter()
        .collect::<Vec<_>>()
        .chunks(MAX_SQL_INSERTS)
    {
        spend_input_accounts(txn, chunk).await?;
    }

    let account_to_transaction = account_transactions
        .iter()
        .map(|account_transaction| {
            (
                account_transaction.hash.clone(),
                account_transaction.signature.clone(),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut leaf_nodes_with_signatures: Vec<(LeafNode, Signature)> = out_accounts
        .iter()
        .map(|account| {
            (
                LeafNode::from(account.clone()),
                account_to_transaction
                    .get(&account.hash)
                    .map(|signature| signature.clone())
                    // HACK: We should always have a signature for account transactions, but sometimes
                    //       we don't generate it for mock tests.
                    .unwrap_or(Signature::from([0; 64])),
            )
        })
        .chain(leaf_nullifications.iter().map(|leaf_nullification| {
            (
                LeafNode::from(leaf_nullification.clone()),
                leaf_nullification.signature.clone(),
            )
        }))
        .collect();

    leaf_nodes_with_signatures.sort_by_key(|x| x.0.seq);

    debug!("Persisting state nodes...");
    for chunk in leaf_nodes_with_signatures.chunks(MAX_SQL_INSERTS) {
        let chunk_vec = chunk.iter().map(|x| x.clone()).collect_vec();
        persist_state_tree_history(txn, chunk_vec.clone()).await?;
        let leaf_nodes_chunk = chunk_vec
            .iter()
            .map(|(leaf_node, _)| LeafNode::from(leaf_node.clone()))
            .collect_vec();

        persist_leaf_nodes(txn, leaf_nodes_chunk, TREE_HEIGHT).await?;
    }

    let transactions_vec = transactions.into_iter().collect::<Vec<_>>();

    debug!("Persisting transaction metadatas...");
    for chunk in transactions_vec.chunks(MAX_SQL_INSERTS) {
        persist_transactions(txn, chunk).await?;
    }

    debug!("Persisting account transactions...");
    let account_transactions = account_transactions.into_iter().collect::<Vec<_>>();
    for chunk in account_transactions.chunks(MAX_SQL_INSERTS) {
        persist_account_transactions(txn, chunk).await?;
    }

    debug!("Persisting index tree updates...");
    multi_append_fully_specified(txn, indexed_merkle_tree_updates, ADDRESS_TREE_HEIGHT).await?;

    Ok(())
}

async fn persist_state_tree_history(
    txn: &DatabaseTransaction,
    chunk: Vec<(LeafNode, Signature)>,
) -> Result<(), IngesterError> {
    let state_tree_history = chunk
        .into_iter()
        .map(|(leaf_node, signature)| state_tree_histories::ActiveModel {
            tree: Set(leaf_node.tree.to_bytes_vec()),
            seq: Set(leaf_node.seq as i64),
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

pub fn parse_token_data(account: &Account) -> Result<Option<TokenData>, IngesterError> {
    match account.data.clone() {
        Some(data) if account.owner.0 == COMPRESSED_TOKEN_PROGRAM => {
            let data_slice = data.data.0.as_slice();
            let token_data = TokenData::try_from_slice(data_slice).map_err(|e| IngesterError::ParserError(format!("Failed to parse token data: {:?}", e)))?;
            Ok(Some(token_data))
        }
        _ => Ok(None),
    }
}

async fn spend_input_accounts(
    txn: &DatabaseTransaction,
    in_accounts: &[Hash],
) -> Result<(), IngesterError> {
    // Perform the update operation on the identified records
    let query = accounts::Entity::update_many()
        .col_expr(accounts::Column::Spent, Expr::value(true))
        .col_expr(
            accounts::Column::PrevSpent,
            Expr::col(accounts::Column::Spent).into(),
        )
        .filter(
            accounts::Column::Hash.is_in(
                in_accounts
                    .iter()
                    .map(|account| account.to_vec())
                    .collect::<Vec<Vec<u8>>>(),
            ),
        )
        .build(txn.get_database_backend());

    execute_account_update_query_and_update_balances(
        txn,
        query,
        AccountType::Account,
        ModificationType::Spend,
    )
    .await?;

    debug!("Marking token accounts as spent...",);
    let query = token_accounts::Entity::update_many()
        .col_expr(token_accounts::Column::Spent, Expr::value(true))
        .col_expr(
            token_accounts::Column::PrevSpent,
            Expr::col(token_accounts::Column::Spent).into(),
        )
        .filter(
            token_accounts::Column::Hash.is_in(
                in_accounts
                    .iter()
                    .map(|account| account.to_vec())
                    .collect::<Vec<Vec<u8>>>(),
            ),
        )
        .build(txn.get_database_backend());

    execute_account_update_query_and_update_balances(
        txn,
        query,
        AccountType::TokenAccount,
        ModificationType::Spend,
    )
    .await?;

    Ok(())
}

pub struct EnrichedTokenAccount {
    pub token_data: TokenData,
    pub hash: Hash,
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
            account_type,
            e.to_string(),
            query.sql
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

    if values.len() > 0 {
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

async fn append_output_accounts(
    txn: &DatabaseTransaction,
    out_accounts: &[Account],
) -> Result<(), IngesterError> {
    let mut account_models = Vec::new();
    let mut token_accounts = Vec::new();

    for account in out_accounts {
        account_models.push(accounts::ActiveModel {
            hash: Set(account.hash.to_vec()),
            address: Set(account.address.map(|x| x.to_bytes_vec())),
            discriminator: Set(account
                .data
                .as_ref()
                .map(|x| Decimal::from(x.discriminator.0))),
            data: Set(account.data.as_ref().map(|x| x.data.clone().0)),
            data_hash: Set(account.data.as_ref().map(|x| x.data_hash.to_vec())),
            tree: Set(account.tree.to_bytes_vec()),
            leaf_index: Set(account.leaf_index.0 as i64),
            owner: Set(account.owner.to_bytes_vec()),
            lamports: Set(Decimal::from(account.lamports.0)),
            spent: Set(false),
            slot_created: Set(account.slot_created.0 as i64),
            seq: Set(account.seq.0 as i64),
            prev_spent: Set(None),
        });

        if let Some(token_data) = parse_token_data(account)? {
            token_accounts.push(EnrichedTokenAccount {
                token_data,
                hash: account.hash.clone(),
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

pub async fn persist_token_accounts(
    txn: &DatabaseTransaction,
    token_accounts: Vec<EnrichedTokenAccount>,
) -> Result<(), IngesterError> {
    let token_models = token_accounts
        .into_iter()
        .map(
            |EnrichedTokenAccount { token_data, hash }| token_accounts::ActiveModel {
                hash: Set(hash.into()),
                mint: Set(token_data.mint.to_bytes_vec()),
                owner: Set(token_data.owner.to_bytes_vec()),
                amount: Set(Decimal::from(token_data.amount.0)),
                delegate: Set(token_data.delegate.map(|d| d.to_bytes_vec())),
                state: Set(token_data.state as i32),
                spent: Set(false),
                prev_spent: Set(None),
                tlv: Set(token_data.tlv.map(|t| t.0)),
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

fn get_node_direct_ancestors(leaf_index: i64) -> Vec<i64> {
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
        .offset(1000)
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
