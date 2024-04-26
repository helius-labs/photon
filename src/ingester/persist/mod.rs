use super::{
    error,
    parser::state_update::{AccountTransaction, EnrichedPathNode},
};
use crate::{
    common::typedefs::{account::Account, hash::Hash, token_data::TokenData},
    dao::generated::{account_transactions, transactions},
    ingester::parser::state_update::Transaction,
};
use crate::{
    dao::generated::{accounts, state_trees, token_accounts},
    ingester::parser::state_update::StateUpdate,
};
use borsh::BorshDeserialize;
use log::debug;
use sea_orm::{
    sea_query::OnConflict, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait,
    QueryTrait, Set, Statement,
};
use std::collections::{HashMap, HashSet};

use error::IngesterError;
use solana_program::pubkey;
use solana_sdk::pubkey::Pubkey;
use sqlx::types::Decimal;

const COMPRESSED_TOKEN_PROGRAM: Pubkey = pubkey!("9sixVEthz2kMSKfeApZXHwuboT6DZuT6crAYJTciUCqE");
// To avoid exceeding the 25k total parameter limit, we set the insert limit to 1k (as we have fewer
// than 10 columns per table).
pub const MAX_SQL_INSERTS: usize = 1000;

pub async fn persist_state_update(
    txn: &DatabaseTransaction,
    mut state_update: StateUpdate,
) -> Result<(), IngesterError> {
    state_update.prune_redundant_updates();
    let StateUpdate {
        in_accounts,
        out_accounts,
        path_nodes,
        account_transactions,
    } = state_update;
    if in_accounts.is_empty() && out_accounts.is_empty() && path_nodes.is_empty() {
        return Ok(());
    }
    debug!(
        "Persisting state update with {} input accounts, {} output accounts, and {} path nodes",
        in_accounts.len(),
        out_accounts.len(),
        path_nodes.len()
    );

    debug!("Persisting spent accounts...");
    for chunk in in_accounts.chunks(MAX_SQL_INSERTS) {
        spend_input_accounts(txn, chunk).await?;
    }
    debug!("Persisting output accounts...");
    for chunk in out_accounts.chunks(MAX_SQL_INSERTS) {
        append_output_accounts(txn, chunk).await?;
    }

    debug!("Persisting path nodes...");
    for chunk in path_nodes
        .into_values()
        .collect::<Vec<_>>()
        .chunks(MAX_SQL_INSERTS)
    {
        persist_path_nodes(txn, chunk).await?;
    }

    let transactions: HashSet<Transaction> = account_transactions
        .iter()
        .map(|t| Transaction {
            signature: t.signature,
            slot: t.slot,
        })
        .collect();

    let mut transactions: Vec<Transaction> = transactions.into_iter().collect();
    transactions.sort_by_key(|t| t.signature);

    debug!("Persisting transactions nodes...");
    for chunk in transactions.chunks(MAX_SQL_INSERTS) {
        persist_transactions(txn, chunk).await?;
    }

    debug!("Persisting account transactions...");
    let account_transactions = account_transactions.into_iter().collect::<Vec<_>>();
    for chunk in account_transactions.chunks(MAX_SQL_INSERTS) {
        persist_account_transactions(txn, chunk).await?;
    }

    Ok(())
}

pub fn parse_token_data(account: &Account) -> Result<Option<TokenData>, IngesterError> {
    match account.data.clone() {
        Some(data) if account.owner.0 == COMPRESSED_TOKEN_PROGRAM => {
            let token_data = TokenData::try_from_slice(&data.data.0).map_err(|_| {
                IngesterError::ParserError("Failed to parse token data".to_string())
            })?;
            Ok(Some(token_data))
        }
        _ => Ok(None),
    }
}

async fn spend_input_accounts(
    txn: &DatabaseTransaction,
    in_accounts: &[Account],
) -> Result<(), IngesterError> {
    let in_account_models: Vec<accounts::ActiveModel> = in_accounts
        .iter()
        .map(|account| accounts::ActiveModel {
            hash: Set(account.hash.to_vec()),
            spent: Set(true),
            data: Set(None),
            owner: Set(account.owner.0.to_bytes().to_vec()),
            discriminator: Set(None),
            lamports: Set(Decimal::from(account.lamports.0)),
            slot_updated: Set(account.slot_updated.0 as i64),
            tree: Set(account.tree.0.to_bytes().to_vec()),
            leaf_index: Set(account.leaf_index.0 as i64),
            ..Default::default()
        })
        .collect();

    let query = accounts::Entity::insert_many(in_account_models)
        .on_conflict(
            OnConflict::column(accounts::Column::Hash)
                .update_columns([
                    accounts::Column::Hash,
                    accounts::Column::Data,
                    accounts::Column::Lamports,
                    accounts::Column::Spent,
                    accounts::Column::SlotUpdated,
                    accounts::Column::Tree,
                ])
                .to_owned(),
        )
        .build(txn.get_database_backend());

    execute_account_update_query_and_update_balances(
        txn,
        query,
        AccountType::Account,
        ModificationType::Spend,
    )
    .await?;

    let mut token_models = Vec::new();
    for in_accounts in in_accounts {
        let token_data = parse_token_data(in_accounts)?;
        if let Some(token_data) = token_data {
            token_models.push(token_accounts::ActiveModel {
                hash: Set(in_accounts.hash.to_vec()),
                spent: Set(true),
                amount: Set(Decimal::from(token_data.amount.0)),
                owner: Set(token_data.owner.to_bytes_vec()),
                mint: Set(token_data.mint.to_bytes_vec()),
                state: Set(token_data.state as i32),
                delegated_amount: Set(Decimal::from(0)),
                ..Default::default()
            });
        }
    }
    if !token_models.is_empty() {
        debug!("Marking {} token accounts as spent...", token_models.len());
        let query = token_accounts::Entity::insert_many(token_models)
            .on_conflict(
                OnConflict::column(token_accounts::Column::Hash)
                    .update_columns([
                        token_accounts::Column::Hash,
                        token_accounts::Column::Amount,
                        token_accounts::Column::Spent,
                    ])
                    .to_owned(),
            )
            .build(txn.get_database_backend());
        execute_account_update_query_and_update_balances(
            txn,
            query,
            AccountType::TokenAccount,
            ModificationType::Spend,
        )
        .await?;
    }

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

fn bytes_to_sql_format(database_backend: DatabaseBackend, bytes: Vec<u8>) -> String {
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
    let (original_table_name, owner_table_name, balance_column, additional_columns) =
        match account_type {
            AccountType::Account => ("accounts", "owner_balances", "lamports", ""),
            AccountType::TokenAccount => {
                ("token_accounts", "token_owner_balances", "amount", ", mint")
            }
        };
    let prev_spent_set = match modification_type {
        ModificationType::Append => "".to_string(),
        ModificationType::Spend => {
            format!(", \"prev_spent\" = \"{original_table_name}\".\"spent\"")
        }
    };
    query.sql = format!(
        "{} {} RETURNING owner,prev_spent,{}{}",
        query.sql, prev_spent_set, balance_column, additional_columns
    );
    let result = txn.query_all(query).await.map_err(|e| {
        IngesterError::DatabaseError(format!(
            "Got error appending {:?} accounts {}",
            account_type,
            e.to_string()
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
            slot_updated: Set(account.slot_updated.0 as i64),
            seq: Set(account.seq.map(|s| s.0 as i64)),
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
                delegated_amount: Set(Decimal::from(token_data.delegated_amount.0)),
                is_native: Set(token_data.is_native.map(|x| Decimal::from(x.0))),
                spent: Set(false),
                prev_spent: Set(None),
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

async fn persist_path_nodes(
    txn: &DatabaseTransaction,
    nodes: &[EnrichedPathNode],
) -> Result<(), IngesterError> {
    if nodes.is_empty() {
        return Ok(());
    }
    let node_models = nodes
        .iter()
        .map(|node| state_trees::ActiveModel {
            tree: Set(node.tree.to_vec()),
            level: Set(node.level as i64),
            node_idx: Set(node.node.index as i64),
            hash: Set(node.node.node.to_vec()),
            leaf_idx: Set(node.leaf_index.map(|x| x as i64)),
            seq: Set(node.seq as i64),
            slot_updated: Set(node.slot as i64),
        })
        .collect::<Vec<_>>();

    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let mut query = state_trees::Entity::insert_many(node_models)
        .on_conflict(
            OnConflict::columns([state_trees::Column::Tree, state_trees::Column::NodeIdx])
                .update_columns([
                    state_trees::Column::Hash,
                    state_trees::Column::Seq,
                    state_trees::Column::SlotUpdated,
                ])
                .to_owned(),
        )
        .build(txn.get_database_backend());
    query.sql = format!("{} WHERE excluded.seq > state_trees.seq", query.sql);
    txn.execute(query).await.map_err(|e| {
        IngesterError::DatabaseError(format!("Failed to persist path nodes: {}", e))
    })?;

    Ok(())
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
        txn.execute(query).await?;
    }

    Ok(())
}
