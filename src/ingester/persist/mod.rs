use std::collections::HashSet;

use super::{
    error,
    parser::{
        state_update::{AccountTransaction, EnrichedPathNode},
    },
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
    sea_query::OnConflict, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryTrait, Set,
};

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
    for chunk in path_nodes.chunks(MAX_SQL_INSERTS) {
        persist_path_nodes(txn, chunk).await?;
    }

    debug!("Persisting path nodes...");
    for chunk in path_nodes.chunks(MAX_SQL_INSERTS) {
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
    for chunk in account_transactions.chunks(MAX_SQL_INSERTS) {
        persist_account_transactions(txn, chunk).await?;
    }

    Ok(())
}

fn parse_token_data(account: &Account) -> Result<Option<TokenData>, IngesterError> {
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
            lamports: Set(Decimal::from(0)),
            slot_updated: Set(account.slot_updated as i64),
            tree: Set(account.tree.0.to_bytes().to_vec()),
            leaf_index: Set(account.leaf_index as i64),
            ..Default::default()
        })
        .collect();

    if !in_account_models.is_empty() {
        accounts::Entity::insert_many(in_account_models)
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
            .exec(txn)
            .await?;
    }
    let mut token_models = Vec::new();
    for in_accounts in in_accounts {
        let token_data = parse_token_data(&in_accounts)?;
        if let Some(token_data) = token_data {
            token_models.push(token_accounts::ActiveModel {
                hash: Set(in_accounts.hash.to_vec()),
                spent: Set(true),
                amount: Set(Decimal::from(0)),
                slot_updated: Set(in_accounts.slot_updated as i64),
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
        token_accounts::Entity::insert_many(token_models)
            .on_conflict(
                OnConflict::column(token_accounts::Column::Hash)
                    .update_columns([
                        token_accounts::Column::Hash,
                        token_accounts::Column::Amount,
                        token_accounts::Column::Spent,
                    ])
                    .to_owned(),
            )
            .exec(txn)
            .await?;
    }

    Ok(())
}

pub struct EnrichedTokenAccount {
    pub token_data: TokenData,
    pub hash: Hash,
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
            address: Set(account.address.clone().map(|x| x.to_bytes_vec())),
            discriminator: Set(None),
            data: Set(account.clone().data.map(|x| x.data.0)),
            data_hash: Set(account.clone().data.map(|x| x.data_hash.to_vec())),
            tree: Set(account.tree.to_bytes_vec()),
            leaf_index: Set(account.leaf_index as i64),
            owner: Set(account.owner.to_bytes_vec()),
            lamports: Set(Decimal::from(account.lamports)),
            spent: Set(false),
            slot_updated: Set(account.slot_updated as i64),
            seq: Set(account.seq.map(|s| s as i64)),
            ..Default::default()
        });

        if let Some(token_data) = parse_token_data(account)? {
            token_accounts.push(EnrichedTokenAccount {
                token_data,
                hash: account.hash.clone(),
            });
        }
    }

    // The state tree is append-only so conflicts only occur if a record is already inserted or
    // marked as spent spent.
    //
    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    if !out_accounts.is_empty() {
        let query = accounts::Entity::insert_many(account_models)
            .on_conflict(
                OnConflict::column(accounts::Column::Hash)
                    .do_nothing()
                    .to_owned(),
            )
            .build(txn.get_database_backend());
        txn.execute(query).await?;
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
                amount: Set(Decimal::from(token_data.amount)),
                delegate: Set(token_data.delegate.map(|d| d.to_bytes_vec())),
                state: Set(token_data.state as i32),
                delegated_amount: Set(Decimal::from(token_data.delegated_amount)),
                is_native: Set(token_data.is_native.map(Decimal::from)),
                spent: Set(false),
                ..Default::default()
            },
        )
        .collect::<Vec<_>>();

    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let query = token_accounts::Entity::insert_many(token_models)
        .on_conflict(
            OnConflict::column(token_accounts::Column::Hash)
                .do_nothing()
                .to_owned(),
        )
        .build(txn.get_database_backend());
    txn.execute(query).await?;

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
    txn.execute(query).await?;

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
            closure: Set(transaction.closure),
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
                    account_transactions::Column::Closure,
                ])
                .do_nothing()
                .to_owned(),
            )
            .build(txn.get_database_backend());
        txn.execute(query).await?;
    }

    Ok(())
}
