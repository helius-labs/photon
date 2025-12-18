use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::{accounts, state_trees};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use std::collections::HashMap;

/// Maximum number of parameters allowed in a single PostgreSQL query.
const MAX_SQL_PARAMS: usize = 30_000;

/// Finds accounts by multiple hashes, optionally filtering by spent status.
pub async fn find_accounts_by_hashes(
    conn: &DatabaseConnection,
    hashes: &[Hash],
    spent_filter: Option<bool>,
) -> Result<HashMap<Vec<u8>, accounts::Model>, sea_orm::DbErr> {
    let raw_hashes: Vec<Vec<u8>> = hashes.iter().map(|h| h.to_vec()).collect();

    let mut result = HashMap::new();

    for chunk in raw_hashes.chunks(MAX_SQL_PARAMS) {
        let mut query =
            accounts::Entity::find().filter(accounts::Column::Hash.is_in(chunk.to_vec()));

        if let Some(spent) = spent_filter {
            query = query.filter(accounts::Column::Spent.eq(spent));
        }

        let accounts = query.all(conn).await?;

        for account in accounts {
            result.insert(account.hash.clone(), account);
        }
    }

    Ok(result)
}

/// Finds accounts by multiple addresses, optionally filtering by spent status
pub async fn find_accounts_by_addresses(
    conn: &DatabaseConnection,
    addresses: &[SerializablePubkey],
    spent_filter: Option<bool>,
) -> Result<HashMap<Vec<u8>, accounts::Model>, sea_orm::DbErr> {
    let raw_addresses: Vec<Vec<u8>> = addresses.iter().map(|addr| addr.to_bytes_vec()).collect();

    let mut result = HashMap::new();

    for chunk in raw_addresses.chunks(MAX_SQL_PARAMS) {
        let mut query =
            accounts::Entity::find().filter(accounts::Column::Address.is_in(chunk.to_vec()));

        if let Some(spent) = spent_filter {
            query = query.filter(accounts::Column::Spent.eq(spent));
        }

        let accounts = query.all(conn).await?;

        for account in accounts {
            result.insert(account.address.clone().unwrap_or_default(), account);
        }
    }

    Ok(result)
}

/// Finds leaf nodes in state_trees by multiple hashes
pub async fn find_leaf_nodes_by_hashes(
    conn: &DatabaseConnection,
    hashes: &[Hash],
) -> Result<HashMap<Vec<u8>, state_trees::Model>, sea_orm::DbErr> {
    let raw_hashes: Vec<Vec<u8>> = hashes.iter().map(|h| h.to_vec()).collect();

    let mut result = HashMap::new();

    for chunk in raw_hashes.chunks(MAX_SQL_PARAMS) {
        let leaf_nodes = state_trees::Entity::find()
            .filter(
                state_trees::Column::Hash
                    .is_in(chunk.to_vec())
                    .and(state_trees::Column::Level.eq(0)),
            )
            .all(conn)
            .await?;

        for node in leaf_nodes {
            result.insert(node.hash.clone(), node);
        }
    }

    Ok(result)
}

/// Finds a single account by hash
pub async fn find_account_by_hash(
    conn: &DatabaseConnection,
    hash: &Hash,
) -> Result<Option<accounts::Model>, sea_orm::DbErr> {
    accounts::Entity::find()
        .filter(accounts::Column::Hash.eq(hash.to_vec()))
        .one(conn)
        .await
}

/// Finds a single leaf node by hash
pub async fn find_leaf_node_by_hash(
    conn: &DatabaseConnection,
    hash: &Hash,
) -> Result<Option<state_trees::Model>, sea_orm::DbErr> {
    state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .eq(hash.to_vec())
                .and(state_trees::Column::Level.eq(0)),
        )
        .one(conn)
        .await
}
