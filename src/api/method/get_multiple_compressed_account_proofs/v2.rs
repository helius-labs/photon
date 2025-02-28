use crate::api::error::PhotonApiError;
use crate::api::method::utils::PAGE_LIMIT;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::{accounts, state_trees};
use crate::ingester::persist::{
    get_multiple_compressed_leaf_proofs, get_multiple_compressed_leaf_proofs_by_indices,
    MerkleProofWithContext,
};
use jsonrpsee_core::Serialize;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    Statement, TransactionTrait,
};
use serde::Deserialize;
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountProofsResponseV2 {
    pub context: Context,
    pub value: Vec<GetMultipleCompressedAccountProofsResponseValueV2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountProofsResponseValueV2 {
    pub proof: Vec<Hash>,
    pub root: Hash,
    pub leaf_index: u32,
    pub hash: Hash,
    pub merkle_tree: SerializablePubkey,
    pub queue: SerializablePubkey,
    pub root_seq: u64,
    pub prove_by_index: bool,
    pub tree_type: u16,
}

impl From<MerkleProofWithContext> for GetMultipleCompressedAccountProofsResponseValueV2 {
    fn from(proof: MerkleProofWithContext) -> Self {
        GetMultipleCompressedAccountProofsResponseValueV2 {
            proof: proof.proof,
            root: proof.root,
            leaf_index: proof.leaf_index,
            hash: proof.hash,
            merkle_tree: proof.merkle_tree,
            root_seq: proof.root_seq,
            // Default values to be overridden as needed
            prove_by_index: false,
            tree_type: 0,
            queue: SerializablePubkey::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HashList(pub Vec<Hash>);

pub async fn get_multiple_compressed_account_proofs_v2(
    conn: &DatabaseConnection,
    request: HashList,
) -> Result<GetMultipleCompressedAccountProofsResponseV2, PhotonApiError> {
    let hashes = request.0;

    // Validate input size
    if hashes.len() > PAGE_LIMIT as usize {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many hashes requested {}. Maximum allowed: {}",
            hashes.len(),
            PAGE_LIMIT
        )));
    }

    let context = Context::extract(conn).await?;
    let tx = conn.begin().await?;

    // Set transaction isolation level for PostgreSQL
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }

    // Find accounts for all hashes
    let accounts = accounts::Entity::find()
        .filter(accounts::Column::Hash.is_in(hashes.iter().map(|h| h.to_vec()).collect::<Vec<_>>()))
        .all(&tx)
        .await?;

    if accounts.len() != hashes.len() {
        return Err(PhotonApiError::RecordNotFound(
            "Some accounts not found".to_string(),
        ));
    }

    // Create a map from hash to account for easy lookup
    let account_map: HashMap<Vec<u8>, accounts::Model> = accounts
        .into_iter()
        .map(|acc| (acc.hash.clone(), acc))
        .collect();

    // Find leaf nodes in state_trees for all hashes
    let leaf_nodes = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .is_in(hashes.iter().map(|h| h.to_vec()).collect::<Vec<_>>())
                .and(state_trees::Column::Level.eq(0)),
        )
        .all(&tx)
        .await?;

    // Create a set of hashes found in state_trees
    let state_tree_hashes: std::collections::HashSet<Vec<u8>> =
        leaf_nodes.iter().map(|node| node.hash.clone()).collect();

    // Split hashes into those found in state_trees (for hash-based proofs)
    // and those only found in accounts (for index-based proofs)
    let mut hash_based_proofs: Vec<Hash> = Vec::new();
    let mut index_based_proofs: Vec<(Hash, SerializablePubkey, u64)> = Vec::new();

    for hash in &hashes {
        if state_tree_hashes.contains(&hash.to_vec()) {
            // Found in state_trees, use hash-based proof
            hash_based_proofs.push(hash.clone());
        } else if let Some(account) = account_map.get(&hash.to_vec()) {
            // Found in accounts but not in state_trees, use index-based proof
            let merkle_tree = SerializablePubkey::try_from(account.tree.clone())?;
            let leaf_index = account.leaf_index as u64;
            index_based_proofs.push((hash.clone(), merkle_tree, leaf_index));
        }
    }

    // Get proofs for both methods
    let mut hash_based_result = if !hash_based_proofs.is_empty() {
        get_multiple_compressed_leaf_proofs(&tx, hash_based_proofs)
            .await?
            .into_iter()
            .map(|proof| {
                let mut response_value: GetMultipleCompressedAccountProofsResponseValueV2 =
                    proof.into();
                response_value.prove_by_index = false;
                response_value
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // Process index-based proofs
    let mut index_based_result = Vec::new();
    for (merkle_tree, indices) in index_based_proofs
        .iter()
        .map(|(_, tree, idx)| (tree, idx))
        .fold(HashMap::new(), |mut acc, (tree, idx)| {
            acc.entry(*tree).or_insert_with(Vec::new).push(*idx);
            acc
        })
    {
        let proofs =
            get_multiple_compressed_leaf_proofs_by_indices(&tx, merkle_tree, indices).await?;

        for proof in proofs {
            let mut response_value: GetMultipleCompressedAccountProofsResponseValueV2 =
                proof.into();
            response_value.prove_by_index = true;
            index_based_result.push(response_value);
        }
    }

    // Combine results
    let mut result = Vec::new();
    result.append(&mut hash_based_result);
    result.append(&mut index_based_result);

    // Enrich with account data
    for value in &mut result {
        if let Some(account) = account_map.get(&value.hash.to_vec()) {
            value.tree_type = account.tree_type as u16;
            value.queue = SerializablePubkey::try_from(account.queue.clone())?;
        }
    }

    // Sort the result to match the original request order
    let hash_to_index: HashMap<Vec<u8>, usize> = hashes
        .iter()
        .enumerate()
        .map(|(i, hash)| (hash.to_vec(), i))
        .collect();

    result.sort_by_key(|value| {
        hash_to_index
            .get(&value.hash.to_vec())
            .cloned()
            .unwrap_or(usize::MAX)
    });

    tx.commit().await?;

    Ok(GetMultipleCompressedAccountProofsResponseV2 {
        value: result,
        context,
    })
}
