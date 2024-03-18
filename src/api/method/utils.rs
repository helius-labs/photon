use crate::dao::generated::{blocks, state_trees, token_owners, utxos};

use schemars::JsonSchema;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect,
};
use serde::{Deserialize, Serialize};

use crate::dao::typedefs::hash::{Hash, ParseHashError};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::super::error::PhotonApiError;
use sea_orm_migration::sea_query::Expr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Utxo {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: String,
    pub owner: SerializablePubkey,
    pub lamports: u64,
    pub tree: Option<SerializablePubkey>,
    pub seq: Option<u64>,
    pub slot_updated: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountRequest {
    pub address: SerializablePubkey,
}

pub fn parse_utxo_model(utxo: utxos::Model) -> Result<Utxo, PhotonApiError> {
    Ok(Utxo {
        hash: utxo.hash.try_into()?,
        address: utxo.account.map(SerializablePubkey::try_from).transpose()?,
        #[allow(deprecated)]
        data: base64::encode(utxo.data),
        owner: utxo.owner.try_into()?,
        tree: utxo.tree.map(|tree| tree.try_into()).transpose()?,
        lamports: utxo.lamports as u64,
        slot_updated: utxo.slot_updated as u64,
        seq: utxo.seq.map(|seq| seq as u64),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenUxto {
    pub owner: SerializablePubkey,
    pub mint: SerializablePubkey,
    pub amount: u64,
    pub delegate: Option<SerializablePubkey>,
    pub is_native: bool,
    pub close_authority: Option<SerializablePubkey>,
    pub frozen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenAccountList {
    // TODO: Add cursor
    pub items: Vec<TokenUxto>,
}

pub enum OwnerOrDelegate {
    Owner(SerializablePubkey),
    Delegate(SerializablePubkey),
}

pub async fn fetch_token_accounts(
    conn: &sea_orm::DatabaseConnection,
    owner_or_delegate: OwnerOrDelegate,
    mint: Option<SerializablePubkey>,
) -> Result<TokenAccountList, PhotonApiError> {
    let mut filter = match owner_or_delegate {
        OwnerOrDelegate::Owner(owner) => token_owners::Column::Owner.eq::<Vec<u8>>(owner.into()),
        OwnerOrDelegate::Delegate(delegate) => {
            token_owners::Column::Delegate.eq::<Vec<u8>>(delegate.into())
        }
    };
    if let Some(m) = mint {
        filter = filter.and(token_owners::Column::Mint.eq::<Vec<u8>>(m.into()));
    }

    let result = token_owners::Entity::find()
        .filter(filter)
        .order_by_asc(token_owners::Column::Mint)
        .order_by_asc(token_owners::Column::Hash)
        .all(conn)
        .await?;

    let items: Result<Vec<TokenUxto>, PhotonApiError> =
        result.into_iter().map(parse_token_owners_model).collect();
    let items = items?;

    Ok(TokenAccountList { items })
}

pub fn parse_token_owners_model(
    token_owner: token_owners::Model,
) -> Result<TokenUxto, PhotonApiError> {
    Ok(TokenUxto {
        owner: token_owner.owner.try_into()?,
        mint: token_owner.mint.try_into()?,
        amount: token_owner.amount as u64,
        delegate: token_owner
            .delegate
            .map(SerializablePubkey::try_from)
            .transpose()?,
        is_native: token_owner.is_native.is_some(),
        close_authority: token_owner
            .close_authority
            .map(SerializablePubkey::try_from)
            .transpose()?,
        frozen: token_owner.frozen,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CompressedAccountRequest {
    pub address: SerializablePubkey,
}

pub fn get_proof_path(index: i64) -> Vec<i64> {
    let mut indexes = vec![];
    let mut idx = index;
    while idx > 1 {
        if idx % 2 == 0 {
            indexes.push(idx + 1)
        } else {
            indexes.push(idx - 1)
        }
        idx >>= 1
    }
    indexes.push(1);
    indexes
}

#[derive(FromQueryResult)]
pub struct BalanceModel {
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResponseWithContext<T> {
    pub context: Context,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromQueryResult)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Context {
    pub slot: u64,
}

impl Context {
    pub async fn extract(db: &DatabaseConnection) -> Result<Self, PhotonApiError> {
        Ok(blocks::Entity::find()
            .column_as(Expr::col(blocks::Column::Slot).max(), "slot")
            .into_model::<Context>()
            .one(db)
            .await?
            .ok_or(PhotonApiError::RecordNotFound(
                "No data has been indexed".to_string(),
            ))?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProofResponse {
    pub root: Hash,
    pub proof: Vec<Hash>,
}

pub fn build_full_proof(
    proof_nodes: Vec<state_trees::Model>,
    required_node_indices: Vec<i64>,
) -> Result<ProofResponse, ParseHashError> {
    let depth = required_node_indices.len();
    let mut full_proof = vec![vec![0; 32]; depth];

    // Important:
    // We assume that the proof_nodes are already sorted in descending order (by level).
    for node in proof_nodes {
        full_proof[node.level as usize] = node.hash
    }

    let hashes: Result<Vec<Hash>, _> = full_proof
        .iter()
        .map(|proof| Hash::try_from(proof.clone()))
        .collect();
    let hashes = hashes?;

    Ok(ProofResponse {
        root: hashes[depth - 1].clone(),
        proof: hashes[..(depth - 1)].to_vec(),
    })
}

#[test]
fn test_build_full_proof() {
    // Beautiful art.
    //           1
    //        /     \
    //       /       \
    //      2          3
    //     / \        /  \
    //    /   \      /    \
    //   4     5    6      7
    //  / \   / \  / \    / \
    // 8  9  10 11 12 13 14 15

    // Let's say we want to prove leaf = 12
    // and we have never indexed any data beyond leaf 12.
    // This means nodes 7, 13, 14, 15 are mising from the DB.
    let required_node_indices: Vec<i64> = vec![1, 2, 7, 13];

    // We don't care about these values for this test.
    let tree = vec![0; 32];
    let seq = 0;
    let slot_updated = 0;

    // These are the nodes we got from the DB.
    // DB response is sorted in ascending order by level (leaf to root; bottom to top).
    let root_node = state_trees::Model {
        id: 1,
        node_idx: 1,
        leaf_idx: None,
        level: 3,
        hash: vec![1; 32],
        tree: tree.clone(),
        seq,
        slot_updated,
    };
    let node_2 = state_trees::Model {
        id: 2,
        node_idx: 2,
        leaf_idx: None,
        level: 2,
        hash: vec![2; 32],
        tree: tree.clone(),
        seq,
        slot_updated,
    };
    let proof_nodes = vec![root_node.clone(), node_2.clone()];
    let ProofResponse { proof, root } =
        build_full_proof(proof_nodes, required_node_indices.clone()).unwrap();

    // The root hash should correspond to the first node.
    assert_eq!(root, Hash::try_from(root_node.hash).unwrap());
    // And the proof size should match the tree depth, minus one value (root hash).
    assert_eq!(proof.len(), required_node_indices.len() - 1);

    // The first two values in the proof correspond to node 13 and 7, respectively.
    // Neither have an indexed node in the DB, and should be replaced with an empty hash.
    //
    // The last value corresponds to node 2 which was indexed in the DB.
    assert_eq!(proof[0], Hash::try_from(vec![0; 32]).unwrap());
    assert_eq!(proof[1], Hash::try_from(vec![0; 32]).unwrap());
    assert_eq!(proof[2], Hash::try_from(node_2.hash).unwrap());
}
