use crate::dao::generated::{accounts, blocks, state_trees, token_accounts};

use schemars::JsonSchema;
use sea_orm::sea_query::SimpleExpr;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect,
};
use serde::{de, Deserialize, Deserializer, Serialize};

use crate::dao::typedefs::hash::{Hash, ParseHashError};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::super::error::PhotonApiError;
use sea_orm_migration::sea_query::Expr;

pub const PAGE_LIMIT: u64 = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct Limit(u64);

impl Limit {
    pub fn new(value: u64) -> Result<Self, &'static str> {
        if value > PAGE_LIMIT {
            Err("Value must be less than or equal to 1000")
        } else {
            Ok(Limit(value))
        }
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Limit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if value > PAGE_LIMIT {
            Err(de::Error::invalid_value(
                de::Unexpected::Unsigned(value),
                &"a value less than or equal to 1000",
            ))
        } else {
            Ok(Limit(value))
        }
    }
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

#[derive(FromQueryResult)]
struct ContextModel {
    // Postgres and SQLlite do not support u64 as return type. We need to use i64 and cast it to u64.
    slot: i64,
}

impl Context {
    pub async fn extract(db: &DatabaseConnection) -> Result<Self, PhotonApiError> {
        let context = blocks::Entity::find()
            .select_only()
            .column_as(Expr::col(blocks::Column::Slot).max(), "slot")
            .into_model::<ContextModel>()
            .one(db)
            .await?
            .ok_or(PhotonApiError::RecordNotFound(
                "No data has been indexed".to_string(),
            ))?;
        Ok(Context {
            slot: context.slot as u64,
        })
    }
}

pub type AccountResponse = ResponseWithContext<Account>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Account {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: String,
    pub owner: SerializablePubkey,
    pub lamports: u64,
    pub tree: Option<SerializablePubkey>,
    pub seq: Option<u64>,
    pub slot_updated: u64,
}

pub fn parse_account_model(account: accounts::Model) -> Result<Account, PhotonApiError> {
    Ok(Account {
        hash: account.hash.try_into()?,
        address: account
            .address
            .map(SerializablePubkey::try_from)
            .transpose()?,
        #[allow(deprecated)]
        data: base64::encode(account.data),
        owner: account.owner.try_into()?,
        tree: account.tree.map(|tree| tree.try_into()).transpose()?,
        lamports: account.lamports as u64,
        slot_updated: account.slot_updated as u64,
        seq: account.seq.map(|seq| seq as u64),
    })
}

pub type TokenAccountListResponse = ResponseWithContext<TokenAccountList>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenAcccount {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub owner: SerializablePubkey,
    pub mint: SerializablePubkey,
    pub amount: u64,
    pub delegate: Option<SerializablePubkey>,
    pub is_native: bool,
    pub close_authority: Option<SerializablePubkey>,
    pub frozen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenAccountList {
    pub items: Vec<TokenAcccount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

pub enum Authority {
    Owner(SerializablePubkey),
    Delegate(SerializablePubkey),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByAuthorityOptions {
    pub mint: Option<SerializablePubkey>,
    pub cursor: Option<String>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByAuthority(
    pub SerializablePubkey,
    pub Option<GetCompressedTokenAccountsByAuthorityOptions>,
);

pub async fn fetch_token_accounts(
    conn: &sea_orm::DatabaseConnection,
    owner_or_delegate: Authority,
    options: Option<GetCompressedTokenAccountsByAuthorityOptions>,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let mut filter = match owner_or_delegate {
        Authority::Owner(owner) => token_accounts::Column::Owner.eq::<Vec<u8>>(owner.into()),
        Authority::Delegate(delegate) => {
            token_accounts::Column::Delegate.eq::<Vec<u8>>(delegate.into())
        }
    }
    .and(token_accounts::Column::Spent.eq(false));

    let mut limit = PAGE_LIMIT;
    if let Some(options) = options {
        if let Some(mint) = options.mint {
            filter = filter.and(token_accounts::Column::Mint.eq::<Vec<u8>>(mint.into()));
        }
        if let Some(cursor) = options.cursor {
            let bytes = bs58::decode(cursor.clone()).into_vec().map_err(|_| {
                PhotonApiError::ValidationError(format!("Invalid cursor {}", cursor))
            })?;
            let expected_cursor_length = 64;
            if bytes.len() != expected_cursor_length {
                return Err(PhotonApiError::ValidationError(format!(
                    "Invalid cursor length. Expected {}. Received {}.",
                    expected_cursor_length,
                    bytes.len()
                )));
            }
            let (mint, hash) = bytes.split_at(32);
            filter = filter
                .and(token_accounts::Column::Mint.gte::<Vec<u8>>(mint.into()))
                .and(token_accounts::Column::Hash.gt::<Vec<u8>>(hash.into()));
        }
        if let Some(l) = options.limit {
            limit = l.value();
        }
    }

    let result = token_accounts::Entity::find()
        .filter(filter)
        .order_by_asc(token_accounts::Column::Mint)
        .order_by_asc(token_accounts::Column::Hash)
        .limit(limit)
        .all(conn)
        .await?;

    let items: Result<Vec<TokenAcccount>, PhotonApiError> =
        result.into_iter().map(parse_token_accounts_model).collect();
    let items = items?;
    let mut cursor = items.last().map(|item| {
        bs58::encode::<Vec<u8>>({
            let item = item.clone();
            let mut bytes: Vec<u8> = item.mint.into();
            let hash_bytes: Vec<u8> = item.hash.into();
            bytes.extend_from_slice(hash_bytes.as_slice());
            bytes
        })
        .into_string()
    });
    if items.len() < limit as usize {
        cursor = None;
    }

    Ok(TokenAccountListResponse {
        value: TokenAccountList { items, cursor },
        context,
    })
}

pub fn parse_token_accounts_model(
    token_account: token_accounts::Model,
) -> Result<TokenAcccount, PhotonApiError> {
    Ok(TokenAcccount {
        hash: token_account.hash.try_into()?,
        address: token_account
            .address
            .map(SerializablePubkey::try_from)
            .transpose()?,
        owner: token_account.owner.try_into()?,
        mint: token_account.mint.try_into()?,
        amount: token_account.amount as u64,
        delegate: token_account
            .delegate
            .map(SerializablePubkey::try_from)
            .transpose()?,
        is_native: token_account.is_native.is_some(),
        close_authority: token_account
            .close_authority
            .map(SerializablePubkey::try_from)
            .transpose()?,
        frozen: token_account.frozen,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CompressedAccountRequest {
    pub address: Option<SerializablePubkey>,
    pub hash: Option<Hash>,
}

pub enum AccountIdentifier {
    Address(SerializablePubkey),
    Hash(Hash),
}

pub enum AccountDataTable {
    Accounts,
    TokenAccounts,
}

impl AccountIdentifier {
    pub fn filter(&self, table: AccountDataTable) -> SimpleExpr {
        match table {
            AccountDataTable::Accounts => match &self {
                AccountIdentifier::Address(address) => {
                    accounts::Column::Address.eq::<Vec<u8>>(address.clone().into())
                }
                AccountIdentifier::Hash(hash) => accounts::Column::Hash.eq(hash.to_vec()),
            }
            .and(accounts::Column::Spent.eq(false)),
            AccountDataTable::TokenAccounts => match &self {
                AccountIdentifier::Address(address) => {
                    token_accounts::Column::Owner.eq::<Vec<u8>>(address.clone().into())
                }
                AccountIdentifier::Hash(hash) => token_accounts::Column::Hash.eq(hash.to_vec()),
            }
            .and(token_accounts::Column::Spent.eq(false)),
        }
    }

    pub fn not_found_error(&self) -> PhotonApiError {
        match &self {
            AccountIdentifier::Address(address) => {
                PhotonApiError::RecordNotFound(format!("Account {} not found", address))
            }
            AccountIdentifier::Hash(hash) => {
                PhotonApiError::RecordNotFound(format!("Account with hash {} not found", hash))
            }
        }
    }
}

impl CompressedAccountRequest {
    pub fn parse_id(&self) -> Result<AccountIdentifier, PhotonApiError> {
        if let Some(address) = &self.address {
            Ok(AccountIdentifier::Address(address.clone()))
        } else if let Some(hash) = &self.hash {
            Ok(AccountIdentifier::Hash(hash.clone()))
        } else {
            Err(PhotonApiError::ValidationError(
                "Either address or hash must be provided".to_string(),
            ))
        }
    }
}

#[derive(FromQueryResult)]
pub struct BalanceModel {
    pub amount: i64,
}

#[derive(FromQueryResult)]
pub struct LamportModel {
    pub lamports: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProofResponse {
    pub root: Hash,
    pub proof: Vec<Hash>,
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
