use std::str::FromStr;

use crate::common::typedefs::account::{Account, AccountData};
use crate::common::typedefs::bs58_string::Base58String;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::serializable_signature::SerializableSignature;
use crate::common::typedefs::token_data::{AccountState, TokenData};
use crate::dao::generated::{accounts, blocks, token_accounts};

use byteorder::{ByteOrder, LittleEndian};
use sea_orm::sea_query::SimpleExpr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, Statement, Value,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Number;
use solana_sdk::signature::Signature;

use sqlx::types::Decimal;
use utoipa::openapi::{ObjectBuilder, RefOr, Schema, SchemaType};
use utoipa::ToSchema;

use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;

use super::super::error::PhotonApiError;
use sea_orm_migration::sea_query::Expr;

pub const PAGE_LIMIT: u64 = 1000;

pub fn parse_decimal(value: Decimal) -> Result<u64, PhotonApiError> {
    value
        .to_string()
        .parse::<u64>()
        .map_err(|_| PhotonApiError::UnexpectedError("Invalid decimal value".to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
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

impl Default for Limit {
    fn default() -> Self {
        Limit(PAGE_LIMIT)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromQueryResult)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Context {
    pub slot: u64,
}

pub fn slot_schema() -> Schema {
    Schema::Object(
        ObjectBuilder::new()
            .schema_type(SchemaType::Integer)
            .description(Some("The current slot"))
            .default(Some(serde_json::Value::Number(
                Number::from_str("1").unwrap(),
            )))
            .example(Some(serde_json::Value::Number(serde_json::Number::from(1))))
            .build(),
    )
}

impl<'__s> ToSchema<'__s> for Context {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Object)
                .property("slot", slot_schema())
                .required("slot")
                .build(),
        );
        ("Context", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, utoipa::openapi::schema::Schema)> {
        Vec::new()
    }
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

pub fn parse_discriminator(discriminator: Option<Vec<u8>>) -> Option<u64> {
    discriminator.map(|discriminator| LittleEndian::read_u64(&discriminator))
}

fn parse_leaf_index(leaf_index: i64) -> Result<u32, PhotonApiError> {
    leaf_index
        .try_into()
        .map_err(|_| PhotonApiError::UnexpectedError("Invalid leaf index".to_string()))
}

pub fn parse_account_model(account: accounts::Model) -> Result<Account, PhotonApiError> {
    let data = match (account.data, account.data_hash, account.discriminator) {
        (Some(data), Some(data_hash), Some(discriminator)) => Some(AccountData {
            data: Base64String(data),
            data_hash: data_hash.try_into()?,
            discriminator: parse_decimal(discriminator)?,
        }),
        (None, None, None) => None,
        _ => {
            return Err(PhotonApiError::UnexpectedError(
                "Invalid account data".to_string(),
            ))
        }
    };

    Ok(Account {
        hash: account.hash.try_into()?,
        address: account
            .address
            .map(SerializablePubkey::try_from)
            .transpose()?,
        data,
        owner: account.owner.try_into()?,
        tree: account.tree.try_into()?,
        leaf_index: parse_leaf_index(account.leaf_index)?,
        lamports: parse_decimal(account.lamports)?,
        slot_updated: account.slot_updated as u64,
        seq: account.seq.map(|seq| seq as u64),
    })
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TokenAccountListResponse {
    pub context: Context,
    pub value: TokenAccountList,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenAcccount {
    pub account: Account,
    pub token_data: TokenData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenAccountList {
    pub items: Vec<TokenAcccount>,
    pub cursor: Option<Base58String>,
}

pub enum Authority {
    Owner(SerializablePubkey),
    Delegate(SerializablePubkey),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByAuthorityOptions {
    pub mint: Option<SerializablePubkey>,
    pub cursor: Option<Base58String>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByOwner {
    pub owner: SerializablePubkey,
    pub mint: Option<SerializablePubkey>,
    pub cursor: Option<Base58String>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByDelegate {
    pub delegate: SerializablePubkey,
    pub mint: Option<SerializablePubkey>,
    pub cursor: Option<Base58String>,
    pub limit: Option<Limit>,
}

#[derive(FromQueryResult)]
pub struct EnrichedTokenAccountModel {
    pub hash: Vec<u8>,
    pub owner: Vec<u8>,
    pub mint: Vec<u8>,
    pub amount: Decimal,
    pub delegate: Option<Vec<u8>>,
    pub frozen: bool,
    pub is_native: Option<Decimal>,
    pub delegated_amount: Decimal,
    pub spent: bool,
    pub slot_updated: i64,
    // Needed for generating proof
    pub data: Vec<u8>,
    pub data_hash: Option<Vec<u8>>,
    pub discriminator: Vec<u8>,
    pub lamports: Decimal,
    pub tree: Option<Vec<u8>>,
    pub leaf_index: Option<i64>,
    pub seq: Option<i64>,
}

pub async fn fetch_token_accounts(
    conn: &sea_orm::DatabaseConnection,
    owner_or_delegate: Authority,
    options: GetCompressedTokenAccountsByAuthorityOptions,
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
    if let Some(mint) = options.mint {
        filter = filter.and(token_accounts::Column::Mint.eq::<Vec<u8>>(mint.into()));
    }
    if let Some(cursor) = options.cursor {
        let bytes = bs58::decode(cursor.0.clone())
            .into_vec()
            .map_err(|_| PhotonApiError::ValidationError(format!("Invalid cursor {}", cursor.0)))?;
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

    let items = token_accounts::Entity::find()
        .find_also_related(accounts::Entity)
        .filter(filter)
        .order_by_asc(token_accounts::Column::Mint)
        .order_by_asc(token_accounts::Column::Hash)
        .limit(limit)
        .all(conn)
        .await?
        .drain(..)
        .map(|(token_account, account)| {
            let account = account.ok_or(PhotonApiError::RecordNotFound(
                "Base account not found for token account".to_string(),
            ))?;
            Ok(TokenAcccount {
                account: parse_account_model(account)?,
                token_data: TokenData {
                    mint: token_account.mint.try_into()?,
                    owner: token_account.owner.try_into()?,
                    amount: parse_decimal(token_account.amount)?,
                    delegate: token_account
                        .delegate
                        .map(SerializablePubkey::try_from)
                        .transpose()?,
                    delegated_amount: parse_decimal(token_account.delegated_amount)?,
                    is_native: token_account.is_native.map(parse_decimal).transpose()?,
                    state: (AccountState::try_from(token_account.state as u8)).map_err(|e| {
                        PhotonApiError::UnexpectedError(format!(
                            "Unable to parse account state {}",
                            e
                        ))
                    })?,
                },
            })
        })
        .collect::<Result<Vec<TokenAcccount>, PhotonApiError>>()?;

    let mut cursor = items.last().map(|item| {
        bs58::encode::<Vec<u8>>({
            let item = item.clone();
            let mut bytes: Vec<u8> = item.token_data.mint.into();
            let hash_bytes: Vec<u8> = item.account.hash.into();
            bytes.extend_from_slice(hash_bytes.as_slice());
            bytes
        })
        .into_string()
    });
    if items.len() < limit as usize {
        cursor = None;
    }

    Ok(TokenAccountListResponse {
        value: TokenAccountList {
            items,
            cursor: cursor.map(Base58String),
        },
        context,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CompressedAccountRequest {
    pub address: Option<SerializablePubkey>,
    pub hash: Option<Hash>,
}

impl CompressedAccountRequest {
    pub fn adjusted_schema() -> RefOr<Schema> {
        let mut schema = CompressedAccountRequest::schema().1;
        let object = match schema {
            RefOr::T(Schema::Object(ref mut object)) => {
                let example = serde_json::to_value(CompressedAccountRequest {
                    hash: Some(Hash::default()),
                    address: None,
                })
                .unwrap();
                object.default = Some(example.clone());
                object.example = Some(example);
                object.description = Some("Request for compressed account data".to_string());
                object.clone()
            }
            _ => unimplemented!(),
        };
        RefOr::T(Schema::Object(object))
    }
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
                    accounts::Column::Address.eq::<Vec<u8>>((*address).into())
                }
                AccountIdentifier::Hash(hash) => accounts::Column::Hash.eq(hash.to_vec()),
            }
            .and(accounts::Column::Spent.eq(false)),
            AccountDataTable::TokenAccounts => match &self {
                AccountIdentifier::Address(address) => {
                    token_accounts::Column::Owner.eq::<Vec<u8>>((*address).into())
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
            Ok(AccountIdentifier::Address(*address))
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
    pub amount: Decimal,
}

#[derive(FromQueryResult)]
pub struct LamportModel {
    pub lamports: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct HashRequest {
    pub hash: Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SignatureInfo {
    pub signature: SerializableSignature,
    pub slot: u64,
    pub block_time: i64,
}

#[derive(FromQueryResult)]
pub struct SignatureInfoModel {
    pub signature: Vec<u8>,
    pub slot: i64,
    pub block_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SignatureInfoList {
    pub items: Vec<SignatureInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedSignatureInfoList {
    pub items: Vec<SignatureInfo>,
    pub cursor: Option<String>,
}

pub enum SignatureFilter {
    Account(Hash),
    Address(SerializablePubkey),
    Owner(SerializablePubkey),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignatureSearchType {
    Standard,
    Token,
}

pub async fn search_for_signatures(
    conn: &DatabaseConnection,
    search_type: SignatureSearchType,
    signature_filter: SignatureFilter,
    cursor: Option<String>,
    limit: Option<Limit>,
) -> Result<PaginatedSignatureInfoList, PhotonApiError> {
    if search_type == SignatureSearchType::Token {
        match signature_filter {
            SignatureFilter::Owner(_) => {}
            _ => {
                return Err(PhotonApiError::ValidationError(
                    "Only owner search is supported for token signatures".to_string(),
                ))
            }
        }
    }

    let base_table = match search_type {
        SignatureSearchType::Standard => "accounts",
        SignatureSearchType::Token => "token_accounts",
    };

    let (filter, arg): (String, Vec<u8>) = match signature_filter {
        SignatureFilter::Account(hash) => ("WHERE account_transactions.hash = $1".to_string(), hash.into()),
        SignatureFilter::Address(address) => {
            ("JOIN accounts ON account_transactions.hash = accounts.hash WHERE account.address = $1".to_string(), address.into())
        }
        SignatureFilter::Owner(owner) => (format!(
            "JOIN {base_table} ON account_transactions.hash = {base_table}.hash WHERE {base_table}.owner = $1"
        ), owner.into()),
    };
    let arg: Value = arg.into();

    let (cursor_filter, cursor_args): (String, Vec<Value>) = match cursor {
        Some(cursor) => {
            let bytes = bs58::decode(cursor.clone()).into_vec().map_err(|_| {
                PhotonApiError::ValidationError(format!("Invalid cursor {}", cursor))
            })?;
            let slot_bytes = 8;
            let signature_bytes = 64;
            let expected_cursor_length = slot_bytes + signature_bytes;
            if bytes.len() != expected_cursor_length {
                return Err(PhotonApiError::ValidationError(format!(
                    "Invalid cursor length. Expected {}. Received {}.",
                    expected_cursor_length,
                    bytes.len()
                )));
            }
            let (slot, signature) = bytes.split_at(slot_bytes);
            let slot = LittleEndian::read_u64(slot);
            let signature = Signature::try_from(signature).map_err(|_| {
                PhotonApiError::ValidationError("Invalid signature in cursor".to_string())
            })?;

            (
                "AND transactions.slot <= $2 AND transactions.signature < $3".to_string(),
                vec![
                    slot.into(),
                    Into::<Vec<u8>>::into(Into::<[u8; 64]>::into(signature)).into(),
                ],
            )
        }
        None => ("".to_string(), vec![]),
    };
    let limit = limit.unwrap_or_default().0;

    let raw_sql = format!(
        "
        SELECT DISTINCT transactions.signature, transactions.slot, blocks.block_time
        FROM account_transactions
        JOIN transactions ON account_transactions.signature = transactions.signature
        JOIN blocks ON transactions.slot = blocks.slot
        {filter}
        {cursor_filter}
        ORDER BY transactions.slot, transactions.signature DESC
        LIMIT {limit}
    "
    );

    let signatures: Vec<SignatureInfoModel> =
        SignatureInfoModel::find_by_statement(Statement::from_sql_and_values(
            conn.get_database_backend(),
            &raw_sql,
            vec![arg].into_iter().chain(cursor_args),
        ))
        .all(conn)
        .await?;

    let signatures = signatures
        .into_iter()
        .map(|signature| {
            Ok(SignatureInfo {
                signature: SerializableSignature(
                    Signature::try_from(signature.signature).map_err(|_| {
                        PhotonApiError::UnexpectedError("Invalid signature".to_string())
                    })?,
                ),
                slot: signature.slot as u64,
                block_time: signature.block_time,
            })
        })
        .collect::<Result<Vec<SignatureInfo>, PhotonApiError>>()?;

    let cursor = match signatures.len() < limit as usize {
        true => None,
        false => signatures.last().map(|signature| {
            bs58::encode::<Vec<u8>>({
                let mut bytes = signature.slot.to_le_bytes().to_vec();
                bytes.extend_from_slice(signature.signature.0.as_ref());
                bytes
            })
            .into_string()
        }),
    };

    Ok(PaginatedSignatureInfoList {
        items: signatures,
        cursor,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// We do not use generics to simplify documentation generation.
pub struct GetPaginatedSignaturesResponse {
    pub context: Context,
    pub value: PaginatedSignatureInfoList,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// We do not use generics to simplify documentation generation.
pub struct AccountBalanceResponse {
    pub context: Context,
    pub value: u64,
}

impl AccountBalanceResponse {
    pub fn adjusted_schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        let mut schema = AccountBalanceResponse::schema().1;
        let object = match schema {
            utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(ref mut object)) => {
                let example = serde_json::to_value(AccountBalanceResponse {
                    context: { Context { slot: 1 } },
                    value: 1,
                })
                .unwrap();
                object.default = Some(example.clone());
                object.example = Some(example);
                object.description = Some("Response for compressed account balance".to_string());
                object.clone()
            }
            _ => unimplemented!(),
        };
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(object))
    }
}
