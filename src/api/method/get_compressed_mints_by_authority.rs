use crate::api::error::PhotonApiError;
use crate::api::method::get_compressed_mint::{mint_model_to_mint_data, CompressedMint};
use crate::api::method::utils::PAGE_LIMIT;
use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::bs58_string::Base58String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::limit::Limit;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::{accounts, mints};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, Order, QueryFilter, QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Authority type filter for mint queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub enum MintAuthorityType {
    /// Filter by mint authority only
    MintAuthority,
    /// Filter by freeze authority only
    FreezeAuthority,
    /// Filter by either mint authority or freeze authority (default)
    #[default]
    Both,
}

/// Request for getting compressed mints by authority
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedMintsByAuthorityRequest {
    /// The authority pubkey to search for
    pub authority: SerializablePubkey,
    /// Type of authority to filter by
    pub authority_type: MintAuthorityType,
    /// Pagination cursor
    #[serde(default)]
    pub cursor: Option<Base58String>,
    /// Maximum number of results to return
    #[serde(default)]
    pub limit: Option<Limit>,
}

/// List of compressed mints with pagination
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct CompressedMintList {
    pub items: Vec<CompressedMint>,
    pub cursor: Option<Base58String>,
}

/// Response for getCompressedMintsByAuthority
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedMintsByAuthorityResponse {
    pub context: Context,
    pub value: CompressedMintList,
}

pub async fn get_compressed_mints_by_authority(
    conn: &DatabaseConnection,
    request: GetCompressedMintsByAuthorityRequest,
) -> Result<GetCompressedMintsByAuthorityResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let authority_bytes: Vec<u8> = request.authority.into();
    let authority_type = request.authority_type;

    // Build the filter based on authority type
    let mut filter = mints::Column::Spent.eq(false);
    match authority_type {
        MintAuthorityType::MintAuthority => {
            filter = filter.and(mints::Column::MintAuthority.eq(authority_bytes.clone()));
        }
        MintAuthorityType::FreezeAuthority => {
            filter = filter.and(mints::Column::FreezeAuthority.eq(authority_bytes.clone()));
        }
        MintAuthorityType::Both => {
            filter = filter.and(
                mints::Column::MintAuthority
                    .eq(authority_bytes.clone())
                    .or(mints::Column::FreezeAuthority.eq(authority_bytes.clone())),
            );
        }
    }

    // Apply cursor pagination
    let mut limit = PAGE_LIMIT;
    if let Some(cursor) = request.cursor {
        let bytes = cursor.0;
        if bytes.len() != 32 {
            return Err(PhotonApiError::ValidationError(format!(
                "Invalid cursor length. Expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        filter = filter.and(mints::Column::Hash.gt(bytes));
    }
    if let Some(l) = request.limit {
        limit = l.value();
    }

    // Query mints with limit first
    let mint_results = mints::Entity::find()
        .filter(filter)
        .order_by(mints::Column::Hash, Order::Asc)
        .limit(limit)
        .all(conn)
        .await?;

    // Then fetch the related accounts
    let mut items = Vec::with_capacity(mint_results.len());
    for mint in mint_results {
        let account = accounts::Entity::find()
            .filter(accounts::Column::Hash.eq(mint.hash.clone()))
            .one(conn)
            .await?;

        if let Some(account) = account {
            let mint_data = mint_model_to_mint_data(&mint)?;
            let account_v2: AccountV2 = account.try_into()?;
            items.push(CompressedMint {
                account: account_v2,
                mint_data,
            });
        }
    }

    // Build cursor from last item
    let mut cursor = items.last().map(|item| {
        let hash_bytes: Vec<u8> = item.account.hash.clone().into();
        Base58String(hash_bytes)
    });
    if items.len() < limit as usize {
        cursor = None;
    }

    Ok(GetCompressedMintsByAuthorityResponse {
        context,
        value: CompressedMintList { items, cursor },
    })
}
