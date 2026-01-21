use crate::api::error::PhotonApiError;
use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::mint_data::MintData;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::{accounts, mints};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request for getting a compressed mint
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedMintRequest {
    /// The compressed address of the mint
    #[serde(default)]
    pub address: Option<SerializablePubkey>,
    /// The mint PDA (Solana account address when decompressed)
    #[serde(default)]
    pub mint_pda: Option<SerializablePubkey>,
}

/// Compressed mint account with underlying account data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CompressedMint {
    pub account: AccountV2,
    pub mint_data: MintData,
}

/// Response for getCompressedMint
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedMintResponse {
    pub context: Context,
    pub value: Option<CompressedMint>,
}

/// Convert mint model to MintData.
pub fn mint_model_to_mint_data(mint: &mints::Model) -> Result<MintData, PhotonApiError> {
    Ok(MintData {
        mint_pda: mint.mint_pda.clone().try_into()?,
        mint_signer: mint.mint_signer.clone().try_into()?,
        mint_authority: mint
            .mint_authority
            .clone()
            .map(SerializablePubkey::try_from)
            .transpose()?,
        freeze_authority: mint
            .freeze_authority
            .clone()
            .map(SerializablePubkey::try_from)
            .transpose()?,
        supply: UnsignedInteger(mint.supply as u64),
        decimals: mint.decimals as u8,
        version: mint.version as u8,
        mint_decompressed: mint.mint_decompressed,
        extensions: mint.extensions.clone().map(Base64String),
    })
}

pub async fn get_compressed_mint(
    conn: &DatabaseConnection,
    request: GetCompressedMintRequest,
) -> Result<GetCompressedMintResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    // Validate request - at least one identifier must be provided
    if request.address.is_none() && request.mint_pda.is_none() {
        return Err(PhotonApiError::ValidationError(
            "Either address or mint_pda must be provided".to_string(),
        ));
    }

    // Build the filter
    let mut filter = mints::Column::Spent.eq(false);
    if let Some(address) = request.address {
        filter = filter.and(mints::Column::Address.eq::<Vec<u8>>(address.into()));
    }
    if let Some(mint_pda) = request.mint_pda {
        filter = filter.and(mints::Column::MintPda.eq::<Vec<u8>>(mint_pda.into()));
    }

    // Query the mints table with joined accounts
    let result = mints::Entity::find()
        .find_also_related(accounts::Entity)
        .filter(filter)
        .one(conn)
        .await?;

    let value = match result {
        Some((mint, Some(account))) => {
            let mint_data = mint_model_to_mint_data(&mint)?;
            let account: AccountV2 = account.try_into()?;
            Some(CompressedMint { account, mint_data })
        }
        _ => None,
    };

    Ok(GetCompressedMintResponse { context, value })
}
