use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;

use super::get_token_account_interface::get_token_account_interface;
use super::types::{
    GetAtaInterfaceRequest, GetAtaInterfaceResponse, GetTokenAccountInterfaceRequest,
};

const LIGHT_TOKEN_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("cTokenmWW8bLPjZEBAUgYy3zKxQZW6VKi7bqNFEVv3m");

/// Get Associated Token Account data from either on-chain or compressed sources.
pub async fn get_ata_interface(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetAtaInterfaceRequest,
) -> Result<GetAtaInterfaceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let owner_pubkey = Pubkey::from(request.owner.0.to_bytes());
    let mint_pubkey = Pubkey::from(request.mint.0.to_bytes());

    let ata_address = derive_light_ata(&owner_pubkey, &mint_pubkey)?;
    let ata_serializable = SerializablePubkey::from(ata_address.to_bytes());

    let token_request = GetTokenAccountInterfaceRequest {
        address: ata_serializable,
    };

    let token_response = get_token_account_interface(conn, rpc_client, token_request).await?;

    Ok(GetAtaInterfaceResponse {
        context,
        value: token_response.value,
    })
}

fn derive_light_ata(owner: &Pubkey, mint: &Pubkey) -> Result<Pubkey, PhotonApiError> {
    let seeds = [
        owner.as_ref(),
        LIGHT_TOKEN_PROGRAM_ID.as_ref(),
        mint.as_ref(),
    ];

    let (ata, _bump) = Pubkey::find_program_address(&seeds, &LIGHT_TOKEN_PROGRAM_ID);
    Ok(ata)
}