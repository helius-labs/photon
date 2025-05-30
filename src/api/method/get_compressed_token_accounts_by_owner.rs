use sea_orm::DatabaseConnection;

use super::utils::{
    fetch_token_accounts_v2, Authority, GetCompressedTokenAccountsByAuthorityOptions,
    GetCompressedTokenAccountsByOwner, TokenAccountListResponse, TokenAccountListResponseV2,
};
use super::{super::error::PhotonApiError, utils::fetch_token_accounts};

pub async fn get_compressed_token_accounts_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByOwner,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedTokenAccountsByOwner {
        owner,
        mint,
        cursor,
        limit,
    } = request;
    let options = GetCompressedTokenAccountsByAuthorityOptions {
        mint,
        cursor,
        limit,
    };
    fetch_token_accounts(conn, Authority::Owner(owner), options).await
}

pub async fn get_compressed_token_accounts_by_owner_v2(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByOwner,
) -> Result<TokenAccountListResponseV2, PhotonApiError> {
    let GetCompressedTokenAccountsByOwner {
        owner,
        mint,
        cursor,
        limit,
    } = request;
    let options = GetCompressedTokenAccountsByAuthorityOptions {
        mint,
        cursor,
        limit,
    };
    fetch_token_accounts_v2(conn, Authority::Owner(owner), options).await
}
