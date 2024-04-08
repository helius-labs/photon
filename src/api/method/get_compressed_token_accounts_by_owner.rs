use sea_orm::DatabaseConnection;

use super::utils::{
    Authority, GetCompressedTokenAccountsByAuthorityOptions, GetCompressedTokenAccountsByOwner,
    TokenAccountListResponse,
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
