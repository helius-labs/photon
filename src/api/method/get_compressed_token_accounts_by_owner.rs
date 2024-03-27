use sea_orm::DatabaseConnection;

use super::utils::{Authority, GetCompressedTokenAccountsByAuthority, TokenAccountListResponse};
use super::{super::error::PhotonApiError, utils::fetch_token_accounts};

pub async fn get_compressed_token_accounts_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByAuthority,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedTokenAccountsByAuthority(delegate, options) = request;
    fetch_token_accounts(conn, Authority::Owner(delegate), options).await
}
