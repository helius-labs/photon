use sea_orm::DatabaseConnection;

use super::utils::{Authority, GetCompressedAccountsByAuthority, TokenAccountListResponse};
use super::{super::error::PhotonApiError, utils::fetch_token_accounts};

pub async fn get_compressed_token_accounts_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedAccountsByAuthority,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedAccountsByAuthority(delegate, options) = request;
    fetch_token_accounts(
        conn,
        Authority::Owner(delegate),
        options.and_then(|o| o.mint),
    )
    .await
}
