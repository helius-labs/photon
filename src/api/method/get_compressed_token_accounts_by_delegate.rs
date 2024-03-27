use sea_orm::DatabaseConnection;

use super::{
    super::error::PhotonApiError,
    utils::{
        fetch_token_accounts, Authority, GetCompressedTokenAccountsByAuthority,
        TokenAccountListResponse,
    },
};

pub async fn get_compressed_account_token_accounts_by_delegate(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByAuthority,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedTokenAccountsByAuthority(delegate, options) = request;
    fetch_token_accounts(conn, Authority::Delegate(delegate), options).await
}
