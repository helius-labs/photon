use sea_orm::DatabaseConnection;

use super::{
    super::error::PhotonApiError,
    utils::{
        fetch_token_accounts, Authority, GetCompressedAccountsByAuthority, TokenAccountListResponse,
    },
};

pub async fn get_compressed_account_token_accounts_by_delegate(
    conn: &DatabaseConnection,
    request: GetCompressedAccountsByAuthority,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedAccountsByAuthority(delegate, options) = request;
    fetch_token_accounts(
        conn,
        Authority::Delegate(delegate),
        options.map(|o| o.mint).flatten(),
    )
    .await
}
