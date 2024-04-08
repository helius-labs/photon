use sea_orm::DatabaseConnection;

use super::{
    super::error::PhotonApiError,
    utils::{
        fetch_token_accounts, Authority, GetCompressedTokenAccountsByAuthorityOptions,
        GetCompressedTokenAccountsByDelegate, TokenAccountListResponse,
    },
};

pub async fn get_compressed_account_token_accounts_by_delegate(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByDelegate,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedTokenAccountsByDelegate {
        delegate,
        mint,
        cursor,
        limit,
    } = request;
    let options = GetCompressedTokenAccountsByAuthorityOptions {
        mint,
        cursor,
        limit,
    };
    fetch_token_accounts(conn, Authority::Delegate(delegate), options).await
}
