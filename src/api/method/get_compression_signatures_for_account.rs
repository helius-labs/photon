use super::{
    super::error::PhotonApiError,
    utils::{
        search_for_signatures, GetNonPaginatedSignaturesResponse, HashRequest, SignatureFilter,
        SignatureInfoList, SignatureSearchType,
    },
};
use crate::common::typedefs::context::Context;
use sea_orm::DatabaseConnection;

pub async fn get_compression_signatures_for_account(
    conn: &DatabaseConnection,
    request: HashRequest,
) -> Result<GetNonPaginatedSignaturesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let hash = request.hash;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        Some(SignatureFilter::Account(hash)),
        true,
        None,
        None,
    )
    .await?
    .items;

    if signatures.len() > 2 {
        return Err(PhotonApiError::UnexpectedError(
            "Got too many transactions. This is a bug. An account can be modified at most twice."
                .to_string(),
        ));
    }

    Ok(GetNonPaginatedSignaturesResponse {
        value: SignatureInfoList {
            items: signatures.into_iter().map(|s| s.into()).collect(),
        },
        context,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{DatabaseBackend, MockDatabase, Value};
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn missing_account_returns_empty_signature_list() {
        let connection = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results(vec![vec![BTreeMap::from([(
                "slot",
                Value::BigInt(Some(42)),
            )])]])
            .append_query_results(vec![Vec::<BTreeMap<&str, Value>>::new()])
            .into_connection();

        let response = get_compression_signatures_for_account(&connection, HashRequest::default())
            .await
            .unwrap();

        assert_eq!(response.context.slot, 42);
        assert!(response.value.items.is_empty());
    }
}
