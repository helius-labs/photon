use std::sync::Arc;

use sea_orm::{ConnectionTrait, DatabaseConnection, SqlxPostgresConnector, Statement};
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::{postgres::PgPoolOptions, Executor};
use utoipa::openapi::{ObjectBuilder, RefOr, Schema, SchemaType};
use utoipa::ToSchema;

use crate::common::typedefs::unsigned_integer::UnsignedInteger;

use super::method::get_compressed_account::AccountResponse;
use super::method::get_compressed_balance_by_owner::{
    get_compressed_balance_by_owner, GetCompressedBalanceByOwnerRequest,
};
use super::method::get_compressed_token_balances_by_owner::{
    get_compressed_token_balances_by_owner, GetCompressedTokenBalancesByOwnerRequest,
    TokenBalancesResponse,
};
use super::method::get_compression_signatures_for_account::{
    get_compression_signatures_for_account, GetCompressionSignaturesForAccountResponse,
};
use super::method::get_compression_signatures_for_address::{
    get_compression_signatures_for_address, GetCompressionSignaturesForAddressRequest,
};
use super::method::get_compression_signatures_for_owner::{
    get_compression_signatures_for_owner, GetCompressionSignaturesForOwnerRequest,
};
use super::method::get_compression_signatures_for_token_owner::{
    get_compression_signatures_for_token_owner, GetCompressionSignaturesForTokenOwnerRequest,
};
use super::method::get_latest_compression_signatures::{
    get_latest_compression_signatures, GetLatestCompressionSignaturesRequest,
};
use super::method::get_transaction_with_compression_info::{
    get_transaction_with_compression_info, GetTransactionRequest, GetTransactionResponse,
};
use super::method::utils::{AccountBalanceResponse, GetPaginatedSignaturesResponse, HashRequest};
use super::{
    error::PhotonApiError,
    method::{
        get_compressed_account::get_compressed_account,
        get_compressed_account_proof::{
            get_compressed_account_proof, GetCompressedAccountProofResponse,
        },
        get_compressed_accounts_by_owner::{
            get_compressed_accounts_by_owner, GetCompressedAccountsByOwnerRequest,
            GetCompressedAccountsByOwnerResponse,
        },
        get_compressed_balance::get_compressed_balance,
        get_compressed_token_account_balance::{
            get_compressed_token_account_balance, GetCompressedTokenAccountBalanceResponse,
        },
        get_compressed_token_accounts_by_delegate::get_compressed_account_token_accounts_by_delegate,
        get_compressed_token_accounts_by_owner::get_compressed_token_accounts_by_owner,
        get_indexer_health::get_indexer_health,
        get_indexer_slot::get_indexer_slot,
        get_multiple_compressed_account_proofs::{
            get_multiple_compressed_account_proofs, GetMultipleCompressedAccountProofsResponse,
            HashList,
        },
        get_multiple_compressed_accounts::{
            get_multiple_compressed_accounts, GetMultipleCompressedAccountsRequest,
            GetMultipleCompressedAccountsResponse,
        },
        utils::{
            CompressedAccountRequest, GetCompressedTokenAccountsByDelegate,
            GetCompressedTokenAccountsByOwner, TokenAccountListResponse,
        },
    },
};

pub struct PhotonApiConfig {
    pub db_url: String,
    pub max_conn: i32,
    pub timeout_seconds: i32,
    pub rpc_url: String,
}

pub struct PhotonApi {
    db_conn: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
}

impl PhotonApi {
    pub async fn new_from_config(config: PhotonApiConfig) -> Result<Self, anyhow::Error> {
        let PhotonApiConfig {
            db_url,
            max_conn,
            timeout_seconds,
            ..
        } = config;
        let db_conn = init_pool(&db_url, max_conn, timeout_seconds).await?;
        let rpc_client = Arc::new(RpcClient::new(config.rpc_url));
        Ok(Self {
            db_conn: Arc::new(db_conn),
            rpc_client,
        })
    }

    pub fn new(db_conn: Arc<DatabaseConnection>, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            db_conn,
            rpc_client,
        }
    }
}

pub struct OpenApiSpec {
    pub name: String,
    pub request: Option<RefOr<Schema>>,
    pub response: RefOr<Schema>,
}

impl PhotonApi {
    pub async fn liveness(&self) -> Result<(), PhotonApiError> {
        Ok(())
    }

    pub async fn readiness(&self) -> Result<(), PhotonApiError> {
        self.db_conn
            .execute(Statement::from_string(
                self.db_conn.as_ref().get_database_backend(),
                "SELECT 1".to_string(),
            ))
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    pub async fn get_compressed_account(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<AccountResponse, PhotonApiError> {
        get_compressed_account(&self.db_conn, request).await
    }

    pub async fn get_compressed_account_proof(
        &self,
        request: HashRequest,
    ) -> Result<GetCompressedAccountProofResponse, PhotonApiError> {
        get_compressed_account_proof(&self.db_conn, request).await
    }

    pub async fn get_multiple_compressed_account_proofs(
        &self,
        request: HashList,
    ) -> Result<GetMultipleCompressedAccountProofsResponse, PhotonApiError> {
        get_multiple_compressed_account_proofs(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compressed_token_accounts_by_owner(
        &self,
        request: GetCompressedTokenAccountsByOwner,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_token_accounts_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_accounts_by_delegate(
        &self,
        request: GetCompressedTokenAccountsByDelegate,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_account_token_accounts_by_delegate(&self.db_conn, request).await
    }

    pub async fn get_compressed_balance_by_owner(
        &self,
        request: GetCompressedBalanceByOwnerRequest,
    ) -> Result<AccountBalanceResponse, PhotonApiError> {
        get_compressed_balance_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_balances_by_owner(
        &self,
        request: GetCompressedTokenBalancesByOwnerRequest,
    ) -> Result<TokenBalancesResponse, PhotonApiError> {
        get_compressed_token_balances_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_account_balance(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<GetCompressedTokenAccountBalanceResponse, PhotonApiError> {
        get_compressed_token_account_balance(&self.db_conn, request).await
    }

    pub async fn get_compressed_balance(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<AccountBalanceResponse, PhotonApiError> {
        get_compressed_balance(&self.db_conn, request).await
    }

    pub async fn get_indexer_health(&self) -> Result<String, PhotonApiError> {
        get_indexer_health(self.db_conn.as_ref(), self.rpc_client.as_ref()).await
    }

    pub async fn get_indexer_slot(&self) -> Result<UnsignedInteger, PhotonApiError> {
        get_indexer_slot(self.db_conn.as_ref()).await
    }

    pub async fn get_compressed_accounts_by_owner(
        &self,
        request: GetCompressedAccountsByOwnerRequest,
    ) -> Result<GetCompressedAccountsByOwnerResponse, PhotonApiError> {
        get_compressed_accounts_by_owner(self.db_conn.as_ref(), request).await
    }

    pub async fn get_multiple_compressed_accounts(
        &self,
        request: GetMultipleCompressedAccountsRequest,
    ) -> Result<GetMultipleCompressedAccountsResponse, PhotonApiError> {
        get_multiple_compressed_accounts(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_account(
        &self,
        request: HashRequest,
    ) -> Result<GetCompressionSignaturesForAccountResponse, PhotonApiError> {
        get_compression_signatures_for_account(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_address(
        &self,
        request: GetCompressionSignaturesForAddressRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_compression_signatures_for_address(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_owner(
        &self,
        request: GetCompressionSignaturesForOwnerRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_compression_signatures_for_owner(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_token_owner(
        &self,
        request: GetCompressionSignaturesForTokenOwnerRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_compression_signatures_for_token_owner(self.db_conn.as_ref(), request).await
    }

    pub async fn get_transaction_with_compression_info(
        &self,
        request: GetTransactionRequest,
    ) -> Result<GetTransactionResponse, PhotonApiError> {
        get_transaction_with_compression_info(
            &self.db_conn.as_ref(),
            self.rpc_client.as_ref(),
            request,
        )
        .await
    }

    pub async fn get_latest_compression_signatures(
        &self,
        request: GetLatestCompressionSignaturesRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_latest_compression_signatures(self.db_conn.as_ref(), request).await
    }

    pub fn method_api_specs() -> Vec<OpenApiSpec> {
        vec![
            OpenApiSpec {
                name: "getCompressedAccount".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: AccountResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountProof".to_string(),
                request: Some(HashRequest::schema().1),
                response: GetCompressedAccountProofResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleCompressedAccountProofs".to_string(),
                request: Some(HashList::schema().1),
                response: GetMultipleCompressedAccountProofsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountsByOwner".to_string(),
                request: Some(GetCompressedTokenAccountsByOwner::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountsByDelegate".to_string(),
                request: Some(GetCompressedTokenAccountsByDelegate::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountsByOwner".to_string(),
                request: Some(GetCompressedAccountsByOwnerRequest::schema().1),
                response: GetCompressedAccountsByOwnerResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleCompressedAccounts".to_string(),
                request: Some(GetMultipleCompressedAccountsRequest::adjusted_schema()),
                response: GetMultipleCompressedAccountsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountBalance".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: GetCompressedTokenAccountBalanceResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedBalance".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: AccountBalanceResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedBalanceByOwner".to_string(),
                request: Some(GetCompressedBalanceByOwnerRequest::schema().1),
                response: AccountBalanceResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenBalancesByOwner".to_string(),
                request: Some(GetCompressedTokenBalancesByOwnerRequest::schema().1),
                response: TokenBalancesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForAccount".to_string(),
                request: Some(HashRequest::schema().1),
                response: GetCompressionSignaturesForAccountResponse::schema().1,
            },
            OpenApiSpec {
                name: "getTransactionWithCompressionInfo".to_string(),
                request: Some(GetTransactionRequest::schema().1),
                response: GetTransactionResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForAccount".to_string(),
                request: Some(HashRequest::schema().1),
                response: GetCompressionSignaturesForAccountResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForAddress".to_string(),
                request: Some(GetCompressionSignaturesForAddressRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForOwner".to_string(),
                request: Some(GetCompressionSignaturesForOwnerRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForTokenOwner".to_string(),
                request: Some(GetCompressionSignaturesForTokenOwnerRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getLatestCompressionSignatures".to_string(),
                request: Some(GetLatestCompressionSignaturesRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getIndexerHealth".to_string(),
                request: None,
                response: RefOr::T(Schema::Object(
                    ObjectBuilder::new()
                        .schema_type(SchemaType::String)
                        .description(Some("ok if healthy"))
                        .default(Some(serde_json::Value::String("ok".to_string())))
                        .enum_values(Some(vec!["ok".to_string()]))
                        .build(),
                )),
            },
            OpenApiSpec {
                name: "getIndexerSlot".to_string(),
                request: None,
                response: UnsignedInteger::schema().1,
            },
        ]
    }
}

async fn init_pool(
    db_url: &str,
    max_conn: i32,
    timeout_seconds: i32,
) -> Result<DatabaseConnection, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(max_conn as u32)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                conn.execute(format!("SET statement_timeout = '{}s'", timeout_seconds).as_str())
                    .await?;
                Ok(())
            })
        })
        .connect(db_url)
        .await?;
    Ok(SqlxPostgresConnector::from_sqlx_postgres_pool(pool))
}
