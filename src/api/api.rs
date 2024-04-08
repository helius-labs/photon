use std::sync::Arc;

use crate::api::method::utils::slot_schema;
use sea_orm::{ConnectionTrait, DatabaseConnection, SqlxPostgresConnector, Statement};
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::{postgres::PgPoolOptions, Executor};
use utoipa::openapi::{ObjectBuilder, RefOr, Schema, SchemaType};
use utoipa::ToSchema;

use super::method::get_compressed_account::AccountResponse;
use super::{
    error::PhotonApiError,
    method::{
        get_compressed_account::get_compressed_account,
        get_compressed_account_proof::{
            get_compressed_account_proof, GetCompressedAccountProofResponse, HashRequest,
        },
        get_compressed_accounts_by_owner::{
            get_compressed_accounts_by_owner, GetCompressedAccountsByOwnerRequest,
            GetCompressedAccountsByOwnerResponse,
        },
        get_compressed_balance::{get_compressed_balance, GetCompressedAccountBalance},
        get_compressed_token_account_balance::{
            get_compressed_token_account_balance, GetCompressedTokenAccountBalanceResponse,
        },
        get_compressed_token_accounts_by_delegate::get_compressed_account_token_accounts_by_delegate,
        get_compressed_token_accounts_by_owner::get_compressed_token_accounts_by_owner,
        get_health::get_health,
        get_multiple_compressed_account_proofs::{
            get_multiple_compressed_account_proofs, GetMultipleCompressedAccountProofsResponse,
            HashList,
        },
        get_multiple_compressed_accounts::{
            get_multiple_compressed_accounts, GetMultipleCompressedAccountsRequest,
            GetMultipleCompressedAccountsResponse,
        },
        get_slot::get_slot,
        utils::{
            CompressedAccountRequest, GetCompressedTokenAccountsByAuthority,
            TokenAccountListResponse,
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
        request: GetCompressedTokenAccountsByAuthority,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_token_accounts_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_accounts_by_delegate(
        &self,
        request: GetCompressedTokenAccountsByAuthority,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_account_token_accounts_by_delegate(&self.db_conn, request).await
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
    ) -> Result<GetCompressedAccountBalance, PhotonApiError> {
        get_compressed_balance(&self.db_conn, request).await
    }

    pub async fn get_health(&self) -> Result<String, PhotonApiError> {
        get_health(self.db_conn.as_ref(), self.rpc_client.as_ref()).await
    }

    pub async fn get_slot(&self) -> Result<u64, PhotonApiError> {
        get_slot(self.db_conn.as_ref()).await
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

    // HACK: This could be easily implemented through a macro. Using ChatGPT instead for speed.
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
                request: Some(GetCompressedTokenAccountsByAuthority::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountsByDelegate".to_string(),
                request: Some(GetCompressedTokenAccountsByAuthority::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountBalance".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: GetCompressedTokenAccountBalanceResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedBalance".to_string(),
                request: Some(CompressedAccountRequest::schema().1),
                response: GetCompressedAccountBalance::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountsByOwner".to_string(),
                request: Some(GetCompressedAccountsByOwnerRequest::schema().1),
                response: GetCompressedAccountsByOwnerResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleCompressedAccounts".to_string(),
                request: Some(GetMultipleCompressedAccountsRequest::schema().1),
                response: GetMultipleCompressedAccountsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getHealth".to_string(),
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
                name: "getSlot".to_string(),
                request: None,
                response: RefOr::T(slot_schema()),
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
