use async_trait::async_trait;
use sea_orm::{DatabaseConnection, SqlxPostgresConnector};
use sqlx::{postgres::PgPoolOptions, Executor};

use crate::{
    error::PhotonApiError,
    method::{
        get_compressed_account::{
            get_compressed_account, GetCompressedAccountRequest, GetCompressedAccountResponse,
        },
        get_compressed_account_proof::{
            get_compressed_account_proof, GetCompressedAccountProofRequest,
            GetCompressedAccountProofResponse,
        },
        get_compressed_token_accounts_by_owner::{
            get_compressed_account_token_accounts_by_owner, GetCompressedTokenInfoByOwnerRequest,
            GetCompressedTokenInfoByOwnerResponse,
        },
    },
};

#[async_trait]
pub trait ApiContract: Send + Sync + 'static {
    async fn liveness(&self) -> Result<(), PhotonApiError>;

    async fn readiness(&self) -> Result<(), PhotonApiError>;

    async fn get_compressed_account(
        &self,
        payload: GetCompressedAccountRequest,
    ) -> Result<Option<GetCompressedAccountResponse>, PhotonApiError>;

    async fn get_compressed_account_proof(
        &self,
        payload: GetCompressedAccountProofRequest,
    ) -> Result<Option<GetCompressedAccountProofResponse>, PhotonApiError>;

    async fn get_compressed_account_token_accounts_by_owner(
        &self,
        payload: GetCompressedTokenInfoByOwnerRequest,
    ) -> Result<GetCompressedTokenInfoByOwnerResponse, PhotonApiError>;
}

pub struct PhotonApiConfig {
    pub db_url: String,
    pub max_conn: i32,
    pub timeout_seconds: i32,
}

pub struct PhotonApi {
    db_conn: DatabaseConnection,
}

impl PhotonApi {
    pub async fn new(config: PhotonApiConfig) -> Result<Self, anyhow::Error> {
        let PhotonApiConfig {
            db_url,
            max_conn,
            timeout_seconds,
            ..
        } = config;
        let db_conn = init_pool(&db_url, max_conn, timeout_seconds).await?;
        Ok(Self { db_conn })
    }
}

#[async_trait]
impl ApiContract for PhotonApi {
    async fn liveness(&self) -> Result<(), PhotonApiError> {
        Ok(())
    }

    async fn readiness(&self) -> Result<(), PhotonApiError> {
        todo!();
    }

    async fn get_compressed_account(
        &self,
        request: GetCompressedAccountRequest,
    ) -> Result<Option<GetCompressedAccountResponse>, PhotonApiError> {
        get_compressed_account(&self.db_conn, request).await
    }

    async fn get_compressed_account_proof(
        &self,
        request: GetCompressedAccountProofRequest,
    ) -> Result<Option<GetCompressedAccountProofResponse>, PhotonApiError> {
        get_compressed_account_proof(&self.db_conn, request).await
    }

    async fn get_compressed_account_token_accounts_by_owner(
        &self,
        request: GetCompressedTokenInfoByOwnerRequest,
    ) -> Result<GetCompressedTokenInfoByOwnerResponse, PhotonApiError> {
        get_compressed_account_token_accounts_by_owner(&self.db_conn, request).await
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
