use std::sync::Arc;

use async_trait::async_trait;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, SqlxPostgresConnector, Statement,
};
use sqlx::{postgres::PgPoolOptions, Executor};

use super::{
    error::PhotonApiError,
    method::{
        get_compressed_account::{get_compressed_account, GetCompressedAccountRequest},
        get_compressed_account_proof::{
            get_compressed_account_proof, GetCompressedAccountProofRequest,
            GetCompressedAccountProofResponse,
        },
        get_compressed_token_accounts_by_owner::{
            get_compressed_account_token_accounts_by_owner, GetCompressedTokenInfoByOwnerRequest,
            GetCompressedTokenInfoByOwnerResponse,
        },
        get_utxo::{get_utxo, GetUtxoRequest},
        get_utxo_proof::{get_utxo_proof, GetUtxoProofRequest},
        get_utxos::{get_utxos, GetUtxosRequest, GetUtxosResponse},
        utils::{ProofResponse, Utxo},
    },
};

#[async_trait]
pub trait ApiContract: Send + Sync + 'static {
    async fn liveness(&self) -> Result<(), PhotonApiError>;

    async fn readiness(&self) -> Result<(), PhotonApiError>;

    async fn get_compressed_account(
        &self,
        payload: GetCompressedAccountRequest,
    ) -> Result<Utxo, PhotonApiError>;

    async fn get_compressed_account_proof(
        &self,
        payload: GetCompressedAccountProofRequest,
    ) -> Result<GetCompressedAccountProofResponse, PhotonApiError>;

    async fn get_compressed_account_token_accounts_by_owner(
        &self,
        payload: GetCompressedTokenInfoByOwnerRequest,
    ) -> Result<GetCompressedTokenInfoByOwnerResponse, PhotonApiError>;

    async fn get_utxos(&self, payload: GetUtxosRequest)
        -> Result<GetUtxosResponse, PhotonApiError>;

    async fn get_utxo(&self, request: GetUtxoRequest) -> Result<Utxo, PhotonApiError>;

    async fn get_utxo_proof(
        &self,
        payload: GetUtxoProofRequest,
    ) -> Result<ProofResponse, PhotonApiError>;
}

pub struct PhotonApiConfig {
    pub db_url: String,
    pub max_conn: i32,
    pub timeout_seconds: i32,
}

pub struct PhotonApi {
    db_conn: Arc<DatabaseConnection>,
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
        Ok(Self {
            db_conn: Arc::new(db_conn),
        })
    }
}

impl From<Arc<DatabaseConnection>> for PhotonApi {
    fn from(db_conn: Arc<DatabaseConnection>) -> Self {
        Self { db_conn }
    }
}

#[async_trait]
impl ApiContract for PhotonApi {
    async fn liveness(&self) -> Result<(), PhotonApiError> {
        Ok(())
    }

    async fn readiness(&self) -> Result<(), PhotonApiError> {
        self.db_conn
            .execute(Statement::from_string(
                self.db_conn.as_ref().get_database_backend(),
                "SELECT 1".to_string(),
            ))
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn get_compressed_account(
        &self,
        request: GetCompressedAccountRequest,
    ) -> Result<Utxo, PhotonApiError> {
        get_compressed_account(&self.db_conn, request).await
    }

    async fn get_compressed_account_proof(
        &self,
        request: GetCompressedAccountProofRequest,
    ) -> Result<GetCompressedAccountProofResponse, PhotonApiError> {
        get_compressed_account_proof(&self.db_conn, request).await
    }

    async fn get_compressed_account_token_accounts_by_owner(
        &self,
        request: GetCompressedTokenInfoByOwnerRequest,
    ) -> Result<GetCompressedTokenInfoByOwnerResponse, PhotonApiError> {
        get_compressed_account_token_accounts_by_owner(&self.db_conn, request).await
    }

    async fn get_utxos(
        &self,
        request: GetUtxosRequest,
    ) -> Result<GetUtxosResponse, PhotonApiError> {
        get_utxos(&self.db_conn, request).await
    }

    async fn get_utxo(&self, request: GetUtxoRequest) -> Result<Utxo, PhotonApiError> {
        get_utxo(&self.db_conn, request).await
    }

    async fn get_utxo_proof(
        &self,
        request: GetUtxoProofRequest,
    ) -> Result<ProofResponse, PhotonApiError> {
        get_utxo_proof(&self.db_conn, request).await
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
