use std::sync::Arc;

use sea_orm::{ConnectionTrait, DatabaseConnection, SqlxPostgresConnector, Statement};
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::{postgres::PgPoolOptions, Executor};

use super::{
    error::PhotonApiError,
    method::{
        get_compressed_account::get_compressed_account,
        get_compressed_account_proof::{
            get_compressed_account_proof, GetCompressedAccountProofResponse,
        },
        get_compressed_balance::{get_compressed_balance, GetCompressedAccountBalance},
        get_compressed_token_account_balance::{
            get_compressed_token_account_balance, GetCompressedTokenAccountBalanceResponse,
        },
        get_compressed_token_accounts_by_delegate::{
            get_compressed_account_token_accounts_by_delegate,
            GetCompressedTokenAccountsByDelegateRequest,
        },
        get_compressed_token_accounts_by_owner::{
            get_compressed_token_accounts_by_owner, GetCompressedTokenAccountsByOwnerRequest,
        },
        get_health::get_health,
        get_slot::get_slot,
        get_utxo::{get_utxo, GetUtxoRequest},
        get_utxo_proof::{get_utxo_proof, GetUtxoProofRequest},
        get_utxos::{get_utxos, GetUtxosRequest, GetUtxosResponse},
        utils::{
            CompressedAccountRequest, GetCompressedAccountRequest, ProofResponse, TokenAccountList,
            TokenAccountListResponse, Utxo, UtxoResponse,
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

impl PhotonApi {
    pub async fn get_methods() -> Result<(), PhotonApiError> {
        Ok(())
    }

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
        request: GetCompressedAccountRequest,
    ) -> Result<UtxoResponse, PhotonApiError> {
        get_compressed_account(&self.db_conn, request).await
    }

    pub async fn get_compressed_account_proof(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<GetCompressedAccountProofResponse, PhotonApiError> {
        get_compressed_account_proof(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_accounts_by_owner(
        &self,
        request: GetCompressedTokenAccountsByOwnerRequest,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_token_accounts_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_accounts_by_delegate(
        &self,
        request: GetCompressedTokenAccountsByDelegateRequest,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_account_token_accounts_by_delegate(&self.db_conn, request).await
    }

    pub async fn get_utxos(
        &self,
        request: GetUtxosRequest,
    ) -> Result<GetUtxosResponse, PhotonApiError> {
        get_utxos(&self.db_conn, request).await
    }

    pub async fn get_utxo(&self, request: GetUtxoRequest) -> Result<Utxo, PhotonApiError> {
        get_utxo(&self.db_conn, request).await
    }

    pub async fn get_utxo_proof(
        &self,
        request: GetUtxoProofRequest,
    ) -> Result<ProofResponse, PhotonApiError> {
        get_utxo_proof(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_account_balance(
        &self,
        request: GetCompressedAccountRequest,
    ) -> Result<GetCompressedTokenAccountBalanceResponse, PhotonApiError> {
        get_compressed_token_account_balance(&self.db_conn, request).await
    }

    pub async fn get_compressed_balance(
        &self,
        request: GetCompressedAccountRequest,
    ) -> Result<GetCompressedAccountBalance, PhotonApiError> {
        get_compressed_balance(&self.db_conn, request).await
    }

    pub async fn get_health(&self) -> Result<(), PhotonApiError> {
        get_health(self.db_conn.as_ref(), self.rpc_client.as_ref()).await
    }

    pub async fn get_slot(&self) -> Result<u64, PhotonApiError> {
        get_slot(self.db_conn.as_ref()).await
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
