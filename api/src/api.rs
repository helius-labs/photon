#[document_rpc]
#[async_trait]
pub trait ApiContract: Send + Sync + 'static {
    #[rpc(name = "liveness", summary = "Aliveness check for the API")]
    async fn liveness(&self) -> Result<(), PublicApiError>;

    #[rpc(
        name = "readiness",
        summary = "Readiness check for the API. Requires an available DB connection."
    )]
    async fn readiness(&self) -> Result<(), PublicApiError>;

    #[rpc(
        name = "getCompressedAccount",
        params = "named",
        summary = "Get a compressed account (UTXO)"
    )]
    async fn get_compressed_account(
        &self,
        payload: GetCompressedAcccountRequest,
    ) -> Result<GetCompressedAcccountResponse, PublicApiError>;

    #[rpc(
        name = "getCompressedAccountProof",
        params = "named",
        summary = "Get the merkle proof for a compressed account (UTXO)"
    )]
    async fn get_compressed_account_proof(
        &self,
        payload: GetCompressedAcccountProofRequest,
    ) -> Result<GetCompressedAcccountProofResponse, PublicApiError>;
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
    pub async fn new(config: PhotonApiConfig) {
        let db_conn = init_pool(db_url, max_conn, timeout_seconds);
        PhotonApi { db_conn }
    }
}

#[document_rpc]
#[async_trait]
impl ApiContract for PhotonApi {
    async fn liveness() -> Result<(), PublicApiError> {
        Ok(())
    }

    async fn readiness(&self) -> Result<(), PublicApiError> {
        todo!();
    }

    async fn get_compressed_account(
        &self,
        request: GetCompressedAcccountRequest,
    ) -> Result<GetCompressedAcccountResponse, PublicApiError> {
        get_compressed_account(request)
    }

    async fn get_compressed_account_proof(
        &self,
        request: GetCompressedAcccountProofRequest,
    ) -> Result<GetCompressedAcccountProofResponse, PublicApiError> {
        get_compressed_account(request)
    }
}

async fn init_pool(
    db_url: &str,
    max_conn: i32,
    timeout_seconds: i32,
) -> Result<DatabaseConnection, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(max_conn.unwrap_or(100))
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                conn.execute(format!("SET statement_timeout = '{}s'", timeout_seconds))
                    .await?;
                Ok(())
            })
        })
        .connect(db_url)
        .await?;
    SqlxPostgresConnector::from_sqlx_postgres_pool(pool)
}
