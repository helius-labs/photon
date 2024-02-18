use std::net::SocketAddr;

use hyper::Method;
use jsonrpsee::{
    server::{middleware::proxy_get_request::ProxyGetRequestLayer, ServerBuilder, ServerHandle},
    RpcModule,
};
use log::debug;
use tower_http::cors::{Any, CorsLayer};

use crate::{
    api::{ApiContract, PhotonApi, PhotonApiConfig},
    method::{
        get_compressed_account::GetCompressedAccountRequest,
        get_compressed_account_proof::GetCompressedAccountProofRequest,
    },
};

pub async fn run_server() -> Result<ServerHandle, anyhow::Error> {
    // TODO: Load config from Figment.
    let db_url = "postgres://postgres@localhost/postgres".to_string();
    let max_conn = 100;
    let timeout_seconds = 15;
    let port = 9090;
    let config = PhotonApiConfig {
        db_url,
        max_conn,
        timeout_seconds,
    };

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let cors = CorsLayer::new()
        .allow_methods([Method::POST, Method::GET])
        .allow_origin(Any)
        .allow_headers([hyper::header::CONTENT_TYPE]);
    let middleware = tower::ServiceBuilder::new()
        .layer(cors)
        .layer(ProxyGetRequestLayer::new("/liveness", "liveness")?)
        .layer(ProxyGetRequestLayer::new("/readiness", "readiness")?);
    let server = ServerBuilder::default()
        .set_middleware(middleware)
        .build(addr)
        .await?;
    let api = PhotonApi::new(config).await?;
    let rpc_module = build_rpc_module(Box::new(api))?;
    server.start(rpc_module).map_err(|e| anyhow::anyhow!(e))
}

pub fn build_rpc_module(
    contract: Box<dyn ApiContract>,
) -> Result<RpcModule<Box<dyn ApiContract>>, anyhow::Error> {
    let mut module = RpcModule::new(contract);

    module.register_async_method("liveness", |_rpc_params, rpc_context| async move {
        debug!("Checking Liveness");
        rpc_context.liveness().await.map_err(Into::into)
    })?;

    module.register_async_method("readiness", |_rpc_params, rpc_context| async move {
        debug!("Checking Readiness");
        rpc_context.readiness().await.map_err(Into::into)
    })?;

    module.register_async_method(
        "getCompressedAccount",
        |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetCompressedAccountRequest>()?;
            rpc_context
                .get_compressed_account(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountProof",
        |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetCompressedAccountProofRequest>()?;
            rpc_context
                .get_compressed_account_proof(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    Ok(module)
}
