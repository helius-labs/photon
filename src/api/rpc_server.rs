use std::net::SocketAddr;

use hyper::Method;
use jsonrpsee::{
    server::{middleware::proxy_get_request::ProxyGetRequestLayer, ServerBuilder, ServerHandle},
    RpcModule,
};
use log::debug;
use tower_http::cors::{Any, CorsLayer};

use super::method::{
    get_compressed_account_proof::GetCompressedAccountProofRequest,
    get_compressed_token_accounts_by_delegate::GetCompressedTokenAccountsByDelegateRequest,
    get_compressed_token_accounts_by_owner::GetCompressedTokenAccountsByOwnerRequest,
    get_utxo_proof::GetUtxoProofRequest, get_utxos::GetUtxosRequest,
    utils::GetCompressedAccountRequest,
};
use super::{
    api::{ApiContract, PhotonApi},
    method::get_utxo::GetUtxoRequest,
};

pub async fn run_server(api: PhotonApi, port: u16) -> Result<ServerHandle, anyhow::Error> {
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

    module.register_async_method("getUtxo", |rpc_params, rpc_context| async move {
        let payload = rpc_params.parse::<GetUtxoRequest>()?;
        rpc_context.get_utxo(payload).await.map_err(Into::into)
    })?;

    module.register_async_method("getUtxos", |rpc_params, rpc_context| async move {
        let payload = rpc_params.parse::<GetUtxosRequest>()?;
        rpc_context.get_utxos(payload).await.map_err(Into::into)
    })?;

    module.register_async_method("getUtxoProof", |rpc_params, rpc_context| async move {
        let payload = rpc_params.parse::<GetUtxoProofRequest>()?;
        rpc_context
            .get_utxo_proof(payload)
            .await
            .map_err(Into::into)
    })?;

    module.register_async_method(
        "getCompressedTokenAccountsByOwner",
        |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetCompressedTokenAccountsByOwnerRequest>()?;
            rpc_context
                .get_compressed_token_accounts_by_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountsByDelegate",
        |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetCompressedTokenAccountsByDelegateRequest>()?;
            rpc_context
                .get_compressed_token_accounts_by_delegate(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountBalance",
        |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetCompressedAccountRequest>()?;
            rpc_context
                .get_compressed_token_account_balance(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedBalance",
        |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetCompressedAccountRequest>()?;
            rpc_context
                .get_compressed_balance(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method("getHealth", |_rpc_params, rpc_context| async move {
        rpc_context.get_health().await.map_err(Into::into)
    })?;

    module.register_async_method("getSlot", |_rpc_params, rpc_context| async move {
        rpc_context.get_slot().await.map_err(Into::into)
    })?;

    Ok(module)
}
