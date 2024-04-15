use std::{net::SocketAddr, sync::Arc};

use hyper::Method;
use jsonrpsee::{
    server::{middleware::proxy_get_request::ProxyGetRequestLayer, ServerBuilder, ServerHandle},
    RpcModule,
};
use log::debug;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use crate::ingester::indexer::Indexer;

use super::api::PhotonApi;

pub async fn run_server(
    api: PhotonApi,
    port: u16,
    indexer: Option<Arc<Mutex<Indexer>>>,
) -> Result<ServerHandle, anyhow::Error> {
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
    let rpc_module = build_rpc_module(ApiAndIndexer { api, indexer })?;
    server.start(rpc_module).map_err(|e| anyhow::anyhow!(e))
}

struct ApiAndIndexer {
    api: PhotonApi,
    indexer: Option<Arc<Mutex<Indexer>>>,
}

async fn conditionally_index_latest_blocks(indexer: &Option<Arc<Mutex<Indexer>>>) {
    if let Some(indexer) = indexer {
        indexer.lock().await.index_latest_blocks(None).await;
    }
}

fn build_rpc_module(
    api_and_indexer: ApiAndIndexer,
) -> Result<RpcModule<ApiAndIndexer>, anyhow::Error> {
    let mut module = RpcModule::new(api_and_indexer);

    module.register_async_method("liveness", |_rpc_params, rpc_context| async move {
        debug!("Checking Liveness");
        let ApiAndIndexer { api, .. } = rpc_context.as_ref();
        api.liveness().await.map_err(Into::into)
    })?;

    module.register_async_method("readiness", |_rpc_params, rpc_context| async move {
        debug!("Checking Readiness");
        let ApiAndIndexer { api, .. } = rpc_context.as_ref();
        api.readiness().await.map_err(Into::into)
    })?;

    module.register_async_method(
        "getCompressedAccount",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_compressed_account(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountProof",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_compressed_account_proof(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleCompressedAccountProofs",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_multiple_compressed_account_proofs(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountsByOwner",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_compressed_token_accounts_by_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountsByDelegate",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_compressed_token_accounts_by_delegate(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountBalance",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_compressed_token_account_balance(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedBalance",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_compressed_balance(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method("getHealth", |_rpc_params, rpc_context| async move {
        rpc_context.api.get_health().await.map_err(Into::into)
    })?;

    module.register_async_method("getSlot", |_rpc_params, rpc_context| async move {
        let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
        conditionally_index_latest_blocks(indexer).await;
        api.get_slot().await.map_err(Into::into)
    })?;

    module.register_async_method(
        "getCompressedAccountsByOwner",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_compressed_accounts_by_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleCompressedAccounts",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_multiple_compressed_accounts(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getSignaturesForCompressedAccount",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_signatures_for_compressed_account(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getSignaturesForAddress",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_signatures_for_address(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getSignaturesForOwner",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_signatures_for_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getSignaturesForTokenOwner",
        |rpc_params, rpc_context| async move {
            let ApiAndIndexer { api, indexer } = rpc_context.as_ref();
            conditionally_index_latest_blocks(indexer).await;
            let payload = rpc_params.parse()?;
            api.get_signatures_for_token_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    Ok(module)
}
