use std::net::SocketAddr;

use hyper::Method;
use jsonrpsee::{
    server::{middleware::proxy_get_request::ProxyGetRequestLayer, ServerBuilder, ServerHandle},
    RpcModule,
};
use log::debug;
use tower_http::cors::{Any, CorsLayer};

use super::api::PhotonApi;

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
    let rpc_module = build_rpc_module(api)?;
    server.start(rpc_module).map_err(|e| anyhow::anyhow!(e))
}

fn build_rpc_module(api_and_indexer: PhotonApi) -> Result<RpcModule<PhotonApi>, anyhow::Error> {
    let mut module = RpcModule::new(api_and_indexer);

    module.register_async_method("liveness", |_rpc_params, rpc_context| async move {
        debug!("Checking Liveness");
        let api = rpc_context.as_ref();
        api.liveness().await.map_err(Into::into)
    })?;

    module.register_async_method("readiness", |_rpc_params, rpc_context| async move {
        debug!("Checking Readiness");
        let api = rpc_context.as_ref();
        api.readiness().await.map_err(Into::into)
    })?;

    module.register_async_method(
        "getCompressedAccount",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_account(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountProof",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_account_proof(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountProofV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_account_proof_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleCompressedAccountProofs",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_multiple_compressed_account_proofs(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleCompressedAccountProofsV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_multiple_compressed_account_proofs_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountsByOwner",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_token_accounts_by_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountsByDelegate",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_token_accounts_by_delegate(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedBalanceByOwner",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_balance_by_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenBalancesByOwner",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_token_balances_by_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountBalance",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_token_account_balance(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedBalance",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_account_balance(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountBalance",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_account_balance(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method("getIndexerHealth", |_rpc_params, rpc_context| async move {
        rpc_context
            .as_ref()
            .get_indexer_health()
            .await
            .map_err(Into::into)
    })?;

    module.register_async_method("getIndexerSlot", |_rpc_params, rpc_context| async move {
        let api = rpc_context.as_ref();
        api.get_indexer_slot().await.map_err(Into::into)
    })?;

    module.register_async_method("getQueueElements", |rpc_params, rpc_context| async move {
        let api = rpc_context.as_ref();
        let payload = rpc_params.parse()?;
        api.get_queue_elements(payload).await.map_err(Into::into)
    })?;

    module.register_async_method("getQueueElementsV2", |rpc_params, rpc_context| async move {
        let api = rpc_context.as_ref();
        let payload = rpc_params.parse()?;
        api.get_queue_elements_v2(payload).await.map_err(Into::into)
    })?;

    module.register_async_method("getQueueInfo", |rpc_params, rpc_context| async move {
        let api = rpc_context.as_ref();
        let payload = rpc_params.parse()?;
        api.get_queue_info(payload).await.map_err(Into::into)
    })?;

    module.register_async_method(
        "getBatchAddressUpdateInfo",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_batch_address_update_info(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountsByOwner",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_accounts_by_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountsByOwnerV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_accounts_by_owner_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleCompressedAccounts",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_multiple_compressed_accounts(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressionSignaturesForAccount",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compression_signatures_for_account(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressionSignaturesForAddress",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compression_signatures_for_address(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressionSignaturesForOwner",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compression_signatures_for_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressionSignaturesForTokenOwner",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compression_signatures_for_token_owner(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getTransactionWithCompressionInfo",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_transaction_with_compression_info(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method("getValidityProof", |rpc_params, rpc_context| async move {
        let api = rpc_context.as_ref();
        let payload = rpc_params.parse()?;
        api.get_validity_proof(payload).await.map_err(Into::into)
    })?;

    module.register_async_method("getValidityProofV2", |rpc_params, rpc_context| async move {
        let api = rpc_context.as_ref();
        let payload = rpc_params.parse()?;
        api.get_validity_proof_v2(payload).await.map_err(Into::into)
    })?;

    module.register_async_method(
        "getLatestCompressionSignatures",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_latest_compression_signatures(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getLatestNonVotingSignatures",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_latest_non_voting_signatures(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleNewAddressProofs",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_multiple_new_address_proofs(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleNewAddressProofsV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_multiple_new_address_proofs_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;
    module.register_async_method(
        "getCompressedMintTokenHolders",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_mint_token_holders(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenBalancesByOwnerV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_token_balances_by_owner_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccountV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_account_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getMultipleCompressedAccountsV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_multiple_compressed_accounts_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountsByOwnerV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_token_accounts_by_owner_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedTokenAccountsByDelegateV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_compressed_token_accounts_by_delegate_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getTransactionWithCompressionInfoV2",
        |rpc_params, rpc_context| async move {
            let api = rpc_context.as_ref();
            let payload = rpc_params.parse()?;
            api.get_transaction_with_compression_info_v2(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    Ok(module)
}
