pub fn run_server() -> Result<ServerHandle, anyhow::Error> {
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));
    let cors = CorsLayer::new()
        .allow_methods([Method::POST, Method::GET])
        .allow_origin(Any)
        .allow_headers([hyper::header::CONTENT_TYPE]);
    setup_metrics(&config);
    let middleware = tower::ServiceBuilder::new()
        .layer(cors)
        .layer(ProxyGetRequestLayer::new("/liveness", "liveness")?)
        .layer(ProxyGetRequestLayer::new("/readiness", "readiness")?);
    let server = ServerBuilder::default()
        .set_middleware(middleware)
        .set_logger(MetricMiddleware)
        .build(addr)
        .await?;

    // TODO: Load config from Figment.
    let db_url = "postgres://postgres@localhost/postgres";
    let max_conn = 100;
    let timeout_seconds = 15;
    let config = PhotonApiConfig {
        db_url,
        max_conn,
        timeout_seconds,
    };
    let api = PhotonApi::new(config);

    let rpc_module = build_rpc_module(Box::new(api));
    server.start(rpc_module)
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
            let payload = rpc_params.parse::<GetUTXO>()?;
            rpc_context
                .get_compressed_account(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    module.register_async_method(
        "getCompressedAccount",
        |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetUTXO>()?;
            rpc_context
                .get_compressed_account(payload)
                .await
                .map_err(Into::into)
        },
    )?;

    Ok(module)
}
