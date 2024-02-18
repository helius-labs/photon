use std::env;

use log::{error, info};

use crate::rpc_server::run_server;

pub mod api;
pub mod error;
pub mod method;
mod rpc_server;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let env = env::var("ENV").unwrap_or("local".to_string());
    let env_filter = env::var("RUST_LOG")
        .unwrap_or("info,sqlx::query=warn,jsonrpsee_server::server=warn".to_string());
    let t = tracing_subscriber::fmt().with_env_filter(env_filter);
    if env.eq("local") {
        t.pretty().init();
    } else {
        t.json().init();
    }

    let server_handle: jsonrpsee::server::ServerHandle = run_server().await?;
    info!("Server started");

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutting down server");
            server_handle.stop()?;
        }
        Err(err) => {
            error!("Failed to shutdown server: {}", err);
        }
    }
    tokio::spawn(server_handle.stopped());
    info!("Server shutdown successfully");
    Ok(())
}
