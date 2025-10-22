use std::net::SocketAddr;
use std::sync::Arc;

use sea_orm::DatabaseConnection;
use tonic::transport::Server;

use super::proto::queue_service_server::QueueServiceServer;
use super::proto::FILE_DESCRIPTOR_SET;
use super::queue_monitor::QueueMonitor;
use super::queue_service::PhotonQueueService;

pub async fn run_grpc_server(
    db: Arc<DatabaseConnection>,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let service = PhotonQueueService::new(db.clone());

    let update_sender = service.get_update_sender();

    let monitor = QueueMonitor::new(db, update_sender, 1000);
    tokio::spawn(async move {
        monitor.start().await;
    });

    // Set up reflection service
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    tracing::info!("Starting gRPC server on {}", addr);
    tracing::info!("Queue monitor started (polling every 1s)");

    Server::builder()
        .add_service(QueueServiceServer::new(service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
