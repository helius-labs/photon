use std::net::SocketAddr;
use std::sync::Arc;

use sea_orm::DatabaseConnection;
use tonic::transport::Server;

use super::event_subscriber::GrpcEventSubscriber;
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

    let event_receiver = crate::events::init_event_bus();
    let event_subscriber = GrpcEventSubscriber::new(event_receiver, update_sender.clone());
    tokio::spawn(async move {
        event_subscriber.start().await;
    });
    tracing::info!("Event-driven queue updates enabled");

    // Keep QueueMonitor as backup with 5s polling
    let monitor = QueueMonitor::new(db, update_sender, 5000);
    tokio::spawn(async move {
        monitor.start().await;
    });

    // Set up reflection service
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    tracing::info!("Starting gRPC server on {}", addr);
    tracing::info!("Queue monitor started as backup (polling every 5s)");

    Server::builder()
        .add_service(QueueServiceServer::new(service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
