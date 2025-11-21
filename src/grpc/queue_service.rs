use std::pin::Pin;
use std::sync::Arc;

use sea_orm::DatabaseConnection;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::api::method::get_queue_info;

use super::proto::{
    queue_service_server::QueueService, GetQueueInfoRequest, GetQueueInfoResponse, QueueInfo,
    QueueUpdate, SubscribeQueueUpdatesRequest, UpdateType,
};

pub struct PhotonQueueService {
    db: Arc<DatabaseConnection>,
    update_sender: broadcast::Sender<QueueUpdate>,
}

impl PhotonQueueService {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        let (update_sender, _) = broadcast::channel(1000);
        Self { db, update_sender }
    }

    pub fn get_update_sender(&self) -> broadcast::Sender<QueueUpdate> {
        self.update_sender.clone()
    }
}

#[tonic::async_trait]
impl QueueService for PhotonQueueService {
    async fn get_queue_info(
        &self,
        request: Request<GetQueueInfoRequest>,
    ) -> Result<Response<GetQueueInfoResponse>, Status> {
        let req = request.into_inner();

        let api_request = crate::api::method::get_queue_info::GetQueueInfoRequest {
            trees: if req.trees.is_empty() {
                None
            } else {
                Some(req.trees)
            },
        };

        let api_response = get_queue_info::get_queue_info(self.db.as_ref(), api_request)
            .await
            .map_err(|e| Status::internal(format!("Failed to get queue info: {}", e)))?;

        let queues = api_response
            .queues
            .into_iter()
            .map(|q| QueueInfo {
                tree: q.tree,
                queue: q.queue,
                queue_type: q.queue_type as u32,
                queue_size: q.queue_size,
            })
            .collect();

        Ok(Response::new(GetQueueInfoResponse {
            queues,
            slot: api_response.slot,
        }))
    }

    type SubscribeQueueUpdatesStream =
        Pin<Box<dyn Stream<Item = Result<QueueUpdate, Status>> + Send>>;

    async fn subscribe_queue_updates(
        &self,
        request: Request<SubscribeQueueUpdatesRequest>,
    ) -> Result<Response<Self::SubscribeQueueUpdatesStream>, Status> {
        let req = request.into_inner();
        let mut rx = self.update_sender.subscribe();

        let initial_updates = if req.send_initial_state {
            let api_request = crate::api::method::get_queue_info::GetQueueInfoRequest {
                trees: if req.trees.is_empty() {
                    None
                } else {
                    Some(req.trees.clone())
                },
            };

            let api_response = get_queue_info::get_queue_info(self.db.as_ref(), api_request)
                .await
                .map_err(|e| {
                    Status::internal(format!("Failed to get initial queue info: {}", e))
                })?;

            api_response
                .queues
                .into_iter()
                .map(|q| QueueUpdate {
                    queue_info: Some(QueueInfo {
                        tree: q.tree,
                        queue: q.queue,
                        queue_type: q.queue_type as u32,
                        queue_size: q.queue_size,
                    }),
                    slot: api_response.slot,
                    update_type: UpdateType::Initial as i32,
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let trees_filter = if req.trees.is_empty() {
            None
        } else {
            Some(req.trees)
        };

        let stream = async_stream::stream! {
            for update in initial_updates {
                yield Ok(update);
            }

            while let Ok(update) = rx.recv().await {
                if let Some(ref trees) = trees_filter {
                    if let Some(ref queue_info) = update.queue_info {
                        if !trees.contains(&queue_info.tree) {
                            continue;
                        }
                    }
                }
                yield Ok(update);
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}
