use cadence_macros::statsd_count;
use light_compressed_account::QueueType::{self, InputStateV2, OutputStateV2};
use tokio::sync::broadcast;

use crate::events::{EventSubscriber, IngestionEvent};

use super::proto::{QueueInfo, QueueUpdate, UpdateType};

pub struct GrpcEventSubscriber {
    event_receiver: EventSubscriber,
    update_sender: broadcast::Sender<QueueUpdate>,
}

impl GrpcEventSubscriber {
    pub fn new(
        event_receiver: EventSubscriber,
        update_sender: broadcast::Sender<QueueUpdate>,
    ) -> Self {
        Self {
            event_receiver,
            update_sender,
        }
    }

    pub async fn start(mut self) {
        loop {
            match self.event_receiver.recv().await {
                Some(event) => {
                    tracing::info!("GrpcEventSubscriber received event: {:?}", event);
                    let update = match event {
                        IngestionEvent::AddressQueueInsert {
                            tree,
                            queue,
                            count,
                            slot,
                        } => {
                            tracing::info!(
                                "Creating QueueUpdate for AddressQueueInsert: tree={}, queue_type={}",
                                tree,
                                QueueType::AddressV2 as u32
                            );
                            QueueUpdate {
                                queue_info: Some(QueueInfo {
                                    tree: tree.to_string(),
                                    queue: queue.to_string(),
                                    queue_type: QueueType::AddressV2 as u32,
                                    queue_size: count as u64,
                                }),
                                slot,
                                update_type: UpdateType::ItemAdded as i32,
                            }
                        }

                        IngestionEvent::OutputQueueInsert {
                            tree,
                            queue,
                            count,
                            slot,
                        } => QueueUpdate {
                            queue_info: Some(QueueInfo {
                                tree: tree.to_string(),
                                queue: queue.to_string(),
                                queue_type: OutputStateV2 as u32,
                                queue_size: count as u64,
                            }),
                            slot,
                            update_type: UpdateType::ItemAdded as i32,
                        },

                        IngestionEvent::NullifierQueueInsert {
                            tree,
                            queue,
                            count,
                            slot,
                        } => QueueUpdate {
                            queue_info: Some(QueueInfo {
                                tree: tree.to_string(),
                                queue: queue.to_string(),
                                queue_type: InputStateV2 as u32,
                                queue_size: count as u64,
                            }),
                            slot,
                            update_type: UpdateType::ItemAdded as i32,
                        },
                    };

                    if let Err(e) = self.update_sender.send(update.clone()) {
                        tracing::warn!(
                            "Failed to send gRPC queue update to broadcast channel: {} (likely no active subscribers)",
                            e
                        );
                        crate::metric! {
                            statsd_count!("grpc.event_subscriber.broadcast_failed", 1);
                        }
                    } else {
                        tracing::info!(
                            "Successfully broadcasted gRPC queue update: tree={}, queue_type={}, queue_size={}",
                            update.queue_info.as_ref().map(|qi| qi.tree.as_str()).unwrap_or("unknown"),
                            update.queue_info.as_ref().map(|qi| qi.queue_type).unwrap_or(0),
                            update.queue_info.as_ref().map(|qi| qi.queue_size).unwrap_or(0)
                        );
                        crate::metric! {
                            statsd_count!("grpc.event_subscriber.broadcast_success", 1);
                        }
                    }
                }
                None => {
                    tracing::info!("Event channel closed, GrpcEventSubscriber shutting down");
                    break;
                }
            }
        }
    }
}
