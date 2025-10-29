use light_compressed_account::QueueType::{InputStateV2, OutputStateV2};
use light_compressed_account::TreeType::AddressV2;
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
                    let update = match event {
                        IngestionEvent::AddressQueueInsert {
                            tree,
                            queue,
                            count,
                            slot,
                        } => QueueUpdate {
                            queue_info: Some(QueueInfo {
                                tree: tree.to_string(),
                                queue: queue.to_string(),
                                queue_type: AddressV2 as u32,
                                queue_size: count as u64,
                            }),
                            slot,
                            update_type: UpdateType::ItemAdded as i32,
                        },

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

                    let _ = self.update_sender.send(update);
                }
                None => {
                    tracing::info!("Event channel closed, GrpcEventSubscriber shutting down");
                    break;
                }
            }
        }
    }
}
