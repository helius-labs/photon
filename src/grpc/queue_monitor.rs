use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sea_orm::DatabaseConnection;
use tokio::sync::broadcast;
use tokio::time;

use crate::api::method::get_queue_info;

use super::proto::{QueueInfo, QueueUpdate, UpdateType};

const HEARTBEAT_INTERVAL_SECS: u64 = 30;

pub struct QueueMonitor {
    db: Arc<DatabaseConnection>,
    update_sender: broadcast::Sender<QueueUpdate>,
    poll_interval: Duration,
}

impl QueueMonitor {
    pub fn new(
        db: Arc<DatabaseConnection>,
        update_sender: broadcast::Sender<QueueUpdate>,
        poll_interval_ms: u64,
    ) -> Self {
        Self {
            db,
            update_sender,
            poll_interval: Duration::from_millis(poll_interval_ms),
        }
    }

    pub async fn start(self) {
        let mut interval = time::interval(self.poll_interval);
        let mut previous_state: HashMap<(String, u8), u64> = HashMap::new();
        let mut last_update_time: HashMap<(String, u8), Instant> = HashMap::new();

        loop {
            interval.tick().await;

            let request = get_queue_info::GetQueueInfoRequest { trees: None };

            match get_queue_info::get_queue_info(self.db.as_ref(), request).await {
                Ok(response) => {
                    let mut current_state = HashMap::new();
                    let now = Instant::now();

                    for queue in response.queues {
                        let key = (queue.tree.clone(), queue.queue_type);
                        let previous_size = previous_state.get(&key).copied().unwrap_or(0);
                        let last_update = last_update_time.get(&key).copied();

                        current_state.insert(key.clone(), queue.queue_size);

                        // Send update if:
                        // 1. Queue size changed, OR
                        // 2. Queue is non-empty AND 30+ seconds since last update (heartbeat)
                        let should_send = queue.queue_size != previous_size
                            || (queue.queue_size > 0
                                && last_update.map_or(true, |t| {
                                    now.duration_since(t).as_secs() >= HEARTBEAT_INTERVAL_SECS
                                }));

                        if should_send {
                            let update_type = if queue.queue_size > previous_size {
                                UpdateType::ItemAdded
                            } else if queue.queue_size < previous_size {
                                UpdateType::ItemRemoved
                            } else {
                                // Heartbeat for unchanged non-empty queue
                                UpdateType::ItemAdded
                            };

                            let update = QueueUpdate {
                                queue_info: Some(QueueInfo {
                                    tree: queue.tree,
                                    queue: queue.queue,
                                    queue_type: queue.queue_type as u32,
                                    queue_size: queue.queue_size,
                                }),
                                slot: response.slot,
                                update_type: update_type as i32,
                            };

                            let _ = self.update_sender.send(update);
                            last_update_time.insert(key.clone(), now);
                        }
                    }

                    previous_state = current_state;
                }
                Err(e) => {
                    tracing::error!("Failed to fetch queue info for monitoring: {}", e);
                }
            }
        }
    }
}
