use cadence_macros::statsd_count;
use once_cell::sync::OnceCell;
use solana_pubkey::Pubkey;

/// Events published by the ingestion pipeline
///
/// These events are published immediately when state changes occur during
/// transaction processing.
#[derive(Debug, Clone)]
pub enum IngestionEvent {
    /// Address queue insertion event
    /// Fired when new addresses are added to an address queue
    AddressQueueInsert {
        tree: Pubkey,
        queue: Pubkey,
        count: usize,
        slot: u64,
    },

    /// Output queue insertion event
    /// Fired when accounts are added to the output queue (StateV2)
    OutputQueueInsert {
        tree: Pubkey,
        queue: Pubkey,
        count: usize,
        slot: u64,
    },

    /// Nullifier queue insertion event
    /// Fired when nullifiers are added to the nullifier queue (StateV2)
    NullifierQueueInsert {
        tree: Pubkey,
        queue: Pubkey,
        count: usize,
        slot: u64,
    },
    // Future:
    // AccountCreated { hash: [u8; 32], tree: Pubkey, slot: u64 },
    // AccountNullified { hash: [u8; 32], tree: Pubkey, slot: u64 },
    // TreeRolledOver { old_tree: Pubkey, new_tree: Pubkey, slot: u64 },
}

/// Publisher for ingestion events
///
/// Ingestion code publishes events to this channel, which are then
/// distributed to all subscribers
pub type EventPublisher = tokio::sync::mpsc::UnboundedSender<IngestionEvent>;

/// Subscriber for ingestion events
pub type EventSubscriber = tokio::sync::mpsc::UnboundedReceiver<IngestionEvent>;

/// Global event publisher
///
/// This is initialized once at startup if event notifications are enabled.
static EVENT_PUBLISHER: OnceCell<EventPublisher> = OnceCell::new();

/// Initialize the global event publisher
///
/// This should be called once at startup. Returns the subscriber end of the channel.
pub fn init_event_bus() -> EventSubscriber {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    EVENT_PUBLISHER
        .set(tx)
        .expect("Event publisher already initialized");
    rx
}

/// Publish an event to all subscribers
///
/// This is a fire-and-forget operation. If no subscribers are listening,
/// the event is silently dropped.
pub fn publish(event: IngestionEvent) {
    let event_type = match &event {
        IngestionEvent::OutputQueueInsert { .. } => "output_queue_insert",
        IngestionEvent::AddressQueueInsert { .. } => "address_queue_insert",
        IngestionEvent::NullifierQueueInsert { .. } => "nullifier_queue_insert",
    };

    if let Some(publisher) = EVENT_PUBLISHER.get() {
        if let Err(e) = publisher.send(event) {
            tracing::warn!(
                "Failed to publish ingestion event to event bus: {} (event bus may be closed or full)",
                e
            );
            crate::metric! {
                statsd_count!("events.publish.failed", 1, "event_type" => event_type);
            }
        } else {
            crate::metric! {
                statsd_count!("events.publish.success", 1, "event_type" => event_type);
            }
        }
    } else {
        crate::metric! {
            statsd_count!("events.publish.not_initialized", 1, "event_type" => event_type);
        }
    }
}
