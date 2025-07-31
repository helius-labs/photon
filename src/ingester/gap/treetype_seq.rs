use crate::ingester::gap::{SequenceEntry, StateUpdateFieldType};
use tracing::debug;

#[derive(Debug, Clone)]
pub enum TreeTypeSeq {
    StateV1(SequenceEntry),
    // Output queue (leaf index), Input queue index, Batch event seq with context
    StateV2(StateV2SeqWithContext),
    // event seq with complete context
    AddressV1(SequenceEntry),
    // Input queue index, Batch event seq with context
    AddressV2(SequenceEntry, SequenceEntry), // (input_queue_entry, batch_event_entry)
}

impl Default for TreeTypeSeq {
    fn default() -> Self {
        TreeTypeSeq::StateV1(SequenceEntry::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct StateV2SeqWithContext {
    pub input_queue_entry: Option<SequenceEntry>,
    pub batch_event_entry: Option<SequenceEntry>,
    pub output_queue_entry: Option<SequenceEntry>,
}

/// Helper functions for elegant state updates
impl TreeTypeSeq {
    /// Gets existing StateV2 context or creates a default one
    fn get_or_default_state_v2(current: Option<&TreeTypeSeq>) -> StateV2SeqWithContext {
        current
            .and_then(|seq| match seq {
                TreeTypeSeq::StateV2(ctx) => Some(ctx.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Gets existing AddressV2 input queue entry or creates a default one
    fn get_or_default_address_v2_input(current: Option<&TreeTypeSeq>) -> SequenceEntry {
        current
            .and_then(|seq| match seq {
                TreeTypeSeq::AddressV2(input, _) => Some(input.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Creates a new StateV2 with updated output queue entry
    pub(crate) fn new_state_v2_with_output(
        current: Option<&TreeTypeSeq>,
        output_entry: SequenceEntry,
    ) -> TreeTypeSeq {
        let mut ctx = Self::get_or_default_state_v2(current);
        ctx.output_queue_entry = Some(output_entry);
        TreeTypeSeq::StateV2(ctx)
    }

    /// Creates a new AddressV2 preserving input queue entry
    pub(crate) fn new_address_v2_with_output(
        current: Option<&TreeTypeSeq>,
        output_entry: SequenceEntry,
    ) -> TreeTypeSeq {
        let input_entry = Self::get_or_default_address_v2_input(current);
        TreeTypeSeq::AddressV2(input_entry, output_entry)
    }

    /// Extracts sequence information based on field type and tree type
    ///
    /// Returns `(sequence_number, optional_entry)` where:
    /// - `u64::MAX` indicates invalid state - tree type mismatch or unexpected configuration.
    ///   Gap detection will be skipped entirely for these cases.
    /// - `0` indicates valid initial state - the expected tree type exists but the specific
    ///   sequence entry hasn't been initialized yet. Gap detection remains active.
    /// - Any other value represents an actual sequence number from existing state.
    ///
    /// This distinction is important because:
    /// - Invalid configurations (u64::MAX) should not trigger false-positive gap alerts
    /// - Valid but uninitialized sequences (0) should still detect gaps if the first
    ///   observed sequence is > 1
    pub fn extract_sequence_info(
        &self,
        field_type: &StateUpdateFieldType,
    ) -> (u64, Option<SequenceEntry>) {
        match field_type {
            StateUpdateFieldType::IndexedTreeUpdate => match self {
                TreeTypeSeq::AddressV1(entry) => {
                    debug!("IndexedTreeUpdate with AddressV1, seq: {}", entry.sequence);
                    (entry.sequence, Some(entry.clone()))
                }
                _ => {
                    debug!("IndexedTreeUpdate with unsupported tree type: {:?}", self);
                    (u64::MAX, None)
                }
            },
            StateUpdateFieldType::BatchMerkleTreeEventAddressAppend
            | StateUpdateFieldType::BatchNewAddress => {
                if let TreeTypeSeq::AddressV2(_, entry) = self {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    debug!("Expected AddressV2 for {:?}, got {:?}", field_type, self);
                    (u64::MAX, None)
                }
            }
            StateUpdateFieldType::BatchMerkleTreeEventAppend
            | StateUpdateFieldType::BatchMerkleTreeEventNullify => {
                if let TreeTypeSeq::StateV2(seq_context) = self {
                    if let Some(entry) = &seq_context.batch_event_entry {
                        (entry.sequence, Some(entry.clone()))
                    } else {
                        (0, None)
                    }
                } else {
                    debug!("Expected StateV2 for {:?}, got {:?}", field_type, self);
                    (u64::MAX, None)
                }
            }
            StateUpdateFieldType::LeafNullification => {
                if let TreeTypeSeq::StateV1(entry) = self {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    debug!("Expected StateV1 for LeafNullification, got {:?}", self);
                    (u64::MAX, None)
                }
            }
            StateUpdateFieldType::OutAccount => match self {
                TreeTypeSeq::StateV1(entry) => (entry.sequence, Some(entry.clone())),
                TreeTypeSeq::StateV2(seq_context) => {
                    if let Some(entry) = &seq_context.output_queue_entry {
                        (entry.sequence, Some(entry.clone()))
                    } else {
                        (0, None)
                    }
                }
                _ => {
                    debug!("Expected StateV1/V2 for OutAccount, got {:?}", self);
                    (u64::MAX, None)
                }
            },
            StateUpdateFieldType::BatchNullifyContext => {
                if let TreeTypeSeq::StateV2(seq_context) = self {
                    if let Some(entry) = &seq_context.input_queue_entry {
                        (entry.sequence, Some(entry.clone()))
                    } else {
                        (0, None)
                    }
                } else {
                    debug!("Expected StateV2 for BatchNullifyContext, got {:?}", self);
                    (u64::MAX, None)
                }
            }
        }
    }
}
