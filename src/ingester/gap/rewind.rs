use crate::ingester::gap::SequenceGap;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum RewindCommand {
    Rewind { to_slot: u64, reason: String },
}

#[derive(Debug, Error)]
pub enum RewindError {
    #[error("Failed to send rewind command: {0}")]
    SendError(String),
    #[error("Invalid rewind slot: {0}")]
    InvalidSlot(u64),
}

#[derive(Debug, Clone)]
pub struct RewindController {
    sender: mpsc::UnboundedSender<RewindCommand>,
}

impl RewindController {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<RewindCommand>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender }, receiver)
    }

    pub fn request_rewind(&self, to_slot: u64, reason: String) -> Result<(), RewindError> {
        let command = RewindCommand::Rewind { to_slot, reason };
        self.sender
            .send(command)
            .map_err(|e| RewindError::SendError(e.to_string()))?;
        Ok(())
    }

    pub fn request_rewind_for_gaps(&self, gaps: &[SequenceGap]) -> Result<(), RewindError> {
        if gaps.is_empty() {
            return Ok(());
        }

        let rewind_slot = determine_rewind_slot_from_gaps(gaps);
        let gap_count = gaps.len();
        let reason = format!("Sequence gaps detected: {} gaps found", gap_count);

        tracing::warn!(
            "Requesting rewind to slot {} due to {} sequence gaps",
            rewind_slot,
            gap_count
        );
        self.request_rewind(rewind_slot, reason)
    }
}

/// Determines the appropriate rewind slot based on detected gaps
/// Uses the earliest before_slot from all gaps to ensure we capture all missing data
fn determine_rewind_slot_from_gaps(gaps: &[SequenceGap]) -> u64 {
    gaps.iter()
        .map(|gap| gap.before_slot)
        .filter(|&slot| slot > 0) // Filter out zero slots from initialization
        .min()
        .unwrap_or(0) // Fallback to slot 0 if no valid slots found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingester::gap::{SequenceGap, StateUpdateFieldType};
    use solana_pubkey::Pubkey;

    #[test]
    fn test_rewind_controller_creation() {
        let (controller, _receiver) = RewindController::new();
        let result = controller.request_rewind(100, "test rewind".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_determine_rewind_slot_from_gaps() {
        let gaps = vec![
            SequenceGap {
                before_slot: 1000,
                after_slot: 1002,
                before_signature: "sig1".to_string(),
                after_signature: "sig2".to_string(),
                tree_pubkey: Some(Pubkey::new_unique()),
                field_type: StateUpdateFieldType::IndexedTreeUpdate,
            },
            SequenceGap {
                before_slot: 995,
                after_slot: 997,
                before_signature: "sig3".to_string(),
                after_signature: "sig4".to_string(),
                tree_pubkey: Some(Pubkey::new_unique()),
                field_type: StateUpdateFieldType::IndexedTreeUpdate,
            },
        ];

        let rewind_slot = determine_rewind_slot_from_gaps(&gaps);
        assert_eq!(rewind_slot, 995); // Should pick the earliest before_slot
    }

    #[test]
    fn test_determine_rewind_slot_filters_zero_slots() {
        let gaps = vec![
            SequenceGap {
                before_slot: 0, // Should be filtered out
                after_slot: 1002,
                before_signature: "".to_string(),
                after_signature: "sig2".to_string(),
                tree_pubkey: Some(Pubkey::new_unique()),
                field_type: StateUpdateFieldType::IndexedTreeUpdate,
            },
            SequenceGap {
                before_slot: 995,
                after_slot: 997,
                before_signature: "sig3".to_string(),
                after_signature: "sig4".to_string(),
                tree_pubkey: Some(Pubkey::new_unique()),
                field_type: StateUpdateFieldType::IndexedTreeUpdate,
            },
        ];

        let rewind_slot = determine_rewind_slot_from_gaps(&gaps);
        assert_eq!(rewind_slot, 995); // Should ignore slot 0 and pick 995
    }
}
