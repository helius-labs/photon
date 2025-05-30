use crate::api::error::PhotonApiError;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::persist::compute_parent_hash;
use crate::ingester::persist::leaf_node::leaf_index_to_node_index;
use crate::metric;
use cadence_macros::statsd_count;
use log::info;

#[derive(Debug, Clone)]
pub struct MerkleProofWithContext {
    pub proof: Vec<Hash>,
    pub root: Hash,
    pub leaf_index: u32,
    pub hash: Hash,
    pub merkle_tree: SerializablePubkey,
    pub root_seq: u64,
}

impl MerkleProofWithContext {
    pub fn validate(&self) -> Result<(), PhotonApiError> {
        info!(
            "Validating proof for leaf index: {} tree: {}",
            self.leaf_index, self.merkle_tree
        );
        let leaf_index = self.leaf_index;
        let tree_height = (self.proof.len() + 1) as u32;
        let node_index = leaf_index_to_node_index(leaf_index, tree_height);
        let mut computed_root = self.hash.to_vec();
        info!("leaf_index: {}, node_index: {}", leaf_index, node_index);

        for (idx, node) in self.proof.iter().enumerate() {
            let is_left = (node_index >> idx) & 1 == 0;
            computed_root = compute_parent_hash(
                if is_left {
                    computed_root.clone()
                } else {
                    node.to_vec()
                },
                if is_left {
                    node.to_vec()
                } else {
                    computed_root.clone()
                },
            )
            .map_err(|e| {
                PhotonApiError::UnexpectedError(format!(
                    "Failed to compute parent hash for proof: {}",
                    e
                ))
            })?;
        }
        if computed_root != self.root.to_vec() {
            metric! {
                statsd_count!("invalid_proof", 1);
            }
            return Err(PhotonApiError::UnexpectedError(format!(
                "Computed root does not match the provided root. Proof; {:?}",
                self
            )));
        }

        Ok(())
    }
}
