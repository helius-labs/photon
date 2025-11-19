use lazy_static::lazy_static;
use num_bigint::BigUint;
use std::str::FromStr;

mod helpers;
mod proof;

pub use helpers::{
    compute_hash_by_tree_pubkey, compute_hash_by_tree_type, compute_hash_with_cache,
    compute_range_node_hash_v1, compute_range_node_hash_v2,
    get_top_element, get_zeroeth_exclusion_range, get_zeroeth_exclusion_range_v1,
};

pub use proof::{
    get_exclusion_range_with_proof_v2, get_multiple_exclusion_ranges_with_proofs_v2,
    query_next_smallest_elements,
};

lazy_static! {
    pub static ref HIGHEST_ADDRESS_PLUS_ONE: BigUint = BigUint::from_str(
        "452312848583266388373324160190187140051835877600158453279131187530910662655"
    )
    .unwrap();
}
