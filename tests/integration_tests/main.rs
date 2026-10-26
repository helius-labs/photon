// We move all tests into a single tests/integration_tests directory so that we have a single test
// binary. This has a number of benefits:
// 1. Faster compile time since we only have 1 binary.
// 2. The ability to add global locks on the DB to prevent tests from interfering with each other.
mod account_lookup_index_tests;
#[cfg(feature = "api-profiling")]
mod api_profile;
#[cfg(feature = "api-profiling")]
mod api_profile_workloads;
mod base58_serialization_tests;
mod batch_append_nullified_test;
mod batched_address_tree_tests;
mod batched_state_tree_tests;
mod e2e_tests;
mod keyset_cursor_tests;
mod merkle_tree_deserialization;
mod mock_tests;
mod monitor_tests;
mod open_api_tests;
mod owner_signature_history_tests;
mod owner_signature_tests;
mod prod_tests;
mod proof_root_sequence_tests;
mod queue_info_tests;
mod snapshot_tests;
mod utils;
mod zeroeth_element_fix_test;
