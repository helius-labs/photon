// We move all tests into a single tests/integration_tests directory so that we have a single test
// binary. This has a number of benefits:
// 1. Faster compile time since we only have 1 binary.
// 2. The ability to add global locks on the DB to prevent tests from interfering with each other.
mod batched_address_tree_tests;
mod batched_state_tree_tests;
mod e2e_tests;
mod mock_tests;
mod open_api_tests;
mod prod_tests;
mod snapshot_tests;
mod utils;
