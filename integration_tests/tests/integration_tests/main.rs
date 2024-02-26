
// We move all tests into a single tests/integration_tests directory so that we have a single test
// binary. This has a number of benefits:
// 1. Faster compile time since we only have 1 binary. 
// 2. The ability to add global locks on the DB to prevent tests from interfering with each other.
mod persist_test;
mod utils;
