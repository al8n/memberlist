//! Shared test fixtures for the compio test binaries.
//!
//! Per Cargo's per-test-binary model, each `tests/*.rs` is a
//! separate compilation unit; modules under `tests/support/` are
//! pulled in via `#[path = "support/<file>.rs"] mod support;` from
//! each consumer test file.
