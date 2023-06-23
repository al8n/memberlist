#![forbid(unsafe_code)]
#![deny(warnings)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[cfg(not(feature = "async"))]
compile_error!("showbiz does not support sync currently, `async` feature must be enabled.");

pub use showbiz_core::*;

pub use wasm_agnostic as agnostic;

pub type TokioWasmShowbiz<D> = showbiz_core::Showbiz<
  D,
  showbiz_core::transport::net::NetTransport<agnostic::tokio::TokioWasmRuntime>,
  agnostic::tokio::TokioWasmRuntime,
>;

#[cfg(test)]
mod wasm_tests {
  use showbiz_core::tests::*;
  use tokio::runtime::Runtime;
  use wasm_agnostic::tokio::TokioWasmRuntime;

  #[test]
  fn test_create_secret_key() {
    Runtime::new()
      .unwrap()
      .block_on(test_create_secret_key_runner::<TokioWasmRuntime>());
  }

  #[test]
  fn test_create_secret_key_empty() {
    Runtime::new()
      .unwrap()
      .block_on(test_create_secret_key_empty_runner::<TokioWasmRuntime>());
  }

  #[test]
  fn test_create_keyring_only() {
    Runtime::new()
      .unwrap()
      .block_on(test_create_keyring_only_runner::<TokioWasmRuntime>());
  }

  #[test]
  fn test_create_keyring_and_primary_key() {
    Runtime::new()
      .unwrap()
      .block_on(test_create_keyring_and_primary_key_runner::<TokioWasmRuntime>());
  }

  #[test]
  fn test_create() {
    Runtime::new()
      .unwrap()
      .block_on(test_create_runner::<TokioWasmRuntime>());
  }
}
