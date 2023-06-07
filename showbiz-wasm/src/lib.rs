#![forbid(unsafe_code)]
#![deny(warnings)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[cfg(not(feature = "async"))]
compile_error!("showbiz does not support sync currently, `async` feature must be enabled.");

pub use showbiz_core::*;
pub use showbiz_transport::*;

pub use wasm_agnostic as agnostic;

pub type TokioWasmShowbiz<D> = showbiz_core::Showbiz<
  D,
  showbiz_transport::NetTransport<agnostic::tokio::TokioWasmRuntime>,
  agnostic::tokio::TokioWasmRuntime,
>;
