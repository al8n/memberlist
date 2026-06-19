//! The driver layer: the per-transport driver event loop plus the helpers and
//! options it shares with the QUIC driver.
//!
//! [`stream`] is the TCP/TLS reliable-plane driver; the QUIC driver lives in
//! [`crate::quic::driver`]. [`shared`] holds the observation / event hand-off
//! helpers both loops reuse, and [`options`] the generic-free driver knobs.

pub(crate) mod options;
pub(crate) mod shared;
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
pub(crate) mod stream;
