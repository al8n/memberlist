//! Shared test-fixture vocabulary for `StreamTransport` consumers.
//!
//! Visible to `tcp::*::tests`, `tls::*::tests`, and `streams::*::tests`
//! via `use crate::streams::test_support::*`. Gated `#[cfg(test)]` so
//! the module has zero non-test-build footprint.

use crate::Instant;
use core::net::{IpAddr, Ipv4Addr, SocketAddr};

use smol_str::SmolStr;

use crate::{
  config::EndpointOptions,
  endpoint::Endpoint,
  streams::{bridge::StreamBridge, phase::StreamPhase, transport::StreamTransport},
};

/// Default peer-to-socket resolver for test fixtures where `A = SocketAddr` —
/// the identity. Mirrors `test_sni_provider`'s shape (boxed closure) so test
/// `StreamEndpoint::new` / `with_compression` call sites compose the two
/// closures uniformly.
pub(crate) fn test_peer_to_socket() -> Box<dyn Fn(&SocketAddr) -> SocketAddr + Send + Sync> {
  Box::new(|addr: &SocketAddr| *addr)
}

/// Reliable-unit ceiling for bridge test pairs — `EndpointOptions` default
/// `max_stream_frame_size`, generous above every frame these tests exchange.
pub(crate) const TEST_RELIABLE_MAX: usize = 64 * 1024 * 1024;

/// IPv4 loopback `SocketAddr` on the given port.
pub(crate) fn addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

/// Wrap a cluster-label string into the `Option<Vec<u8>>` shape the TCP
/// records layer expects.
pub(crate) fn label(s: &str) -> Option<Vec<u8>> {
  Some(s.as_bytes().to_vec())
}

/// Default SNI provider for test fixtures — returns `Some("localhost")` to
/// match the localhost-SAN test certs used by TLS tests, and is also
/// harmlessly supplied for plain-TCP tests (the TCP record layer ignores SNI).
pub(crate) fn test_sni_provider<A>() -> Box<dyn Fn(&A) -> Option<String> + Send + Sync>
where
  A: 'static,
{
  Box::new(|_addr: &A| Some("localhost".to_string()))
}

/// `Endpoint<SmolStr, SocketAddr>` rooted at the loopback `port`, named
/// `n-<port>`.
pub(crate) fn endpoint(port: u16) -> Endpoint<SmolStr, SocketAddr> {
  Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new(format!("n-{port}")),
    addr(port),
  ))
}

/// Build a `Handshaking` dialer/acceptor `StreamBridge` pair from
/// caller-supplied record constructors. Compression + encryption start
/// disabled; both bridges share `deadline` and `TEST_RELIABLE_MAX`.
///
/// `FD`/`FA` produce the record-layer halves — TCP supplies
/// `RawRecords::dialer(label, false)` / `acceptor(...)`; TLS supplies
/// `TlsRecords::client(cfg, server_name)` / `server(cfg)`.
pub(crate) fn handshaking_pair<R, FD, FA>(
  deadline: Instant,
  build_dialer: FD,
  build_acceptor: FA,
) -> (
  StreamBridge<SmolStr, SocketAddr, R>,
  StreamBridge<SmolStr, SocketAddr, R>,
)
where
  R: StreamTransport,
  FD: FnOnce() -> R,
  FA: FnOnce() -> R,
{
  let dialer = build_dialer();
  let acceptor = build_acceptor();
  (
    StreamBridge::new(
      dialer,
      deadline,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      TEST_RELIABLE_MAX,
    ),
    StreamBridge::new(
      acceptor,
      deadline,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      TEST_RELIABLE_MAX,
    ),
  )
}

/// Render a `StreamPhase` as a static string label for diagnostics.
pub(crate) fn phase_label(p: &StreamPhase) -> &'static str {
  use crate::bridge_phase::BridgePhase;
  match p {
    StreamPhase::Handshaking => "Handshaking",
    StreamPhase::Established(BridgePhase::Active) => "Established(Active)",
    StreamPhase::Established(BridgePhase::SendClosed) => "Established(SendClosed)",
    StreamPhase::Established(BridgePhase::RecvClosed) => "Established(RecvClosed)",
    StreamPhase::Established(BridgePhase::BothClosed) => "Established(BothClosed)",
    StreamPhase::Established(BridgePhase::Failed(_)) => "Established(Failed)",
  }
}
