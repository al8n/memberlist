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
  event::Event,
  streams::{StreamEndpoint, bridge::StreamBridge, phase::StreamPhase, transport::StreamTransport},
};

/// Default peer-to-socket resolver for test fixtures where `A = SocketAddr` —
/// the identity. Mirrors `test_sni_provider`'s shape (boxed closure) so test
/// `StreamEndpoint::new` / `with_compression` call sites compose the two
/// closures uniformly.
#[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) fn label(s: &str) -> Option<Vec<u8>> {
  Some(s.as_bytes().to_vec())
}

/// Default SNI provider for test fixtures — returns `Some("localhost")` to
/// match the localhost-SAN test certs used by TLS tests, and is also
/// harmlessly supplied for plain-TCP tests (the TCP record layer ignores SNI).
#[allow(dead_code)]
pub(crate) fn test_sni_provider<A>() -> Box<dyn Fn(&A) -> Option<String> + Send + Sync>
where
  A: 'static,
{
  Box::new(|_addr: &A| Some("localhost".to_string()))
}

/// `Endpoint<SmolStr, SocketAddr>` rooted at the loopback `port`, named
/// `n-<port>`.
#[allow(dead_code)]
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

/// Shuttle bytes both ways once: `a` out → `b` in, `b` out → `a` in at
/// `now`. Returns `true` if either side produced bytes this round.
#[allow(dead_code)]
pub(crate) fn shuttle<R>(
  a: &mut StreamBridge<SmolStr, SocketAddr, R>,
  b: &mut StreamBridge<SmolStr, SocketAddr, R>,
  now: Instant,
) -> bool
where
  R: StreamTransport,
{
  let mut moved = false;
  let mut buf = Vec::new();
  if a.poll_transport_transmit(&mut buf) > 0 {
    // Ignoring Err: a terminated bridge is detected via `is_terminal()` /
    // `phase()` at call sites, exactly as the real coordinator reaps on
    // terminality rather than the `Result`.
    let _ = b.handle_transport_data(&buf, now);
    moved = true;
  }
  buf.clear();
  if b.poll_transport_transmit(&mut buf) > 0 {
    // Ignoring Err: same reason as the `a → b` direction above.
    let _ = a.handle_transport_data(&buf, now);
    moved = true;
  }
  moved
}

/// Drain every endpoint event currently queued on the coordinator into a
/// `Vec`, in queue order.
#[allow(dead_code)]
pub(crate) fn drain_events<I, A, R>(coord: &mut StreamEndpoint<I, A, R>) -> Vec<Event<I, A>>
where
  I: crate::Id
    + crate::Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: crate::Data
    + crate::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let mut out = Vec::new();
  while let Some(ev) = coord.poll_event() {
    out.push(ev);
  }
  out
}

/// Assert a bridge's current phase matches `expected`, including a
/// description of the actual phase in the diagnostic if the assertion fails.
///
/// `StreamPhase` does not implement `PartialEq` (its `BridgePhase` interior
/// is not equality-comparable), so the comparison is structural via
/// `phase_label`.
#[allow(dead_code)]
pub(crate) fn assert_phase<R>(bridge: &StreamBridge<SmolStr, SocketAddr, R>, expected: &StreamPhase)
where
  R: StreamTransport,
{
  let actual = bridge.phase_ref();
  assert!(
    phase_label(actual) == phase_label(expected),
    "bridge phase mismatch: expected {}, got {}",
    phase_label(expected),
    phase_label(actual),
  );
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
