//! The backend driver poll-pumps, one per reliable plane.
//!
//! [`quic`] owns a `QuicEndpoint` over QUIC datagrams and streams; [`stream`]
//! owns a `StreamEndpoint` over plain-TCP / TLS records. Each is a quinn-style
//! `Future::poll` pump that solely owns its machine endpoint and advances it.

#[cfg(feature = "quic")]
pub(crate) mod quic;
#[cfg(any(feature = "tcp", feature = "tls"))]
pub(crate) mod stream;

#[cfg(any(feature = "tcp", feature = "tls", feature = "quic"))]
use smallvec::SmallVec;
#[cfg(any(feature = "tcp", feature = "tls", feature = "quic"))]
use std::net::SocketAddr;

/// Wall-clock bound on deferring a DUE `handle_timeout` behind capped inbound
/// paths. Inbound drains roughly FIFO, so the pre-deadline backlog clears
/// within a bounded number of capped polls (microseconds at gossip datagram
/// sizes) — the grace exists so a sustained external flood that keeps every
/// poll capped cannot suppress failure detection, while a burst that merely
/// truncated one batch never prematurely times out the input it buffered.
/// Shared by every reliable-plane driver so both gates use the same bound.
#[cfg(any(feature = "tcp", feature = "tls", feature = "quic"))]
pub(crate) const TIMEOUT_STALENESS_GRACE: core::time::Duration =
  core::time::Duration::from_millis(5);

/// Build a fully-resolved join's reply from its reached set and requested count.
/// A non-empty set resolves `Ok(set)`; an empty set is the all-failed case,
/// surfacing `JoinFailed { requested, contacted: 0 }` with an empty
/// reached-so-far set (the partial-success slot mirrored from the serf driver).
#[cfg(any(feature = "tcp", feature = "tls", feature = "quic"))]
pub(crate) fn join_reply(
  contacted: SmallVec<[SocketAddr; 1]>,
  requested: usize,
) -> crate::command::JoinReply {
  if contacted.is_empty() {
    Err((
      SmallVec::new(),
      crate::error::Error::JoinFailed(crate::error::JoinFailed::new(requested, 0)),
    ))
  } else {
    Ok(contacted)
  }
}
