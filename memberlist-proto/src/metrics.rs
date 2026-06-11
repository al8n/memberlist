//! Operational counters for the Sans-I/O machine.
//!
//! [`Metrics`] is a snapshot of cumulative `u64` counters the single-owner
//! machine bumps as it sheds load at its bounds — every count is a datagram,
//! node, connection, or payload the machine deliberately dropped or withheld.
//! Because the machine is single-owner and synchronous, the counters are plain
//! integers (no atomics); a driver reads a `Copy` snapshot via the endpoint and
//! re-exports it on its handle, alongside the membership snapshot.
//!
//! The counters are monotonically increasing for the lifetime of an endpoint and
//! never reset; a consumer computes rates by differencing successive snapshots.

/// A snapshot of the machine's operational counters. See the module docs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Metrics {
  /// Inbound gossip datagrams dropped because the machine-side ingress buffer
  /// was already full (a memory-DoS backstop against a driver that does not gate
  /// its socket reads).
  pub gossip_ingress_dropped: u64,
  /// New nodes refused admission at the optional `max_members` ceiling. A
  /// refused node is never added to membership; existing members are unaffected.
  pub members_rejected: u64,
  /// Inbound reliable connections refused at the optional `max_inbound_streams`
  /// ceiling (no bridge built; the driver drops the connection).
  pub inbound_streams_rejected: u64,
  /// Indirect-probe forward requests dropped at the `max_indirect_forwards`
  /// ceiling (a relay-amplification backstop).
  pub indirect_forwards_dropped: u64,
  /// Ack payloads withheld from a probe whose source could not be bound to a
  /// tracked member's address (`ack_payload_to_members_only`), bounding the
  /// reflective byte-amplification a spoofed-source ping can elicit.
  pub ack_payloads_withheld: u64,
}
