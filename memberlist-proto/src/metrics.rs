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
  /// Inbound gossip datagrams rejected at admission because they exceeded the
  /// endpoint's own configured gossip MTU (plus its wrapper overheads) —
  /// closing the gap where a source-rotating flood of near-max-datagram-size
  /// frames could otherwise fill `gossip_ingress_dropped`'s count cap at
  /// hundreds of MiB of attacker-controlled queued bytes. Distinct from
  /// `gossip_ingress_dropped`: this is a SIZE rejection, counted before the
  /// datagram can count against either the per-peer or the node-global
  /// ingress cap, never a count-cap load-shed. Zero on a transport with no
  /// per-datagram size cap.
  pub gossip_ingress_oversized: u64,
  /// New nodes refused admission at the optional `max_members` ceiling. A
  /// refused node is never added to membership; existing members are unaffected.
  pub members_rejected: u64,
  /// Inbound reliable connections refused at the optional `max_inbound_streams`
  /// ceiling (no bridge built; the driver drops the connection).
  pub inbound_streams_rejected: u64,
  /// QUIC connections refused at a connection cap before any new
  /// connection-table entry was committed — a memory-exhaustion backstop.
  /// Counts both directions:
  ///
  /// - Inbound Initials (unauthenticated) dropped because the global
  ///   `max_quic_connections` cap or the per-source
  ///   `max_pending_connections_per_source` cap was already reached (the Initial
  ///   is dropped via `quinn_proto::Endpoint::ignore`, no entry created); and
  /// - Outbound dials (reliable exchanges and datagram fallbacks) refused
  ///   because the global `max_quic_connections` cap was already reached, so no
  ///   new outbound connection was created.
  pub quic_connections_rejected: u64,
  /// Indirect-probe forward requests dropped at the `max_indirect_forwards`
  /// ceiling (a relay-amplification backstop).
  pub indirect_forwards_dropped: u64,
  /// Indirect-probe forward requests dropped because the forwarded `Ping`, once
  /// framed, would exceed `gossip_mtu` (a single-datagram budget). A relayed
  /// `IndirectPing` carries an attacker-controlled target id, and node ids are
  /// unbounded, so a large target id can push the forwarded Ping past the
  /// budget; the relay is dropped with no forwarded Ping and no Nack rather than
  /// emitted as a fragmentable, undeliverable datagram. Distinct from
  /// `indirect_forwards_dropped`, which is the `max_indirect_forwards` flood cap.
  pub indirect_forwards_oversized: u64,
  /// Ack payloads withheld from a probe whose source could not be bound to a
  /// tracked member's address (`ack_payload_to_members_only`), bounding the
  /// reflective byte-amplification a spoofed-source ping can elicit.
  pub ack_payloads_withheld: u64,
  /// Inbound failure evidence (`Dead`/`Suspect`) refused at an external ingress
  /// funnel because its incarnation was `u32::MAX` — the single unrefutable
  /// accusation, which an honest node never emits about a live peer. Refusing it
  /// keeps a plaintext forger from pinning a live id un-refutably. Locally
  /// synthesized failure detection is unaffected.
  pub unrefutable_failure_rejected: u64,
}
