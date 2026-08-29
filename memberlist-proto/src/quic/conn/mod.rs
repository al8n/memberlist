//! Per-peer QUIC connection table. One long-lived `quinn_proto::Connection`
//! per peer (idle-evicted by quinn-proto's own `max_idle_timeout`); reaped
//! only when `Connection::is_drained()` (the same protocol quinn uses).
//!
//! ## Residual: the transport layer does not order instance epochs
//!
//! Establishment does not prove the remote instance is *currently* alive: the
//! server completes a handshake by replying to a client's `Finished`, and a
//! delayed pre-crash client `Finished` still arriving under network delay drives
//! that same completion. Nor does any transport-local signal reliably order two
//! connections by which peer instance-epoch opened them — under unbounded
//! network delay a delayed packet can interleave to fool creation-order,
//! establishment-order, and close-the-older tracking alike. This table therefore
//! makes NO attempt to rank connection instance-epochs; it selects the best
//! *usable* connection and lets the most recently established inbound win.
//!
//! The bounded consequence: a zombie (an established connection to a peer's dead
//! prior instance) can capture selection, but only until the negotiated idle
//! timeout reaps it. That finite bound is guaranteed by the file-based
//! `QuicConfigOptions::build` path, which rejects a `max_idle_timeout` that
//! encodes to a disabling zero (zero, or a sub-millisecond value quinn rounds to
//! zero) — so a config-built endpoint always negotiates a finite idle timeout
//! (the minimum of the two peers'). The raw `QuicOptions::new` /
//! `new_with_sni_provider` constructors are an advanced escape hatch that takes a
//! caller-supplied `TransportConfig` verbatim, so a caller CAN disable the idle
//! timeout there (`max_idle_timeout(None)` or an encoded-zero value) and thereby
//! forgo the bound — an opt-out those constructors caution against. Even so this
//! is safety-preserving: every established connection is serviced by the
//! coordinator's unconditional accept loops, so the peer's traffic and its SWIM
//! refutations still flow over whichever connection each side prefers. The signal
//! that actually resolves an instance-epoch conflict is the membership
//! incarnation at the SWIM layer, one layer up — not the transport.

use crate::Instant;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::collections::{HashMap, VecDeque};

use quinn_proto::{Connection, ConnectionEvent, ConnectionHandle, Endpoint as QuinnEndpoint};
use slab::Slab;
use smallvec_wrapper::MediumVec;

/// Which side opened a pooled connection. Only inbound (server-accepted)
/// connections consume a source's per-source pending allowance: a local
/// outbound dial to a peer must never charge against that peer's inbound cap,
/// or simultaneous bidirectional dialing wedges (each side's own outbound to
/// the peer would block the peer's inbound Initial).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ConnDirection {
  /// Locally initiated via [`ConnTable::get_or_dial`].
  Outbound,
  /// Peer-initiated, accepted by the server endpoint via
  /// [`ConnTable::insert_accepted`].
  Inbound,
}

/// Prefix lengths that normalize an inbound source address into the bucket the
/// per-source half-open admission cap and the coordinator-wide half-open budget
/// count against.
///
/// Keying admission on a normalized source IP (rather than the full
/// `SocketAddr`) is what stops a single host from bypassing its per-source
/// allowance by rotating its UDP source port: every port from one address (or,
/// at a shorter prefix, one subnet) folds to the same [`SourceKey`].
///
/// Defaults are `/32` (v4) and `/64` (v6). The v6 default is `/64`, NOT `/128`:
/// a host owns its entire SLAAC `/64`, so keying on the full `/128` would
/// reopen exactly the bypass this normalization closes — a host rotating IPv6
/// source addresses inside its own `/64`. `/64` is therefore required for the
/// property to hold, not a tuning nicety. An operator whose peers share a `/64`
/// (a mainstream cloud topology — e.g. an AWS `/64`-per-subnet — puts many
/// honest peers behind one key) can widen the grain to `/128` at the cost of
/// reopening the rotation surface, relying on the per-source cap instead.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SourcePrefix {
  v4: u8,
  v6: u8,
}

impl SourcePrefix {
  /// Default IPv4 prefix length: `/32` (the full address). Every UDP source
  /// port from one IPv4 host folds to one key.
  pub const DEFAULT_V4: u8 = 32;
  /// Default IPv6 prefix length: `/64` (the SLAAC subnet a host owns). See the
  /// type docs for why this is `/64` and not `/128`.
  pub const DEFAULT_V6: u8 = 64;

  /// Compose a prefix pair. `v4 <= 32` and `v6 <= 128` are operator invariants
  /// enforced by
  /// [`QuicOptions::validate`](crate::quic::QuicOptions::validate); a `0`
  /// selects the whole-family bucket.
  #[must_use]
  #[inline(always)]
  pub const fn new(v4: u8, v6: u8) -> Self {
    Self { v4, v6 }
  }

  /// The IPv4 prefix length.
  #[inline(always)]
  pub const fn v4(&self) -> u8 {
    self.v4
  }

  /// The IPv6 prefix length.
  #[inline(always)]
  pub const fn v6(&self) -> u8 {
    self.v6
  }
}

impl Default for SourcePrefix {
  #[inline(always)]
  fn default() -> Self {
    Self::new(Self::DEFAULT_V4, Self::DEFAULT_V6)
  }
}

/// A normalized inbound-source bucket: the source IP masked to the configured
/// [`SourcePrefix`], with v4-mapped IPv6 canonicalized to IPv4 first. The grain
/// the pending-inbound index and the half-open budget account against.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct SourceKey(IpAddr);

/// Normalize `peer` into its [`SourceKey`] under `prefix`.
///
/// The peer IP is first canonicalized with `to_canonical` (stable since Rust
/// 1.75, below the crate MSRV): a v4-mapped IPv6 address (`::ffff:a.b.c.d`,
/// which a dual-stack listening socket reports for an IPv4 peer) collapses to
/// its IPv4 form, so one host cannot split into two buckets by reaching a
/// dual-stack endpoint over both families. The canonical address is then masked
/// to the family's prefix.
pub(crate) fn source_key(prefix: SourcePrefix, peer: &SocketAddr) -> SourceKey {
  match peer.ip().to_canonical() {
    IpAddr::V4(v4) => SourceKey(IpAddr::V4(mask_v4(v4, prefix.v4))),
    IpAddr::V6(v6) => SourceKey(IpAddr::V6(mask_v6(v6, prefix.v6))),
  }
}

/// Mask an IPv4 address to its high `prefix` bits (`prefix` in `0..=32`).
fn mask_v4(addr: Ipv4Addr, prefix: u8) -> Ipv4Addr {
  // A prefix of `0` selects the whole-family bucket. Branch on it explicitly:
  // `u32::MAX << 32` is, in release, a release-mode masked shift count (Rust
  // masks the shift amount by the operand width, so a shift of 32 masks to 0
  // and the shift is a no-op), which would silently yield the WRONG mask (`/0`
  // behaving as `/32`) rather than the intended all-zero mask. Do NOT rely on
  // the debug-only shift-overflow panic to catch this.
  if prefix == 0 {
    return Ipv4Addr::UNSPECIFIED;
  }
  // `prefix` is in `1..=32` here, so the shift amount `32 - prefix` is in
  // `0..=31` — always a valid `u32` shift.
  let mask = u32::MAX << (32 - prefix);
  Ipv4Addr::from(u32::from(addr) & mask)
}

/// Mask an IPv6 address to its high `prefix` bits (`prefix` in `0..=128`).
fn mask_v6(addr: Ipv6Addr, prefix: u8) -> Ipv6Addr {
  // See [`mask_v4`]: `u128::MAX << 128` is a release-mode masked shift count
  // that would produce `/0` behaving as `/128`, so the whole-family bucket is
  // branched explicitly rather than left to a debug-only panic.
  if prefix == 0 {
    return Ipv6Addr::UNSPECIFIED;
  }
  // `prefix` is in `1..=128` here, so the shift amount `128 - prefix` is in
  // `0..=127` — always a valid `u128` shift.
  let mask = u128::MAX << (128 - prefix);
  Ipv6Addr::from(u128::from(addr) & mask)
}

/// Why [`ConnTable::get_or_dial`] could not return a connection handle.
///
/// Both variants are terminal for the dial attempt; the caller routes the
/// failure the same way it routes a bare dial error — retire the reliable
/// intent through `dial_failed`, or drop a best-effort datagram.
#[derive(Debug)]
pub(crate) enum DialError {
  /// quinn refused to initiate the connection, before any I/O — e.g. CIDs
  /// exhausted or a malformed remote address.
  Connect(quinn_proto::ConnectError),
  /// The global `max_quic_connections` cap is already reached, so NO new
  /// outbound connection may be created. Fires only on the no-reusable-entry
  /// branch: reuse of an existing usable pooled connection (or of a cached
  /// closed handle awaiting its drained-reap) returns before this check, so the
  /// cap bounds total tracked connections without blocking traffic on an
  /// already-pooled peer.
  AtGlobalCap,
}

pub(crate) struct ConnEntry {
  conn: Connection,
  peer: SocketAddr,
  /// Which side opened this connection — [`ConnDirection::Outbound`] for a local
  /// dial, [`ConnDirection::Inbound`] for a server-accepted peer dial. The
  /// per-source pending cap counts inbound connections only (see
  /// [`ConnTable::pending_inbound_from`]).
  direction: ConnDirection,
  /// `true` once the connection has been observed Established
  /// (`!is_handshaking() && !is_closed()`) at least once. Sticky — never
  /// reset. Distinguishes a previously-healthy pooled connection now in
  /// its `Closed`/`Draining` drain window (where `get_or_dial` redials so
  /// a push/pull / reliable-ping intent is not lost in the closed-before-
  /// drained pool window) from a never-Established handshake-failing
  /// connection (where redialing would just produce the same handshake
  /// failure indefinitely — the existing `dial_failed` path is the right
  /// outcome).
  established_at_least_once: bool,
  /// `true` while this entry contributes one unit to its source's
  /// [`ConnTable::pending_inbound`] index. Set exactly once — when an INBOUND
  /// connection is inserted by [`ConnTable::insert_accepted`] (an outbound dial
  /// is never indexed) — and cleared exactly once, whichever comes first:
  /// [`ConnTable::reconcile_pending_inbound`] observes it establish, or
  /// [`ConnTable::reap_if_drained`] removes it while still un-established. It is
  /// the single source of truth the index decrement is guarded on, so every
  /// increment is matched by exactly one decrement (no leak, no double-count)
  /// independent of how many servicing passes run between accept and
  /// establishment or reap.
  pending_indexed: bool,
  /// `ConnectionEvent`s produced by `quinn_proto::Endpoint::handle_event`
  /// during one `service_quinn` iteration on this connection, queued for
  /// delivery on the NEXT iteration of THIS connection. See
  /// [`Self::queue_pending_event`] / [`Self::take_pending_events`].
  pending_events: VecDeque<ConnectionEvent>,
  /// Count of live DIALER (locally opened via `open(Dir::Bi)`) bidi bridges
  /// currently riding this connection. A bridge is a dialer iff its bridge-side
  /// `eager_outbound_label` is `true`; the coordinator increments this at every
  /// dial mint and decrements it at every dialer-bridge reap under that SAME
  /// predicate, so the count returns to 0 once a connection's dialer bridges
  /// have all reaped (and is discarded with the entry if the connection is
  /// dropped wholesale). It bounds the LOCAL outbound half of quinn's
  /// `connection_blocked` set: the coordinator admission-gates `open(Dir::Bi)`
  /// at `super::C_OUT`, so a peer advertising an enormous MAX_STREAMS cannot
  /// make this node hold an attacker-scaled outbound bidi-stream population.
  /// Independent of the coordinator's separate `inbound_bridge_count` (which
  /// counts accepted bridges); a bridge contributes to exactly one of the two.
  outbound_bridge_count: usize,
}

impl ConnEntry {
  pub(crate) fn conn_mut(&mut self) -> &mut Connection {
    // Lazy-track `established_at_least_once` on every mutable observation:
    // any tick that calls into the connection (handle_event, handle_timeout,
    // poll_transmit, streams(), poll(), poll_endpoint_events) reaches the
    // current state through this accessor, so the flag is set on the same
    // tick the connection transitions into Established.
    if !self.established_at_least_once && !self.conn.is_handshaking() && !self.conn.is_closed() {
      self.established_at_least_once = true;
    }
    &mut self.conn
  }

  #[inline(always)]
  pub(crate) fn conn_ref(&self) -> &Connection {
    &self.conn
  }

  /// Whether this connection has been observed Established at least once. The
  /// sticky flag set lazily by [`Self::conn_mut`]; an immutable read that does
  /// NOT itself force the observation (the caller reads the value as of the last
  /// mutable touch). The datagram-servicing path samples it before and after
  /// [`super::QuicEndpoint::service_one_conn`] to detect the establishment
  /// transition that unblocks a pooled dial.
  #[inline(always)]
  pub(crate) fn established_at_least_once(&self) -> bool {
    self.established_at_least_once
  }

  pub(crate) fn peer(&self) -> SocketAddr {
    self.peer
  }

  /// Drain every `ConnectionEvent` queued for this connection by a PREVIOUS
  /// `service_quinn` iteration. Called at the start of each per-connection
  /// iteration to apply the one-tick-deferred feedback from
  /// `quinn_proto::Endpoint::handle_event` — see [`Self::queue_pending_event`]
  /// for the queuing side and the rationale.
  pub(crate) fn take_pending_events(
    &mut self,
  ) -> std::collections::vec_deque::Drain<'_, ConnectionEvent> {
    self.pending_events.drain(..)
  }

  /// Queue a `ConnectionEvent` produced by `quinn.handle_event(ch, ev)` for
  /// delivery on the NEXT `service_quinn` iteration of this connection.
  ///
  /// Mirrors quinn-proto's reference async driver's channel-based feedback
  /// pattern: `forward_endpoint_events` sends `EndpointEvent`s on a channel,
  /// `process_conn_events` receives the resulting `ConnectionEvent`s on the
  /// connection's NEXT scheduling iteration; the endpoint side calls
  /// `Endpoint::handle_event(ch, ev)` and forwards the returned
  /// `ConnectionEvent` via per-connection senders. Same-tick feedback
  /// would interleave NEW_CONNECTION_ID frames with in-flight stream data
  /// in the same packet build and starve concurrent reliable exchanges on
  /// small per-stream receive windows.
  ///
  /// Co-located with the connection: when `ConnTable::reap_if_drained`
  /// removes this entry, the deferred queue drops with it. The bare
  /// `ConnectionHandle` cannot be re-keyed onto a different connection by
  /// quinn's slab `vacant_key()` reuse because there is no global queue
  /// to re-key from.
  pub(crate) fn queue_pending_event(&mut self, ev: ConnectionEvent) {
    self.pending_events.push_back(ev);
  }

  /// `true` iff this connection has deferred `ConnectionEvent` work
  /// queued by a previous `service_quinn` iteration that has not yet
  /// been drained. The composed coordinator's `poll_timeout` surfaces
  /// `Some(last_now)` (immediate-due) whenever any `ConnEntry`
  /// satisfies this — without that wake, a strict-poll driver would
  /// sleep until an unrelated timer fires and the deferred CID
  /// lifecycle (`NewIdentifiers` → NEW_CONNECTION_ID emission) would
  /// stall.
  pub(crate) fn has_pending_events(&self) -> bool {
    !self.pending_events.is_empty()
  }

  /// Number of `ConnectionEvent`s currently queued for delivery on this
  /// connection's next `service_quinn` iteration. Observation-only,
  /// used by the architectural unit test that asserts the deferred
  /// queue drops with the entry on reap.
  #[cfg(test)]
  pub(crate) fn pending_events_len(&self) -> usize {
    self.pending_events.len()
  }

  /// Live count of DIALER (we-opened, `eager_outbound_label`) bidi bridges on
  /// this connection — the outbound half of quinn's `connection_blocked` set the
  /// coordinator's `open(Dir::Bi)` admission gate bounds. See the field docs.
  #[inline(always)]
  pub(crate) fn outbound_bridge_count(&self) -> usize {
    self.outbound_bridge_count
  }

  /// Record one newly minted dialer bridge on this connection. Paired 1:1 with
  /// [`Self::dec_outbound_bridge_count`] under the coordinator's
  /// `eager_outbound_label` predicate, so every increment is matched by exactly
  /// one decrement (or discarded whole with the entry on connection reap).
  #[inline(always)]
  pub(crate) fn inc_outbound_bridge_count(&mut self) {
    self.outbound_bridge_count += 1;
  }

  /// Release one reaped dialer bridge's unit of this connection's outbound
  /// count. Debug-asserts against underflow: a decrement with no matching mint
  /// is a predicate mismatch between the mint and reap sites (the exact fault an
  /// `eager_outbound_label`-vs-`pending_outbound_kinds` keying error would cause).
  #[inline(always)]
  pub(crate) fn dec_outbound_bridge_count(&mut self) {
    debug_assert!(
      self.outbound_bridge_count > 0,
      "outbound_bridge_count underflow: a dialer-bridge reap has no matching mint"
    );
    self.outbound_bridge_count = self.outbound_bridge_count.saturating_sub(1);
  }
}

/// Whether a selection is servicing the RELIABLE plane (push/pull, reliable-ping,
/// user message — an intent that may create a fresh outbound connection and is
/// retired through `dial_failed` on terminal failure) or the UNRELIABLE plane
/// (a best-effort gossip/probe datagram that never surfaces a dial error as a
/// membership signal and never creates a companion connection beside a live
/// handshaking inbound). The discriminant the two selection tables branch on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Reliability {
  /// Reliable-plane intent — the reliable selection table (may dial fresh; a
  /// terminal outbound is authoritative and routes to `dial_failed`).
  Reliable,
  /// Unreliable-plane datagram — the unreliable selection table (every dial
  /// failure degrades to a best-effort UDP fallback; no companion dial beside a
  /// live handshaking inbound).
  Unreliable,
}

/// The at-most-two tracked handles for one logical peer. `outbound` is our most
/// recent self-initiated dial; `inbound` is the peer's most recent accepted
/// connection. Direction is the discriminant the terminality rule is asymmetric
/// on — a closed-never-established outbound is our own failed dial (authoritative
/// unreachability on our egress), while a closed-never-established inbound is the
/// peer's failed dial toward us (says nothing about our egress) — so it is kept
/// structural, one field per direction. A `PeerRoute` with both fields `None` is
/// dropped from the map; the connection slab may still hold further entries for
/// the same peer (draining residue, a superseded same-direction accept) — all
/// serviced by the coordinator, none selectable here.
#[derive(Clone, Copy, Default)]
pub(crate) struct PeerRoute {
  outbound: Option<ConnectionHandle>,
  inbound: Option<ConnectionHandle>,
}

/// Per-handle classification computed inline from the slab state — NOT a stored
/// field, so it is always current. `closed` is `Connection::is_closed()`
/// (`Closed | Draining | Drained`). The direction split of the two
/// closed-never-established classes is the asymmetric-reachability fix: our own
/// failed dial ([`HandleClass::TerminalOutbound`]) proves the peer is
/// unreachable on our egress and is authoritative (anti-storm); the peer's
/// failed dial toward us ([`HandleClass::DeadInbound`]) proves nothing about our
/// egress and is never returned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum HandleClass {
  /// `!closed && !is_handshaking` — usable, ready to mint.
  Established,
  /// `!closed && is_handshaking` — usable, repark.
  Handshaking,
  /// `closed && established_at_least_once` — drain window, redial.
  DrainClosed,
  /// `closed && !established && direction == Outbound` — authoritative failure.
  TerminalOutbound,
  /// `closed && !established && direction == Inbound` — non-authoritative, prune.
  DeadInbound,
  /// The slab slot is gone (race-defensive).
  Gone,
}

pub(crate) struct ConnTable {
  conns: Slab<ConnEntry>,
  /// The at-most-two tracked handles per logical peer — the self-initiated
  /// `outbound` and the peer-accepted `inbound` — that [`Self::get_or_dial`]
  /// selects among. Replaces a single direction-blind canonical pointer: a
  /// peer-initiated inbound still handshaking can no longer occupy the slot a
  /// reliable outbound intent selects, so an inbound that stalls or fails does
  /// not suppress our own independent outbound dial to a peer reachable on our
  /// egress. An entry whose both fields fall to `None` is removed.
  peer_routes: HashMap<SocketAddr, PeerRoute>,
  /// Per-normalized-source count of INBOUND connections that have not yet
  /// established — the half-open population the coordinator's per-source pending
  /// cap bounds. Keyed on the [`SourceKey`] derived under [`Self::source_prefix`]
  /// (a de-ported, prefix-masked source IP) rather than the full `SocketAddr`,
  /// so a host cannot bypass its per-source allowance by rotating its UDP source
  /// port — every port of one source folds to one key. Kept as an index so
  /// admission is an O(1) lookup ([`Self::pending_inbound_from`]) instead of a
  /// scan of the whole connection slab: at connection-table saturation an
  /// attacker flooding fresh-DCID Initials would otherwise force an O(total
  /// connections) scan per rejected datagram. A key with zero pending inbound
  /// connections has no entry (the map never stores a zero).
  pending_inbound: HashMap<SourceKey, usize>,
  /// The prefix every `pending_inbound` key is normalized under.
  ///
  /// **Load-bearing immutable invariant.** Set once from validated config at
  /// construction and NEVER mutated (there is deliberately no setter). A pending
  /// unit is charged under `source_key(source_prefix, peer)` at accept and
  /// released under the SAME derivation at establishment or reap; a runtime
  /// change with entries outstanding would derive a different key on release
  /// than on charge, leaking the old bucket and underflowing the new one.
  source_prefix: SourcePrefix,
  /// Running total of every `pending_inbound` count — the coordinator-wide
  /// half-open inbound population the dedicated budget bounds. Maintained as a
  /// scalar so that budget check is an O(1) compare ([`Self::pending_total`])
  /// rather than a sum over the map. Moved in lockstep with `pending_inbound`
  /// at the single increment site ([`Self::insert_accepted`]) and inside the
  /// single decrement primitive ([`Self::release_pending_inbound`]), so it
  /// always equals `Σ pending_inbound.values()`.
  pending_total: usize,
}

impl ConnTable {
  pub(crate) fn new(source_prefix: SourcePrefix) -> Self {
    Self {
      conns: Slab::new(),
      peer_routes: HashMap::new(),
      pending_inbound: HashMap::new(),
      source_prefix,
      pending_total: 0,
    }
  }

  pub(crate) fn get_mut(&mut self, ch: ConnectionHandle) -> Option<&mut ConnEntry> {
    self.conns.get_mut(ch.0)
  }

  /// Immutable connection-entry lookup, for observation-only accessors that
  /// must inspect connection state (e.g. `is_drained()`) without a `&mut`.
  pub(crate) fn get(&self, ch: ConnectionHandle) -> Option<&ConnEntry> {
    self.conns.get(ch.0)
  }

  /// Classify one tracked handle from its current slab state. See
  /// [`HandleClass`]; direction resolves the two closed-never-established
  /// classes.
  fn classify(&self, ch: ConnectionHandle) -> HandleClass {
    match self.conns.get(ch.0) {
      None => HandleClass::Gone,
      Some(e) => {
        if !e.conn.is_closed() {
          if e.conn.is_handshaking() {
            HandleClass::Handshaking
          } else {
            HandleClass::Established
          }
        } else if e.established_at_least_once {
          HandleClass::DrainClosed
        } else if e.direction == ConnDirection::Outbound {
          HandleClass::TerminalOutbound
        } else {
          HandleClass::DeadInbound
        }
      }
    }
  }

  /// The best usable handle for `peer` — the derived selection an observer
  /// (`live_connections_to`, `try_open_uni_stream_to`, datagram sizing) rides,
  /// with NO dial side effect. Prefers an established connection, then a
  /// handshaking one; outbound before inbound within each class. A closed/terminal
  /// handle is never usable, so this returns `None` for a peer whose only
  /// tracked connections are draining or handshake-failed. At most one handle
  /// (0 or 1 per peer).
  pub(crate) fn handle_for(&self, peer: &SocketAddr) -> Option<ConnectionHandle> {
    let route = self.peer_routes.get(peer)?;
    let candidates = [route.outbound, route.inbound];
    for want in [HandleClass::Established, HandleClass::Handshaking] {
      for ch in candidates.into_iter().flatten() {
        if self.classify(ch) == want {
          return Some(ch);
        }
      }
    }
    None
  }

  /// Debug tripwire for the logical-immutable identity contract: a connection's
  /// `peer` is the key its route is filed under, so no `peer_routes` key OTHER
  /// than `ch`'s own peer may reference `ch`. A future change that let `peer`
  /// follow QUIC path migration would file the route under a stale key while
  /// `peer()` returns the migrated address, stranding peer-keyed parked dials;
  /// this catches that divergence. Debug-only (the scan compiles out in release).
  pub(crate) fn debug_assert_peer_is_route_key(&self, ch: ConnectionHandle) {
    debug_assert!(
      {
        match self.conns.get(ch.0) {
          None => true,
          Some(e) => !self
            .peer_routes
            .iter()
            .any(|(k, r)| *k != e.peer && (r.outbound == Some(ch) || r.inbound == Some(ch))),
        }
      },
      "connection {ch:?} is referenced by a peer_routes key other than its own peer"
    );
  }

  /// Pre-pass over `peer`'s route, applied before either selection table.
  ///
  /// **P1 — prune a closed inbound.** A `DeadInbound` (closed, never
  /// established — the peer's failed dial toward us) or `DrainClosed`
  /// (established-then-closed) inbound, or one whose slab slot is gone, is
  /// cleared from `route.inbound`; the slab entry is kept for its
  /// handle-equality reap. Past P1, `route.inbound` only ever carries a live
  /// (Established/Handshaking) inbound.
  ///
  /// **P2 — clear the outbound drain window.** A `DrainClosed`
  /// (established-then-closed) outbound, or one whose slab slot is gone, is
  /// cleared from `route.outbound` so a fresh redial can proceed. A
  /// `TerminalOutbound` (closed, never established — our own failed dial) is
  /// LEFT in place: it is authoritative unreachability on our egress and
  /// suppresses a fresh-handshake storm. Past P2,
  /// `route.outbound ∈ {None, Handshaking, Established, TerminalOutbound}`.
  ///
  /// A route left with both fields `None` is removed (no permanent empty
  /// entries).
  fn prepass(&mut self, peer: &SocketAddr) {
    let Some(route) = self.peer_routes.get(peer) else {
      return;
    };
    let clear_inbound = match route.inbound {
      None => false,
      // Any closed (or slab-gone) inbound is pruned; only a live inbound may
      // survive P1. `is_closed()` covers `DeadInbound` and `DrainClosed`.
      Some(ch) => self.conns.get(ch.0).is_none_or(|e| e.conn.is_closed()),
    };
    let clear_outbound = match route.outbound {
      None => false,
      // Only a `DrainClosed` (or slab-gone) outbound is cleared; a
      // `TerminalOutbound` stays authoritative.
      Some(ch) => self
        .conns
        .get(ch.0)
        .is_none_or(|e| e.conn.is_closed() && e.established_at_least_once),
    };
    if clear_inbound || clear_outbound {
      let route = self
        .peer_routes
        .get_mut(peer)
        .expect("route present for the peer just inspected");
      if clear_inbound {
        route.inbound = None;
      }
      if clear_outbound {
        route.outbound = None;
      }
    }
    if self
      .peer_routes
      .get(peer)
      .is_some_and(|r| r.outbound.is_none() && r.inbound.is_none())
    {
      self.peer_routes.remove(peer);
    }
  }

  /// Commit a fresh self-initiated outbound `quinn.connect` and record it as
  /// `route.outbound`. Enforces the global connection cap right before the new
  /// slab slot commits (the only site a NEW connection is created), so a large
  /// or attacker-influenced membership set cannot grow the slab past the cap
  /// through reliable dials and datagram fallbacks. Reached only when
  /// `route.outbound` is `None` after the pre-pass.
  fn dial_fresh(
    &mut self,
    quinn: &mut QuinnEndpoint,
    now: Instant,
    client: quinn_proto::ClientConfig,
    peer: SocketAddr,
    server_name: &str,
    max_connections: Option<usize>,
  ) -> Result<ConnectionHandle, DialError> {
    // `len()` counts every tracked slab entry (handshaking, established,
    // draining), so this caps the total outbound + inbound population.
    if max_connections.is_some_and(|max| self.conns.len() >= max) {
      return Err(DialError::AtGlobalCap);
    }
    // `server_name` is the rustls verification identity for the peer's cert,
    // supplied by the driver's SNI provider so the name is keyed on the
    // membership address. quinn-proto forwards it verbatim to
    // `config.crypto.start_session`; the rustls-backed `start_session` parses
    // it via `ServerName::try_from` and feeds the configured verifier.
    let (ch, conn) = quinn
      .connect(now.into_std(), client, peer, server_name)
      .map_err(DialError::Connect)?;
    let slot = self.conns.insert(ConnEntry {
      conn,
      peer,
      direction: ConnDirection::Outbound,
      established_at_least_once: false,
      // A local outbound dial is never counted against the peer's inbound
      // pending allowance (see `pending_inbound_from`), so it is not indexed.
      pending_indexed: false,
      pending_events: VecDeque::new(),
      outbound_bridge_count: 0,
    });
    debug_assert_eq!(slot, ch.0, "quinn ConnectionHandle is the slab vacant_key");
    self.peer_routes.entry(peer).or_default().outbound = Some(ch);
    Ok(ch)
  }

  /// Select the connection handle for an intent to `peer`, dialing a fresh
  /// outbound when the selection tables call for one.
  ///
  /// quinn-proto assigns `ConnectionHandle(slab.vacant_key())` inside
  /// `Endpoint::connect`, so the slab index and the handle's inner value are
  /// always in sync (asserted at insert time).
  ///
  /// The selection runs the [`Self::prepass`], classifies the surviving
  /// `route.outbound` (`O`) and `route.inbound` (`I`), then applies the table
  /// for `reliability`:
  ///
  /// **Reliable** (first matching row): R1 `O` Established → `O`; R2 `I`
  /// Established → `I`; R3 `O` Handshaking → `O`; R4 `O` TerminalOutbound + `I`
  /// Handshaking → `I`; R5 `O` TerminalOutbound + no `I` → `O` (downstream
  /// `dial_failed`, anti-storm); R6 no `O` + `I` Handshaking → dial fresh under
  /// cap, else degrade to `I` (a synchronous connect error degrades identically
  /// — return the live inbound and repark, never propagate); R7 no `O`, no `I`
  /// → dial fresh under cap, else propagate the dial error.
  ///
  /// **Unreliable** (first matching row): U1 `O` Established → `O`; U2 `I`
  /// Established → `I`; U3 `O` Handshaking → `O`; U4 `I` Handshaking → `I` (no
  /// companion dial); U5 `O` TerminalOutbound + no `I` → `O`; U6 no `O`, no `I`
  /// → cold-dial under cap, else the dial error (the datagram caller maps every
  /// dial error to a best-effort UDP fallback).
  ///
  /// **Fresh-dial condition** (the asymmetric-reachability core): a
  /// `quinn.connect` is issued iff `route.outbound` is `None` after the pre-pass
  /// (R6/R7/U6) — never for a Handshaking/Established/TerminalOutbound outbound,
  /// and never keyed on `route.inbound`. So a peer's stalled or failed inbound
  /// never suppresses our own independent outbound dial to a peer reachable on
  /// our egress.
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn get_or_dial(
    &mut self,
    quinn: &mut QuinnEndpoint,
    now: Instant,
    client: quinn_proto::ClientConfig,
    peer: SocketAddr,
    server_name: &str,
    max_connections: Option<usize>,
    reliability: Reliability,
  ) -> Result<ConnectionHandle, DialError> {
    use HandleClass::{Established, Handshaking, TerminalOutbound};
    self.prepass(&peer);
    let (outbound, inbound) = match self.peer_routes.get(&peer) {
      Some(r) => (r.outbound, r.inbound),
      None => (None, None),
    };
    let oc = outbound.map(|ch| self.classify(ch));
    let ic = inbound.map(|ch| self.classify(ch));
    match reliability {
      Reliability::Reliable => {
        if oc == Some(Established) {
          return Ok(outbound.expect("Established outbound handle present")); // R1
        }
        if ic == Some(Established) {
          return Ok(inbound.expect("Established inbound handle present")); // R2
        }
        if oc == Some(Handshaking) {
          return Ok(outbound.expect("Handshaking outbound handle present")); // R3
        }
        if oc == Some(TerminalOutbound) {
          if ic == Some(Handshaking) {
            // R4: our dial failed but the peer's inbound may still establish.
            return Ok(inbound.expect("Handshaking inbound handle present"));
          }
          debug_assert!(
            ic.is_none(),
            "reliable R5 reached with a live inbound the pre-pass should have surfaced earlier"
          );
          // R5: authoritative terminal — downstream `dial_failed` (anti-storm).
          return Ok(outbound.expect("TerminalOutbound handle present"));
        }
        debug_assert!(
          oc.is_none(),
          "reliable selection reached the fresh-dial rows with a non-None outbound class"
        );
        if ic == Some(Handshaking) {
          // R6: dial our own outbound under cap; at cap OR on a synchronous
          // connect error, degrade to the live inbound and repark (never
          // propagate the dial error — a live candidate exists).
          return match self.dial_fresh(quinn, now, client, peer, server_name, max_connections) {
            Ok(ch) => Ok(ch),
            Err(_) => Ok(inbound.expect("Handshaking inbound handle present")),
          };
        }
        // R7: no candidate of either direction — propagate the dial error.
        debug_assert!(ic.is_none(), "reliable R7 reached with a live inbound");
        self.dial_fresh(quinn, now, client, peer, server_name, max_connections)
      }
      Reliability::Unreliable => {
        if oc == Some(Established) {
          return Ok(outbound.expect("Established outbound handle present")); // U1
        }
        if ic == Some(Established) {
          return Ok(inbound.expect("Established inbound handle present")); // U2
        }
        if oc == Some(Handshaking) {
          return Ok(outbound.expect("Handshaking outbound handle present")); // U3
        }
        if ic == Some(Handshaking) {
          // U4: ride the live handshaking inbound (→ NotReady → UDP); NEVER
          // create a companion outbound beside a live inbound.
          return Ok(inbound.expect("Handshaking inbound handle present"));
        }
        if oc == Some(TerminalOutbound) {
          debug_assert!(ic.is_none(), "unreliable U5 reached with a live inbound");
          // U5: (→ NotReady → UDP).
          return Ok(outbound.expect("TerminalOutbound handle present"));
        }
        // U6: no candidate — cold-dial under cap; the datagram caller maps a
        // dial error (at cap / connect error) to a best-effort UDP fallback.
        debug_assert!(
          oc.is_none() && ic.is_none(),
          "unreliable U6 reached with a live handle"
        );
        self.dial_fresh(quinn, now, client, peer, server_name, max_connections)
      }
    }
  }

  /// Establishment-chokepoint promotion. Called the pass a connection's sticky
  /// `established_at_least_once` flips `false → true`. A newly-established
  /// INBOUND becomes `route.inbound` unconditionally: completing the handshake is
  /// the strongest liveness signal available at the transport layer, so the most
  /// recently established inbound is the one selection should ride. Touches only
  /// `.inbound` (never displaces a self-initiated outbound). A newly-established
  /// OUTBOUND must already be its peer's tracked `route.outbound` (an
  /// establishing outbound is `!closed`, so the pre-pass never cleared it) —
  /// asserted, not written.
  ///
  /// The transport layer does NOT attempt to order connection instance-epochs
  /// (see the module-level residual note): establishment proves an Initial +
  /// Finished were exchanged, not that the remote instance is currently alive, so
  /// a zombie can capture selection until the idle timeout reaps it. This is
  /// bounded and safety-preserving; the resolving signal is the membership
  /// incarnation at the SWIM layer.
  pub(crate) fn on_established_transition(&mut self, ch: ConnectionHandle) {
    let (peer, direction) = match self.conns.get(ch.0) {
      Some(e) => (e.peer, e.direction),
      None => return,
    };
    match direction {
      ConnDirection::Inbound => {
        self.peer_routes.entry(peer).or_default().inbound = Some(ch);
      }
      ConnDirection::Outbound => {
        debug_assert!(
          self.peer_routes.get(&peer).and_then(|r| r.outbound) == Some(ch),
          "an establishing outbound must already be its peer's tracked outbound route"
        );
      }
    }
  }

  /// Record an inbound connection accepted by the server endpoint.
  ///
  /// The accepted connection is always inserted into the slab (so quinn can
  /// drive it and its inbound bidi streams are serviced). It is written into
  /// `route.inbound` only when the current inbound slot is empty or holds a
  /// closed/gone connection — an accept never displaces a live (handshaking or
  /// established) tracked inbound.
  ///
  /// The rule: an accept proves only that an Initial datagram arrived, which a
  /// delayed pre-crash Initial can also produce; it does NOT prove the remote
  /// instance is currently alive. Route displacement between competing inbounds
  /// is therefore gated on the establishment chokepoint
  /// ([`Self::on_established_transition`]), which promotes the most recently
  /// established inbound — not at accept time, where a delayed pre-crash Initial
  /// (structurally incapable of completing its handshake) would otherwise
  /// displace a live route. Accept-time writes fill only an empty or closed slot.
  ///
  /// NEVER written into `route.outbound`: an accept can therefore never displace
  /// a self-initiated outbound, else the peer-initiated connection would become
  /// the slot our outbound exchanges select while the peer binds its accept side
  /// to the other connection, wedging the exchange (the peer wrongly never
  /// confirmed → false Suspect). Keeping the two directions in separate fields
  /// makes that wedge-avoidance structural.
  ///
  /// The displaced (previous) inbound, if any, stays in the slab until its
  /// drained-reap; its handle is no longer in `route.inbound`, so the
  /// equality-guarded [`Self::reap_if_drained`] cannot clobber this one.
  pub(crate) fn insert_accepted(
    &mut self,
    ch: ConnectionHandle,
    conn: Connection,
    peer: SocketAddr,
  ) {
    let slot = self.conns.insert(ConnEntry {
      conn,
      peer,
      direction: ConnDirection::Inbound,
      established_at_least_once: false,
      // A freshly accepted inbound connection is un-established, so it enters
      // the per-source pending index. Paired with the decrement in
      // `reconcile_pending_inbound` (on establishment) or `reap_if_drained`
      // (on removal while still un-established).
      pending_indexed: true,
      pending_events: VecDeque::new(),
      // An inbound (server-accepted) connection can still be the canonical
      // handle a later local dial rides (simultaneous bidirectional dial), so a
      // dialer bridge may be opened on it — start the outbound count at zero.
      outbound_bridge_count: 0,
    });
    assert_eq!(
      slot, ch.0,
      "accepted connection slab slot must equal ConnectionHandle"
    );
    *self
      .pending_inbound
      .entry(source_key(self.source_prefix, &peer))
      .or_insert(0) += 1;
    self.pending_total += 1;
    // Fill only an empty or closed inbound slot; never displace a live inbound
    // at accept (that decision belongs to the establishment chokepoint). Never
    // touch `route.outbound`.
    let replace = match self.peer_routes.get(&peer).and_then(|r| r.inbound) {
      None => true,
      Some(cur) => self.conns.get(cur.0).is_none_or(|e| e.conn.is_closed()),
    };
    if replace {
      self.peer_routes.entry(peer).or_default().inbound = Some(ch);
    }
  }

  /// Drained-reap: if `Connection::is_drained()`, relay `EndpointEvent::drained()`
  /// to quinn (cleans its CID index) then drop the slab entry and clear it from
  /// its peer's route.
  ///
  /// Returns `true` if the connection was reaped.
  ///
  /// This mirrors the protocol quinn's own runtime uses: once
  /// `Connection::is_drained()` the runtime sends `EndpointEvent::drained()`
  /// (via `Endpoint::handle_event`) and then frees the connection.
  ///
  /// **Handle-equality-guarded route removal.** A superseding write may have
  /// already repointed `route.outbound`/`route.inbound` at a fresh handle (a
  /// drain-window redial, a newest-established-inbound promotion) while the slab
  /// still holds the OLD entry awaiting this reap. Clearing a field
  /// unconditionally would clobber the newer handle, so each field is cleared
  /// only when it still equals the handle being reaped. A `PeerRoute` left with
  /// both fields `None` is dropped.
  ///
  /// Contract: once this returns `true` the slab slot is gone — the caller must not forward any further `poll_endpoint_events()` for `ch` into the endpoint (that would double-drain a removed handle).
  pub(crate) fn reap_if_drained(
    &mut self,
    quinn: &mut QuinnEndpoint,
    ch: ConnectionHandle,
  ) -> bool {
    let drained = match self.conns.get(ch.0) {
      Some(e) => e.conn.is_drained(),
      None => return false,
    };
    if !drained {
      return false;
    }
    // Notify quinn's endpoint so it can retire the connection's CID entries.
    // Ignoring the returned `Option<ConnectionEvent>`: a `drained()` event is
    // a terminal endpoint-level retire — quinn returns `None` here in
    // practice and no further connection-level work is owed.
    let _ = quinn.handle_event(ch, quinn_proto::EndpointEvent::drained());
    if let Some(e) = self.conns.try_remove(ch.0) {
      // A still-indexed entry is an inbound connection removed before it ever
      // established: release its unit of the source's pending allowance. An
      // entry that established already had its unit released by
      // `reconcile_pending_inbound`, so `pending_indexed` is false and this is
      // skipped — the decrement happens exactly once.
      if e.pending_indexed {
        // Read the `Copy` prefix out before the `&mut self.pending_inbound`
        // borrow; `SourceKey` is `Copy`, so there is no borrow friction.
        let k = source_key(self.source_prefix, &e.peer);
        Self::release_pending_inbound(&mut self.pending_inbound, &mut self.pending_total, k);
      }
      if let Some(route) = self.peer_routes.get_mut(&e.peer) {
        if route.outbound == Some(ch) {
          route.outbound = None;
        }
        if route.inbound == Some(ch) {
          route.inbound = None;
        }
      }
      if self
        .peer_routes
        .get(&e.peer)
        .is_some_and(|r| r.outbound.is_none() && r.inbound.is_none())
      {
        self.peer_routes.remove(&e.peer);
      }
    }
    true
  }

  /// Release one unit of `key`'s pending-inbound index, moving `pending_total`
  /// in lockstep. The single decrement primitive: every caller has already
  /// confirmed the entry was indexed (`pending_indexed`), so the key MUST have a
  /// positive count here. Removes the map entry at zero so an idle source leaves
  /// no residue. The coordinator-wide `pending_total` is decremented only inside
  /// the `Some` arm — i.e. only when a key unit is actually released — so it
  /// stays equal to `Σ pending_inbound.values()`.
  fn release_pending_inbound(
    pending_inbound: &mut HashMap<SourceKey, usize>,
    pending_total: &mut usize,
    key: SourceKey,
  ) {
    match pending_inbound.get_mut(&key) {
      Some(count) => {
        debug_assert!(*count > 0, "pending-inbound index underflow for {key:?}");
        *count -= 1;
        if *count == 0 {
          pending_inbound.remove(&key);
        }
        debug_assert!(*pending_total > 0, "pending-inbound total underflow");
        *pending_total -= 1;
      }
      None => debug_assert!(
        false,
        "pending-inbound index missing an indexed entry for {key:?}"
      ),
    }
  }

  /// Observe whether an inbound connection has established and, if so, release
  /// its unit of the per-source pending index exactly once. Called once per
  /// connection per servicing pass from `service_quinn`: a still-handshaking or
  /// never-indexed (outbound / already-released) entry is a no-op, so the index
  /// tracks the live half-open population without a scan.
  ///
  /// Establishment is authoritative via the sticky `established_at_least_once`
  /// flag (set the first tick the connection is observed
  /// `!is_handshaking() && !is_closed()`); this forces that observation current
  /// before reading it, so an establishment reached earlier in the same tick is
  /// caught here rather than lingering in the index until the next pass.
  pub(crate) fn reconcile_pending_inbound(&mut self, ch: ConnectionHandle) {
    let released_peer = {
      let Some(entry) = self.conns.get_mut(ch.0) else {
        return;
      };
      if !entry.pending_indexed {
        return;
      }
      // Only inbound connections ever enter the pending index (`insert_accepted`
      // is the sole increment site); an outbound dial is never charged.
      debug_assert_eq!(
        entry.direction,
        ConnDirection::Inbound,
        "only inbound connections may be pending-indexed"
      );
      // `conn_mut()` performs the sticky establishment observation.
      let _ = entry.conn_mut();
      if !entry.established_at_least_once {
        return;
      }
      entry.pending_indexed = false;
      entry.peer
    };
    // Derive the key after the `entry` borrow ends; `self.source_prefix` is
    // `Copy`, so reading it does not conflict with the `&mut` borrows below.
    let k = source_key(self.source_prefix, &released_peer);
    Self::release_pending_inbound(&mut self.pending_inbound, &mut self.pending_total, k);
  }

  /// Total number of connections currently tracked — every slab entry,
  /// counting handshaking, established, and still-draining connections alike.
  /// The global QUIC connection cap is enforced against this before an inbound
  /// Initial commits new state.
  pub(crate) fn len(&self) -> usize {
    self.conns.len()
  }

  /// Number of inbound (server-accepted) connections whose NORMALIZED source key
  /// (see [`Self::source_prefix`]) matches `source`'s, that have not yet
  /// established — still handshaking, OR handshake-failed and awaiting their
  /// drained-reap. This is the half-open state the per-source pending cap bounds
  /// before an unauthenticated inbound Initial commits new state. Because the
  /// lookup is keyed on the normalized source (de-ported, prefix-masked), every
  /// UDP source port from one host — and, at a shorter prefix, one subnet —
  /// counts against a single allowance, so port rotation cannot bypass the cap.
  ///
  /// Two properties the cap depends on:
  ///
  /// - Direction: a LOCAL outbound dial to `source` is NOT counted. Counting it
  ///   would wedge simultaneous bidirectional dialing — each side's own
  ///   handshaking outbound to the peer would block the peer's inbound Initial,
  ///   so neither exchange completes.
  /// - Never-established: a failed inbound handshake becomes non-handshaking but
  ///   its closed/draining slab entry lingers until [`Self::reap_if_drained`]
  ///   frees it. It stays charged here (still `pending_indexed`) so a source
  ///   cannot exceed the cap by repeatedly opening handshakes that fail — its
  ///   closed-but-undrained entries still consume the allowance. An inbound
  ///   connection that DID establish has graduated out of the half-open budget
  ///   (its unit was released by [`Self::reconcile_pending_inbound`]) and is not
  ///   counted.
  ///
  /// O(1): a direct read of the [`Self::pending_inbound`] index, not a scan of
  /// the connection slab — so a rejected inbound Initial at connection-table
  /// saturation cannot be amplified into O(total connections) work.
  pub(crate) fn pending_inbound_from(&self, source: &SocketAddr) -> usize {
    self
      .pending_inbound
      .get(&source_key(self.source_prefix, source))
      .copied()
      .unwrap_or(0)
  }

  /// The coordinator-wide count of inbound connections that have not yet
  /// established — `Σ pending_inbound.values()`, maintained as a scalar so the
  /// dedicated half-open budget is an O(1) admission compare. Summed across
  /// every normalized source, this is the population a subnet flood would grow
  /// to starve established/outbound work were it unbounded.
  pub(crate) fn pending_total(&self) -> usize {
    self.pending_total
  }

  /// Inbound (server-accepted) connections from `source` that have left the
  /// handshaking phase WITHOUT ever establishing — the failed/closed-but-
  /// undrained population [`Self::pending_inbound_from`] must keep charged
  /// (the state an `is_handshaking()`-only count would wrongly free). Distinct
  /// from still-handshaking entries; used by the regression test that a failed
  /// inbound handshake does not release the source's allowance before its reap.
  #[cfg(test)]
  pub(crate) fn failed_never_established_inbound_from(&self, source: &SocketAddr) -> usize {
    self
      .conns
      .iter()
      .filter(|(_, e)| {
        source_key(self.source_prefix, &e.peer) == source_key(self.source_prefix, source)
          && e.direction == ConnDirection::Inbound
          && !e.established_at_least_once
          && !e.conn.is_handshaking()
      })
      .count()
  }

  /// Independent recount of the connections currently indexed under `source`'s
  /// NORMALIZED key (`pending_indexed`), read straight from the slab rather than
  /// the index — matching by [`source_key`] so every port that aggregates into
  /// one bucket is recounted together. The index invariant is
  /// `pending_inbound_from == this recount` for every source at all times: a
  /// missed increment/decrement or a decrement that did not clear the bit makes
  /// the two diverge, so the counter-maintenance test asserts their equality
  /// after every accept / establish / reap to catch a leak, underflow, or
  /// double-count. An un-normalized recount here would falsely break the
  /// invariant the moment one key aggregates multiple source ports.
  #[cfg(test)]
  pub(crate) fn indexed_inbound_recount(&self, source: &SocketAddr) -> usize {
    self
      .conns
      .iter()
      .filter(|(_, e)| {
        source_key(self.source_prefix, &e.peer) == source_key(self.source_prefix, source)
          && e.pending_indexed
      })
      .count()
  }

  /// Independent recount of `pending_total` — the sum of every `pending_inbound`
  /// value, read straight from the map rather than the maintained scalar. The
  /// budget invariant is `pending_total() == this` at all times; the exact-once
  /// test asserts equality after every step to catch a scalar that drifted from
  /// the map (a missed lockstep move).
  #[cfg(test)]
  pub(crate) fn pending_total_recount(&self) -> usize {
    self.pending_inbound.values().sum()
  }

  /// Count of INBOUND slab entries that have not established
  /// (`!established_at_least_once`), read straight from the slab. Every such
  /// entry is still charged (an un-established inbound stays `pending_indexed`
  /// until reaped, and establishment is the only non-reap release), so this must
  /// equal `pending_total()` at all times — the third face of the exact-once
  /// invariant.
  #[cfg(test)]
  pub(crate) fn unestablished_inbound_count(&self) -> usize {
    self
      .conns
      .iter()
      .filter(|(_, e)| e.direction == ConnDirection::Inbound && !e.established_at_least_once)
      .count()
  }

  /// Snapshot of all live connection handles, for driver polling loops.
  pub(crate) fn iter_handles(&self) -> MediumVec<ConnectionHandle> {
    self
      .conns
      .iter()
      .map(|(k, _)| ConnectionHandle(k))
      .collect()
  }
}

#[cfg(test)]
mod tests;
