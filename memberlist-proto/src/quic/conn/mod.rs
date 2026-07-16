//! Per-peer QUIC connection table. One long-lived `quinn_proto::Connection`
//! per peer (idle-evicted by quinn-proto's own `max_idle_timeout`); reaped
//! only when `Connection::is_drained()` (the same protocol quinn uses).

use crate::Instant;
use core::net::SocketAddr;
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
}

pub(crate) struct ConnTable {
  conns: Slab<ConnEntry>,
  peers: HashMap<SocketAddr, ConnectionHandle>,
  /// Per-source count of INBOUND connections that have not yet established — the
  /// half-open population the coordinator's per-source pending cap bounds. Kept
  /// as an index so admission is an O(1) lookup ([`Self::pending_inbound_from`])
  /// instead of a scan of the whole connection slab: at connection-table
  /// saturation an attacker flooding fresh-DCID Initials would otherwise force
  /// an O(total connections) scan per rejected datagram. A source with zero
  /// pending inbound connections has no entry (the map never stores a zero).
  pending_inbound: HashMap<SocketAddr, usize>,
}

impl ConnTable {
  pub(crate) fn new() -> Self {
    Self {
      conns: Slab::new(),
      peers: HashMap::new(),
      pending_inbound: HashMap::new(),
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

  pub(crate) fn handle_for(&self, peer: &SocketAddr) -> Option<ConnectionHandle> {
    self.peers.get(peer).copied()
  }

  /// Return the existing connection handle for `peer`, or dial a new one.
  ///
  /// quinn-proto assigns `ConnectionHandle(slab.vacant_key())` inside
  /// `Endpoint::connect`, so the slab index and the handle's inner value are
  /// always in sync (asserted at insert time).
  ///
  /// **Closed-before-drained pool window: redial, don't reuse.** A pooled
  /// `Connection` may sit in `Closed`/`Draining` for the 3×PTO drain window
  /// before it reaches `Drained`. During that window, quinn-proto's
  /// `Streams::open(Dir::Bi)` already returns `None` (it checks
  /// `self.conn_state.is_closed()`, and `Connection::is_closed()` covers
  /// `Closed | Draining | Drained`), but `Connection::is_handshaking`
  /// is *also* `false`, so the coordinator's `service_dials` handshaking-
  /// requeue path would not catch it; the intent would fall into
  /// `dial_failed` and the push/pull / reliable-ping-fallback / user message
  /// would be silently lost. We therefore probe the cached connection with
  /// `is_closed()` (the same predicate `Streams::open` uses to refuse) AND
  /// `established_at_least_once` (sticky — set the first time the conn was
  /// observed `!is_handshaking() && !is_closed()`): a closed conn that
  /// was previously Established is in the drain window and we redial (drop
  /// only the `peers` mapping; keep the slab entry so `reap_if_drained`
  /// completes the drained-reap on the original handle later; dial a fresh
  /// connection). A closed conn that never reached Established is a
  /// handshake failure for an unreachable peer — returning the cached
  /// handle lets `service_dials::open(Dir::Bi)=None && !is_handshaking()`
  /// fall through to `dial_failed`, so a genuinely-unreachable peer does
  /// not generate fresh handshake attempts inside one push/pull deadline. The slab momentarily holds two entries
  /// for the same peer (the old one awaiting drained-reap; the new one
  /// live); this is by design — `peers[peer]` always points at the live
  /// one for new outbound exchanges, and `reap_if_drained` clears the old
  /// slab slot when its `Drained` state is reached.
  pub(crate) fn get_or_dial(
    &mut self,
    quinn: &mut QuinnEndpoint,
    now: Instant,
    client: quinn_proto::ClientConfig,
    peer: SocketAddr,
    server_name: &str,
  ) -> Result<ConnectionHandle, quinn_proto::ConnectError> {
    if let Some(ch) = self.peers.get(&peer).copied() {
      let (reusable, was_established) = match self.conns.get(ch.0) {
        Some(e) => (!e.conn.is_closed(), e.established_at_least_once),
        None => (false, false),
      };
      if reusable {
        return Ok(ch);
      }
      // Cached conn is closed (or its slab slot is gone — race-defensive).
      // If it was previously Established, we are in the closed-before-
      // drained pool window: drop ONLY the `peers` mapping (keep the
      // slab so its in-flight drained-reap can complete on the original
      // handle) and fall through to dial a fresh connection. If it was
      // NEVER Established, treat the cached entry as authoritative —
      // returning it lets the existing `service_dials` path fire
      // `dial_failed` via `open(Bi)=None && !is_handshaking()`.
      if !was_established {
        return Ok(ch);
      }
      self.peers.remove(&peer);
    }
    // `server_name` is the rustls verification identity for the peer's
    // cert — supplied by the driver's SNI provider so the verification
    // name is keyed on the membership address (not a hardcoded constant).
    // quinn-proto forwards it verbatim to
    // `config.crypto.start_session(version, server_name, ...)`; the
    // rustls-backed `start_session` parses it via `ServerName::try_from`
    // and feeds the result to the configured `ServerCertVerifier`.
    let (ch, conn) = quinn.connect(now.into_std(), client, peer, server_name)?;
    let slot = self.conns.insert(ConnEntry {
      conn,
      peer,
      direction: ConnDirection::Outbound,
      established_at_least_once: false,
      // A local outbound dial is never counted against the peer's inbound
      // pending allowance (see `pending_inbound_from`), so it is not indexed.
      pending_indexed: false,
      pending_events: VecDeque::new(),
    });
    debug_assert_eq!(slot, ch.0, "quinn ConnectionHandle is the slab vacant_key");
    self.peers.insert(peer, ch);
    Ok(ch)
  }

  /// Record an inbound connection accepted by the server endpoint.
  ///
  /// The accepted connection is always inserted into the slab (so quinn can
  /// drive it and its inbound bidi streams are serviced). The `peers`
  /// address→handle map — which `get_or_dial` consults to route NEW
  /// OUTBOUND streams — is (re)pointed at this connection only if the
  /// existing canonical (if any) is no longer usable, using the **same
  /// `!Connection::is_closed()` predicate** `get_or_dial` uses for
  /// outbound reuse.
  ///
  /// Symmetry with `get_or_dial`. Outbound dial considers a cached
  /// connection re-usable iff `!is_closed()` (quinn-proto's
  /// `Connection::is_closed` covers `Closed|Draining|Drained`); a closed
  /// cached handle is treated as unreachable (`Streams::open(Bi)=None`).
  /// Inbound accept must use the *same* predicate: when the existing
  /// canonical is closed/draining/drained, the new accepted connection
  /// becomes canonical (`peers[peer] = new_ch`); the old slab entry
  /// persists until its drained-reap completes (its `peers` mapping is
  /// already gone, so the equality-guarded `reap_if_drained` cannot
  /// clobber the new canonical). Without this symmetry an accepted live
  /// connection from a peer can be hidden behind a closed-never-
  /// Established cached handle — subsequent outbound dials hit the
  /// closed cached handle and silently `dial_failed`, leaving the live
  /// accepted connection unused.
  ///
  /// Simultaneous bidirectional dial. When both peers `connect` to each
  /// other before either accepts, each side ends up with one
  /// self-initiated and one accepted connection to the same address. If
  /// the self-initiated one is healthy (`!is_closed()`) it stays
  /// canonical for this node's outbound exchanges — unconditionally
  /// clobbering `peers` would route outbound onto the peer-initiated
  /// connection while the peer's accept side is bound to the other one,
  /// wedging the exchange (the peer is then wrongly never confirmed →
  /// false Suspect). The `!is_closed()` gate preserves this property.
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
    });
    assert_eq!(
      slot, ch.0,
      "accepted connection slab slot must equal ConnectionHandle"
    );
    *self.pending_inbound.entry(peer).or_insert(0) += 1;
    let existing_usable = self
      .peers
      .get(&peer)
      .and_then(|existing| self.conns.get(existing.0))
      .is_some_and(|e| !e.conn.is_closed());
    if !existing_usable {
      self.peers.insert(peer, ch);
    }
  }

  /// Drained-reap: if `Connection::is_drained()`, relay `EndpointEvent::drained()`
  /// to quinn (cleans its CID index) then drop the slab + peers entry.
  ///
  /// Returns `true` if the connection was reaped.
  ///
  /// This mirrors the protocol quinn's own runtime uses: once
  /// `Connection::is_drained()` the runtime sends `EndpointEvent::drained()`
  /// (via `Endpoint::handle_event`) and then frees the connection.
  ///
  /// **Handle-equality-guarded `peers` removal.** When a redial happened
  /// during the original connection's drain window (`get_or_dial` saw the
  /// cached `Connection` in `Closed`/`Draining`/`Drained` and dialed a
  /// fresh `ConnectionHandle`), `peers[e.peer]` now points at the NEW
  /// handle while the slab still holds the OLD entry awaiting this reap.
  /// Removing `peers[e.peer]` unconditionally would clobber the redial's
  /// mapping. We therefore drop the `peers` entry only when it still points
  /// at the handle being reaped — the canonical post-redial invariant.
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
        Self::release_pending_inbound(&mut self.pending_inbound, e.peer);
      }
      if self.peers.get(&e.peer).copied() == Some(ch) {
        self.peers.remove(&e.peer);
      }
    }
    true
  }

  /// Release one unit of `peer`'s pending-inbound index. The single decrement
  /// primitive: every caller has already confirmed the entry was indexed
  /// (`pending_indexed`), so the source MUST have a positive count here. Removes
  /// the map entry at zero so an idle source leaves no residue.
  fn release_pending_inbound(pending_inbound: &mut HashMap<SocketAddr, usize>, peer: SocketAddr) {
    match pending_inbound.get_mut(&peer) {
      Some(count) => {
        debug_assert!(*count > 0, "pending-inbound index underflow for {peer}");
        *count -= 1;
        if *count == 0 {
          pending_inbound.remove(&peer);
        }
      }
      None => debug_assert!(
        false,
        "pending-inbound index missing an indexed entry for {peer}"
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
    Self::release_pending_inbound(&mut self.pending_inbound, released_peer);
  }

  /// Total number of connections currently tracked — every slab entry,
  /// counting handshaking, established, and still-draining connections alike.
  /// The global QUIC connection cap is enforced against this before an inbound
  /// Initial commits new state.
  pub(crate) fn len(&self) -> usize {
    self.conns.len()
  }

  /// Number of inbound (server-accepted) connections from `source` that have
  /// not yet established — still handshaking, OR handshake-failed and awaiting
  /// their drained-reap. This is the half-open state the per-source pending cap
  /// bounds before an unauthenticated inbound Initial commits new state.
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
    self.pending_inbound.get(source).copied().unwrap_or(0)
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
        e.peer == *source
          && e.direction == ConnDirection::Inbound
          && !e.established_at_least_once
          && !e.conn.is_handshaking()
      })
      .count()
  }

  /// Independent recount of the connections currently indexed under `source`
  /// (`pending_indexed`), read straight from the slab rather than the index.
  /// The index invariant is `pending_inbound_from == this recount` for every
  /// source at all times: a missed increment/decrement or a decrement that did
  /// not clear the bit makes the two diverge, so the counter-maintenance test
  /// asserts their equality after every accept / establish / reap to catch a
  /// leak, underflow, or double-count.
  #[cfg(test)]
  pub(crate) fn indexed_inbound_recount(&self, source: &SocketAddr) -> usize {
    self
      .conns
      .iter()
      .filter(|(_, e)| e.peer == *source && e.pending_indexed)
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
