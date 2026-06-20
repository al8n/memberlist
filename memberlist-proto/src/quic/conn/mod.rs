//! Per-peer QUIC connection table. One long-lived `quinn_proto::Connection`
//! per peer (idle-evicted by quinn-proto's own `max_idle_timeout`); reaped
//! only when `Connection::is_drained()` (the same protocol quinn uses).

use crate::Instant;
use core::net::SocketAddr;
use std::collections::{HashMap, VecDeque};

use quinn_proto::{Connection, ConnectionEvent, ConnectionHandle, Endpoint as QuinnEndpoint};
use slab::Slab;
use smallvec_wrapper::MediumVec;

pub(crate) struct ConnEntry {
  conn: Connection,
  peer: SocketAddr,
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
}

impl ConnTable {
  pub(crate) fn new() -> Self {
    Self {
      conns: Slab::new(),
      peers: HashMap::new(),
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
      established_at_least_once: false,
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
      established_at_least_once: false,
      pending_events: VecDeque::new(),
    });
    assert_eq!(
      slot, ch.0,
      "accepted connection slab slot must equal ConnectionHandle"
    );
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
      if self.peers.get(&e.peer).copied() == Some(ch) {
        self.peers.remove(&e.peer);
      }
    }
    true
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
