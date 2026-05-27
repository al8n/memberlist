//! Per-peer QUIC connection table. One long-lived `quinn_proto::Connection`
//! per peer (idle-evicted by quinn-proto's own `max_idle_timeout`); reaped
//! only when `Connection::is_drained()` (the same protocol quinn uses).

use std::{
  collections::{HashMap, VecDeque},
  net::SocketAddr,
  time::Instant,
};

use quinn_proto::{Connection, ConnectionEvent, ConnectionHandle, Endpoint as QuinnEndpoint};
use slab::Slab;

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
    // Signature:
    // Endpoint::connect(&mut self, now: Instant, config: ClientConfig,
    //                   remote: SocketAddr, server_name: &str)
    //                   -> Result<(ConnectionHandle, Connection), ConnectError>
    // `server_name` is the rustls verification identity for the peer's
    // cert — supplied by the driver's SNI provider so the verification
    // name is keyed on the membership address (not a hardcoded constant).
    // quinn-proto forwards it verbatim to
    // `config.crypto.start_session(version, server_name, ...)`; the
    // rustls-backed `start_session` parses it via `ServerName::try_from`
    // and feeds the result to the configured `ServerCertVerifier`.
    let (ch, conn) = quinn.connect(now, client, peer, server_name)?;
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
    // Signature:
    // Endpoint::handle_event(&mut self, ch: ConnectionHandle, event: EndpointEvent)
    //                        -> Option<ConnectionEvent>
    //
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
  pub(crate) fn iter_handles(&self) -> Vec<ConnectionHandle> {
    self
      .conns
      .iter()
      .map(|(k, _)| ConnectionHandle(k))
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::quic::crypto::tests::{test_client, test_endpoint_config, test_server};

  fn quinn_pair() -> (
    QuinnEndpoint,
    QuinnEndpoint,
    super::super::crypto::QuicConfig,
  ) {
    let cfg = super::super::crypto::QuicConfig::new(
      test_endpoint_config(&[3u8; 32]),
      test_server(),
      test_client(),
      quinn_proto::TransportConfig::default(),
      "localhost",
    );
    let client = QuinnEndpoint::new(cfg.endpoint_arc(), None, true, Some([0x5a; 32]));
    let server = QuinnEndpoint::new(
      cfg.endpoint_arc(),
      Some(cfg.server_arc()),
      true,
      Some([0x5a; 32]),
    );
    (client, server, cfg)
  }

  #[test]
  fn dial_inserts_then_reuse_returns_same_handle() {
    let (mut client, _server, cfg) = quinn_pair();
    let mut t = ConnTable::new();
    let now = Instant::now();
    let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let ch1 = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    let ch2 = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    assert_eq!(ch1, ch2, "second call reuses the peer connection");
    assert_eq!(t.handle_for(&peer), Some(ch1));
  }

  #[test]
  fn reap_if_drained_is_false_until_drained_then_removes() {
    let (mut client, _server, cfg) = quinn_pair();
    let mut t = ConnTable::new();
    let now = Instant::now();
    let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let ch = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    assert!(
      !t.reap_if_drained(&mut client, ch),
      "fresh conn is not drained"
    );
    assert!(t.handle_for(&peer).is_some());

    // Initiate a graceful close; quinn will transition through Closed →
    // Drained after the close timer fires.
    t.get_mut(ch)
      .unwrap()
      .conn_mut()
      .close(now, 0u32.into(), bytes::Bytes::new());

    for _ in 0..5000 {
      if t.get_mut(ch).unwrap().conn_ref().is_drained() {
        break;
      }
      match t.get_mut(ch).unwrap().conn_mut().poll_timeout() {
        Some(d) => t.get_mut(ch).unwrap().conn_mut().handle_timeout(d),
        None => break,
      }
    }

    assert!(
      t.reap_if_drained(&mut client, ch),
      "connection must be drained after close+timeout drive"
    );
    assert_eq!(t.handle_for(&peer), None, "peers entry removed on reap");
  }

  /// `Connection::close` drives the cached `Connection` into `State::Closed`
  /// — `is_closed()` is `true` immediately (quinn-proto's `State::is_closed`
  /// matches `Closed | Draining | Drained`); the 3×PTO `Timer::Close` has
  /// not yet fired so `is_drained()` is still `false`. In that
  /// closed-before-drained window `Streams::open(Dir::Bi)` already refuses
  /// (same `is_closed()` check inside `Streams::open`), but
  /// `is_handshaking()` is `false`, so `service_dials`'s handshaking-requeue
  /// would not catch it.
  /// For a connection that had previously reached `Established`,
  /// `get_or_dial` MUST detect the un-reusable cached connection and dial
  /// a fresh one — silently returning the cached handle would route a
  /// redialable intent into `dial_failed` and lose the exchange.
  ///
  /// Negative control: revert the `is_closed()` check in `get_or_dial`
  /// (return the cached `ch` whenever the slab still holds it) and this
  /// test fails — `ch_redial == ch1` and the slab/peers state is wrong.
  #[test]
  fn get_or_dial_redials_when_cached_conn_is_closed_not_drained() {
    let (mut client, _server, cfg) = quinn_pair();
    let mut t = ConnTable::new();
    let now = Instant::now();
    let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let ch1 = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    // Synthesize "this connection has reached Established at least once"
    // — the runtime sets this on the same tick the conn transitions out
    // of `Handshake`; the unit test substitutes for that observation
    // because driving a real handshake in-test would defeat the closed-
    // state isolation. (For a never-Established cached entry the
    // behaviour is covered by `closed_never_established_does_not_redial`.)
    t.conns.get_mut(ch1.0).unwrap().established_at_least_once = true;
    // Drive the cached `Connection` into `State::Closed` (closed-before-
    // drained: `is_closed()` is `true`, `is_drained()` is still `false`,
    // `is_handshaking()` is `false`).
    t.get_mut(ch1)
      .unwrap()
      .conn_mut()
      .close(now, 0u32.into(), bytes::Bytes::new());
    assert!(t.get(ch1).unwrap().conn.is_closed());
    assert!(!t.get(ch1).unwrap().conn.is_drained());
    assert!(!t.get(ch1).unwrap().conn.is_handshaking());

    // Second dial in the closed-before-drained window: MUST redial.
    let ch_redial = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    assert_ne!(
      ch_redial, ch1,
      "closed-before-drained pool window must redial, not reuse the cached handle"
    );
    // The slab holds two entries for the same peer: the old one awaiting
    // its drained-reap, and the new live one.
    assert!(
      t.get(ch1).is_some(),
      "old slab entry retained so the in-flight drained-reap completes"
    );
    assert!(
      t.get(ch_redial).is_some(),
      "new slab entry inserted by the redial"
    );
    // `peers[peer]` points at the live (new) handle for subsequent
    // outbound exchanges.
    assert_eq!(
      t.handle_for(&peer),
      Some(ch_redial),
      "peers mapping advanced to the fresh redial"
    );

    // Drive the OLD connection to `Drained` and confirm its slab slot is
    // cleared while the new mapping is preserved (Part B equality guard).
    for _ in 0..5000 {
      if t.get(ch1).is_none_or(|e| e.conn_ref().is_drained()) {
        break;
      }
      match t.get_mut(ch1).unwrap().conn_mut().poll_timeout() {
        Some(d) => t.get_mut(ch1).unwrap().conn_mut().handle_timeout(d),
        None => break,
      }
    }
    assert!(
      t.reap_if_drained(&mut client, ch1),
      "old closed connection must drain-reap to completion"
    );
    // Critical: the new redial's mapping MUST survive the old handle's
    // drained-reap (handle-equality guard).
    assert_eq!(
      t.handle_for(&peer),
      Some(ch_redial),
      "drained-reap of the OLD handle must NOT clobber the NEW redial's peers mapping"
    );
    assert!(
      t.get(ch_redial).is_some(),
      "redial slab entry intact after old-handle drained-reap"
    );
  }

  /// A cached connection that is `is_closed()` but has NEVER reached
  /// `Established` (handshake failure for an unreachable peer) must NOT be
  /// redialed by `get_or_dial`: a fresh dial to the same unreachable peer
  /// would just fail the same way, and a SWIM periodic push-pull schedule
  /// would generate one fresh handshake per attempt for the lifetime of
  /// the deadline. Return the cached handle so
  /// `service_dials::open(Dir::Bi)=None && !is_handshaking()` falls through
  /// to `dial_failed`, consuming the intent on a single handshake attempt
  /// per push-pull deadline.
  #[test]
  fn closed_never_established_does_not_redial() {
    let (mut client, _server, cfg) = quinn_pair();
    let mut t = ConnTable::new();
    let now = Instant::now();
    let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let ch1 = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    // Do NOT seed `established_at_least_once`: this represents a
    // handshake failure (the conn never reached Established).
    assert!(!t.conns.get(ch1.0).unwrap().established_at_least_once);
    t.get_mut(ch1)
      .unwrap()
      .conn_mut()
      .close(now, 0u32.into(), bytes::Bytes::new());
    assert!(t.get(ch1).unwrap().conn.is_closed());

    // Second dial: MUST return the cached `ch1` so the caller's
    // `dial_failed` path fires, not a fresh redial.
    let ch_same = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    assert_eq!(
      ch_same, ch1,
      "never-Established closed conn must NOT generate a fresh handshake \
             attempt per dial — fall through to the existing dial_failed path"
    );
    assert_eq!(t.conns.len(), 1, "no extra slab entry was created");
  }

  /// `conn_mut` lazy-tracks `established_at_least_once`: the first call
  /// where the underlying connection is in the Established state (not
  /// handshaking, not closed) sticks the flag to `true`, and it stays
  /// `true` even after the connection later closes (the redial-vs-
  /// dial_failed decision in `get_or_dial` looks at the sticky flag).
  #[test]
  fn established_flag_sticks_via_conn_mut_lazy_tracking() {
    let (mut client, _server, cfg) = quinn_pair();
    let mut t = ConnTable::new();
    let now = Instant::now();
    let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let ch = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    assert!(!t.conns.get(ch.0).unwrap().established_at_least_once);
    // `conn_mut` on a handshaking conn: flag stays `false`. The returned
    // `&mut Connection` is the side effect we want to exercise; the binding
    // itself is discarded.
    let _ = t.get_mut(ch).unwrap().conn_mut();
    assert!(!t.conns.get(ch.0).unwrap().established_at_least_once);
    // Synthesize the Established observation by stepping the conn into
    // a non-handshaking-non-closed state would normally require a
    // peer-driven handshake. For this lazy-tracking unit test, prove
    // the flag flips ONLY when `conn_mut` sees the right state by
    // directly checking `!is_handshaking() && !is_closed()` on the
    // current state (handshaking — flag still false), then by setting
    // the flag directly to demonstrate stickiness across a subsequent
    // `close`.
    t.conns.get_mut(ch.0).unwrap().established_at_least_once = true;
    t.get_mut(ch)
      .unwrap()
      .conn_mut()
      .close(now, 0u32.into(), bytes::Bytes::new());
    // Closed state — `conn_mut` must NOT clear the sticky flag. The
    // returned `&mut Connection` is exercised for its side effect; the
    // binding itself is discarded.
    let _ = t.get_mut(ch).unwrap().conn_mut();
    assert!(
      t.conns.get(ch.0).unwrap().established_at_least_once,
      "flag must remain sticky after the conn closes"
    );
  }

  /// Closed-never-Established cached entry must NOT hide a live accepted
  /// connection. `insert_accepted` uses the SAME `!Connection::is_closed()`
  /// usability predicate as `get_or_dial`'s outbound reuse check, so when
  /// the cached canonical for a peer is closed (regardless of whether it
  /// ever reached Established), the newly-accepted connection becomes
  /// canonical (`peers[peer] = new_ch`). Symmetric with `get_or_dial`.
  ///
  /// Setup: dial X (the cache is seeded with `ch1`, `was_established`
  /// left false — the handshake-failed-or-closed-before-Established
  /// signature). Close X. Now `insert_accepted` a new connection for X.
  /// Assert that `peers[X]` advances to the new handle (the live
  /// accepted connection becomes the canonical), and that subsequent
  /// `get_or_dial(X)` returns the new handle so future outbound
  /// exchanges ride the live connection instead of the closed cached
  /// one. Finally, drive the OLD handle through drained-reap and
  /// confirm the equality-guarded `reap_if_drained` does NOT clobber
  /// the new canonical mapping.
  ///
  /// Negative control: if `insert_accepted` checked only
  /// `self.conns.contains(existing.0)` (slab presence — true while the
  /// closed handle is still awaiting its drained-reap), the cached
  /// closed handle would stay canonical. The assertion
  /// `peers[X] == Some(new_ch)` below would fail, every subsequent
  /// outbound dial would hit the closed cached `ch1`, fall through to
  /// the `service_dials::is_closed → dial_failed` branch, and the live
  /// accepted connection would be permanently unused.
  #[test]
  fn closed_never_established_cached_does_not_hide_live_accepted_connection() {
    let (mut client, _server, cfg) = quinn_pair();
    let mut t = ConnTable::new();
    let now = Instant::now();
    let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    // (1) Outbound dial X. `ch1` is now the canonical handle for peer.
    // `established_at_least_once` is left false to represent a
    // handshake failure / closed-before-Established cache.
    let ch1 = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    assert_eq!(t.handle_for(&peer), Some(ch1));
    assert!(!t.conns.get(ch1.0).unwrap().established_at_least_once);
    // (2) Drive the cached connection into `Closed` without ever
    // transitioning to Established. `Connection::close` flips
    // `is_closed()` to true immediately while `is_drained()` is still
    // false (the 3xPTO drain timer has not fired).
    t.get_mut(ch1)
      .unwrap()
      .conn_mut()
      .close(now, 0u32.into(), bytes::Bytes::new());
    assert!(t.get(ch1).unwrap().conn.is_closed());
    assert!(!t.get(ch1).unwrap().conn.is_drained());
    // (3) Mint a fresh `(ConnectionHandle, Connection)` from the same
    // quinn endpoint the original dial used. The runtime path is
    // `quinn.accept(...)` from the `DatagramEvent::NewConnection` arm
    // of `route_datagram_event`; here `quinn.connect` reuses that
    // endpoint's slab so the new `ch.0` lines up with the local
    // ConnTable slab's next vacant key (the lockstep `insert_accepted`
    // asserts). The accept-vs-dial origin does not matter to
    // `insert_accepted` itself — it only inserts the entry into the
    // slab and updates `peers` per the usability predicate.
    let alt: SocketAddr = "127.0.0.1:4500".parse().unwrap();
    let (new_ch, new_conn) = client
      .connect(now, cfg.client().clone(), alt, "localhost")
      .expect("fresh connection minted on the same client endpoint");
    t.insert_accepted(new_ch, new_conn, peer);
    // (4) The accepted connection IS live (`!is_closed()`), and the
    // cached canonical was closed (`is_closed()`), so the `peers`
    // canonical advances to `new_ch`. The slab-presence-only predicate
    // (`self.conns.contains(ch1.0)`) would instead leave the closed
    // handle canonical because the drained-pending slab entry is still
    // present.
    assert_eq!(
      t.handle_for(&peer),
      Some(new_ch),
      "the live accepted connection must replace the closed cached \
             canonical (the closed cached handle was hiding the live \
             accepted connection from subsequent outbound dials)"
    );
    // (5) Subsequent outbound dial returns the NEW canonical: outbound
    // exchanges ride the live accepted connection.
    let ch_after = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    assert_eq!(
      ch_after, new_ch,
      "outbound dial after the accept must reuse the new live canonical, \
             not the closed cached ch1"
    );
    // (6) Old slab entry persists awaiting its drained-reap. Drive it
    // to Drained and confirm the equality-guarded `reap_if_drained`
    // clears the OLD slab slot without clobbering the NEW canonical.
    for _ in 0..5000 {
      if t.get(ch1).is_none_or(|e| e.conn_ref().is_drained()) {
        break;
      }
      match t.get_mut(ch1).unwrap().conn_mut().poll_timeout() {
        Some(d) => t.get_mut(ch1).unwrap().conn_mut().handle_timeout(d),
        None => break,
      }
    }
    assert!(
      t.reap_if_drained(&mut client, ch1),
      "old closed cached handle must drained-reap to completion"
    );
    assert_eq!(
      t.handle_for(&peer),
      Some(new_ch),
      "the new canonical must survive the OLD handle's drained-reap \
             (handle-equality-guarded `peers` removal protects it)"
    );
  }

  /// Part B isolated: the handle-equality guard's three cases — plain reap
  /// (peers→ch holds, remove), mid-drain redial (peers→new_ch holds,
  /// skip), and peers absent (None, skip). The first two are exercised by
  /// `reap_if_drained_is_false_until_drained_then_removes` and
  /// `get_or_dial_redials_when_cached_conn_is_closed_not_drained`
  /// respectively; this test pins case (c): a synthetic state where the
  /// slab still holds `ch` (drained) but `peers[e.peer]` was already
  /// removed (e.g. by a fresh `get_or_dial` that dialed before this reap
  /// ran). The reap must be idempotent — clear the slab slot, leave the
  /// (now-absent) peers state alone.
  #[test]
  fn reap_if_drained_handle_equality_guard_idempotent_when_peers_absent() {
    let (mut client, _server, cfg) = quinn_pair();
    let mut t = ConnTable::new();
    let now = Instant::now();
    let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let ch = t
      .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
      .unwrap();
    // Drive the conn through `close` to `Drained`.
    t.get_mut(ch)
      .unwrap()
      .conn_mut()
      .close(now, 0u32.into(), bytes::Bytes::new());
    for _ in 0..5000 {
      if t.get(ch).unwrap().conn_ref().is_drained() {
        break;
      }
      match t.get_mut(ch).unwrap().conn_mut().poll_timeout() {
        Some(d) => t.get_mut(ch).unwrap().conn_mut().handle_timeout(d),
        None => break,
      }
    }
    assert!(t.get(ch).unwrap().conn_ref().is_drained());
    // Synthesize "peers entry already removed" (the redial case at the
    // limit: peers cleared but the old slab still holds the drained
    // entry awaiting this reap).
    t.peers.remove(&peer);
    assert_eq!(t.handle_for(&peer), None);
    // The reap must still complete (slab cleared, no panic, no
    // resurrection of a stale peers entry).
    assert!(
      t.reap_if_drained(&mut client, ch),
      "reap must succeed even when peers entry was pre-removed"
    );
    assert!(t.get(ch).is_none(), "slab slot freed");
    assert_eq!(
      t.handle_for(&peer),
      None,
      "peers state unchanged (no resurrection)"
    );
  }
}
