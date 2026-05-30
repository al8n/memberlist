//! [`Cluster`]: top-level simulation harness.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use memberlist_machine::{AliveDelegate, EndpointConfig, Event, Instant, MergeDelegate};
use memberlist_wire::typed::{
  Alive, Dead, Message, Node, NodeState, PushNodeState, State, Suspect,
};
use smol_str::SmolStr;

use crate::{
  clock::Clock,
  network::{Network, VirtualStream},
};

/// Per-host admission policy. Installs synchronous
/// [`AliveDelegate`]/[`MergeDelegate`] filters on the host endpoint so an
/// inbound Alive / join push-pull can be vetoed inline, exactly as Go
/// memberlist's `config.Alive` / `config.Merge` do.
///
/// The default (`auto_accept`) admits both, matching the no-delegate
/// (`None`) machine default, so existing tests are unaffected.
#[derive(Debug, Clone, Copy)]
pub struct DecisionPolicy {
  accept_alive: bool,
  accept_merge: bool,
}

impl DecisionPolicy {
  /// Admit everything (default behaviour).
  pub const fn auto_accept() -> Self {
    Self {
      accept_alive: true,
      accept_merge: true,
    }
  }

  /// Reject every alive and every join merge.
  pub const fn reject_all() -> Self {
    Self {
      accept_alive: false,
      accept_merge: false,
    }
  }

  /// Replace the admit-inbound-alive flag (default `true`). `false`
  /// installs an `AliveDelegate` that rejects every alive.
  pub const fn with_accept_alive(mut self, accept_alive: bool) -> Self {
    self.accept_alive = accept_alive;
    self
  }

  /// Replace the admit-join-merge flag (default `true`). `false` installs
  /// a `MergeDelegate` that cancels every join merge.
  pub const fn with_accept_merge(mut self, accept_merge: bool) -> Self {
    self.accept_merge = accept_merge;
    self
  }

  /// Whether inbound Alive messages are admitted.
  #[inline(always)]
  pub const fn accept_alive(&self) -> bool {
    self.accept_alive
  }

  /// Whether join push-pull merges are admitted.
  #[inline(always)]
  pub const fn accept_merge(&self) -> bool {
    self.accept_merge
  }
}

impl Default for DecisionPolicy {
  fn default() -> Self {
    Self::auto_accept()
  }
}

/// Simulation adapter: an [`AliveDelegate`] that admits/rejects every alive
/// per a fixed bool (models a `config.Alive` that always returns nil / err).
struct PolicyAliveDelegate {
  accept: bool,
}

impl AliveDelegate<SmolStr, SocketAddr> for PolicyAliveDelegate {
  fn notify_alive(&self, _peer: &NodeState<SmolStr, SocketAddr>) -> bool {
    self.accept
  }
}

/// Simulation adapter: a [`MergeDelegate`] that admits/cancels every join
/// merge per a fixed bool (models a `config.Merge` that always returns
/// nil / err).
struct PolicyMergeDelegate {
  accept: bool,
}

impl MergeDelegate<SmolStr, SocketAddr> for PolicyMergeDelegate {
  fn notify_merge(&self, _peers: &[NodeState<SmolStr, SocketAddr>]) -> bool {
    self.accept
  }
}

/// Top-level simulation harness.
///
/// # Step loop contract
///
/// Each call to [`step`](Cluster::step) represents one simulation tick:
/// 1. Drain datagram transmits → enqueue datagrams.
/// 2. Process `DialRequested` events → create `VirtualStream` pairs.
/// 3. Pipe bytes between active virtual streams.
/// 4. Drain remaining events (admission already applied inline by the
///    per-host [`AliveDelegate`]/[`MergeDelegate`]).
/// 5. Advance simulated time to the next deadline.
/// 6. Deliver matured datagrams.
/// 7. Fire `handle_timeout(now)` on all endpoints.
///
/// Returns `true` if any datagram was delivered or any event was processed.
pub struct Cluster {
  pub(crate) net: Network,
  pub(crate) clock: Clock,
  /// In-flight virtual stream pairs managed by the stream simulation.
  pub(crate) streams: Vec<VirtualStream>,
  /// Per-host admission policies. Missing entries default to `auto_accept`
  /// (no delegate installed). The authoritative copy is mirrored onto the
  /// endpoint as installed delegates by [`apply_policy`](Cluster::apply_policy).
  pub(crate) policies: HashMap<SocketAddr, DecisionPolicy>,
}

impl Cluster {
  /// Create an empty cluster with no nodes.
  pub fn new() -> Self {
    Self {
      net: Network::new(),
      clock: Clock::new(),
      streams: Vec::new(),
      policies: HashMap::new(),
    }
  }

  /// Install the per-host admission `policy` for `host` as synchronous
  /// [`AliveDelegate`]/[`MergeDelegate`] filters on its endpoint.
  ///
  /// By default all hosts admit everything (no delegate). This lets tests
  /// configure a host to veto inbound alives / join merges, simulating a Go
  /// `config.Alive` / `config.Merge` that returns a non-nil error. The
  /// `host` endpoint must already exist (call after `add_node`).
  pub fn set_decision_policy(&mut self, host: SocketAddr, policy: DecisionPolicy) {
    self.policies.insert(host, policy);
    self.apply_policy(host);
  }

  /// Convenience: cause `host` to reject every join push-pull merge.
  /// Simulates a `MergeDelegate` that always cancels.
  pub fn reject_merges(&mut self, host: SocketAddr) {
    let entry = self
      .policies
      .entry(host)
      .or_insert_with(DecisionPolicy::auto_accept);
    *entry = entry.with_accept_merge(false);
    self.apply_policy(host);
  }

  /// Convenience: cause `host` to reject every inbound Alive. Simulates an
  /// `AliveDelegate` that always vetoes.
  pub fn reject_alives(&mut self, host: SocketAddr) {
    let entry = self
      .policies
      .entry(host)
      .or_insert_with(DecisionPolicy::auto_accept);
    *entry = entry.with_accept_alive(false);
    self.apply_policy(host);
  }

  /// Mirror `host`'s current [`DecisionPolicy`] onto its endpoint as
  /// installed delegates. A delegate is always (re)installed reflecting the
  /// current bools — when both are `true` they admit everything, identical
  /// to the no-delegate default. No-op if `host` is not a known endpoint.
  fn apply_policy(&mut self, host: SocketAddr) {
    let policy = self.policies.get(&host).copied().unwrap_or_default();
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.set_alive_delegate(PolicyAliveDelegate {
        accept: policy.accept_alive(),
      });
      ep.set_merge_delegate(PolicyMergeDelegate {
        accept: policy.accept_merge(),
      });
    }
  }

  /// Set the payload that `host` returns in its Ack responses.
  ///
  /// Mirrors the legacy `PingDelegate::ack_payload` callback. The payload is
  /// included in every Ack the host sends, so the pinger's
  /// `Event::PingCompleted` will carry this payload.
  ///
  /// Does nothing if `host` is not a known endpoint.
  ///
  /// # Panics
  ///
  /// Panics if the framed ack carrying `payload` would exceed the host's
  /// gossip packet budget (`Endpoint::set_ack_payload` rejects an
  /// unsendable ack). Sim scenarios pass small payloads, so a rejection
  /// here is a test-authoring error, not a runtime condition.
  pub fn set_ack_payload(&mut self, host: SocketAddr, payload: bytes::Bytes) {
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.set_ack_payload(payload)
        .expect("sim ack payload must fit the gossip budget");
    }
  }

  /// Add a node with the given string ID and `SocketAddr`.
  ///
  /// The node's `Endpoint` is constructed with sensible LAN defaults and
  /// `start_scheduling` is called immediately. Returns the addr for chaining.
  pub fn add_node(&mut self, id: SmolStr, addr: SocketAddr) -> SocketAddr {
    self.add_node_with(id, addr, |c| c)
  }

  /// Add a node with a custom configuration modifier.
  ///
  /// The `config_fn` receives the default [`EndpointConfig`] and may chain
  /// builder methods (e.g. `.with_dead_node_reclaim_time(...)`) before
  /// returning it. Useful for tests that need non-default knobs such as
  /// `dead_node_reclaim_time`.
  pub fn add_node_with(
    &mut self,
    id: SmolStr,
    addr: SocketAddr,
    config_fn: impl FnOnce(EndpointConfig<SmolStr, SocketAddr>) -> EndpointConfig<SmolStr, SocketAddr>,
  ) -> SocketAddr {
    let cfg = EndpointConfig::new(id, addr)
      .with_gossip_interval(Duration::from_millis(200))
      .with_push_pull_interval(Duration::from_secs(30))
      .with_probe_interval(Duration::from_millis(1000))
      .with_probe_timeout(Duration::from_millis(500))
      .with_suspicion_mult(4)
      .with_retransmit_mult(4)
      .with_rng_seed(addr.port() as u64);
    let cfg = config_fn(cfg);
    self.net.add_endpoint(cfg, self.clock.now());
    addr
  }

  /// Run one simulation tick. Returns `true` if anything happened.
  pub fn step(&mut self) -> bool {
    let now = self.clock.now();

    // 1. Drain transmits from all endpoints → enqueue datagrams.
    self.net.drain_transmits(now);

    // 2. Process DialRequested events → establish stream pairs.
    let new_streams = self.net.process_dial_requests(&mut self.streams, now);

    // 3. Pipe bytes between active streams.
    self.net.step_streams(&mut self.streams, now);

    // 4. Drain remaining non-DialRequested events (admission already applied
    //    inline by each host's installed AliveDelegate / MergeDelegate).
    let events_fired = self.net.drain_events(now);

    // 5. Advance to next deadline (endpoints + active streams).
    let stream_deadline = self
      .streams
      .iter()
      .filter_map(|vs| {
        [vs.initiator.poll_timeout(), vs.acceptor.poll_timeout()]
          .into_iter()
          .flatten()
          .min()
      })
      .min();
    let next = [self.net.next_deadline(), stream_deadline]
      .into_iter()
      .flatten()
      .min();
    let next = match next {
      Some(t) => t,
      None => return events_fired || new_streams,
    };
    if next > now {
      self.clock.advance(next - now);
    }
    let now = self.clock.now();

    // 6. Deliver matured datagrams.
    let delivered = self.net.deliver_ready(now);

    // 7. Tick all endpoints.
    self.net.tick_all(now);

    events_fired || new_streams || delivered > 0
  }

  /// Run until no progress is made for 3 consecutive steps,
  /// or `max_steps` total steps have been taken.
  pub fn run_until_quiescent(&mut self, max_steps: usize) {
    let mut idle = 0;
    for _ in 0..max_steps {
      if self.step() {
        idle = 0;
      } else {
        idle += 1;
        if idle >= 3 {
          // Three consecutive no-op steps: quiescent.
          break;
        }
      }
    }
  }

  /// Advance simulated time by `by` and then deliver any newly matured datagrams.
  pub fn advance(&mut self, by: Duration) {
    self.clock.advance(by);
    let now = self.clock.now();
    self.net.deliver_ready(now);
    self.net.tick_all(now);
  }

  /// Look up a member on the node at `host` with node-id `peer`.
  ///
  /// Returns the `host` endpoint's view of the `peer` node.
  /// `None` means either the `host` endpoint does not exist or the `host`
  /// has not yet learned about `peer`.
  pub fn member(
    &self,
    host: SocketAddr,
    peer: &SmolStr,
  ) -> Option<Arc<NodeState<SmolStr, SocketAddr>>> {
    self.net.endpoints.get(&host)?.member(peer)
  }

  /// Simulated time right now.
  pub fn now(&self) -> Instant {
    self.clock.now()
  }

  // ── Test bootstrap helper ─────────────────────────────────────────────────

  /// Test helper: directly inject an `Alive` about `peer_id`/`peer_addr` into
  /// the `host` endpoint's view. Useful for bootstrapping scenarios without a
  /// UDP join handshake. Admission is applied inline by `host`'s installed
  /// [`AliveDelegate`] (none by default → admitted), and any resulting
  /// `NodeJoined` / `NodeUpdated` / `NodeConflict` stays queued for
  /// [`poll_event`](Cluster::poll_event).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn inject_alive(
    &mut self,
    host: SocketAddr,
    peer_id: SmolStr,
    peer_addr: SocketAddr,
    incarnation: u32,
  ) {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    let alive = Alive::new(incarnation, Node::new(peer_id, peer_addr));
    ep.handle_alive(host, alive, now);
  }

  /// Test helper: directly start a probe round on `host` (bypasses the
  /// scheduler). Returns `true` if `start_probe` reports it actually launched
  /// (false if no eligible peers exist).
  ///
  /// Does nothing and returns `false` if `host` is not a known endpoint.
  pub fn trigger_probe(&mut self, host: SocketAddr) -> bool {
    let now = self.clock.now();
    match self.net.endpoints.get_mut(&host) {
      Some(ep) => ep.start_probe(now),
      None => false,
    }
  }

  /// Test helper: inject a Suspect message into `host`'s view of `target_id`.
  ///
  /// The Suspect is attributed to `from_id` (typically the host itself for
  /// scenarios where the host is the witness). `incarnation` is the
  /// incarnation number to use — should be >= the target's current incarnation.
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn inject_suspect(
    &mut self,
    host: SocketAddr,
    target_id: SmolStr,
    from_id: SmolStr,
    incarnation: u32,
  ) {
    use memberlist_wire::typed::Suspect;
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    let s = Suspect::new(incarnation, target_id, from_id);
    ep.handle_suspect(host, s, now);
  }

  /// Test helper: return the gossip-tracked liveness state of `peer` from
  /// `host`'s perspective.
  ///
  /// Unlike [`member`](Cluster::member) (which reflects the wire-protocol
  /// `NodeState.state` fixed at insertion), this returns the value maintained
  /// by the gossip state machine — i.e. it reflects Suspect / Dead
  /// transitions.
  ///
  /// Returns `None` if either `host` or `peer` is unknown.
  pub fn member_liveness(
    &self,
    host: SocketAddr,
    peer: &SmolStr,
  ) -> Option<memberlist_wire::typed::State> {
    self.net.endpoints.get(&host)?.member_liveness(peer)
  }

  /// Test helper: directly initiate a push/pull from `host` to `peer_addr`.
  ///
  /// Bypasses the periodic push-pull scheduler; useful for deterministic
  /// scenario tests that want to trigger a push/pull without waiting for the
  /// scheduler to fire. The resulting `Event::DialRequested` will be processed
  /// by the next call to [`step`](Cluster::step).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn trigger_push_pull(&mut self, host: SocketAddr, peer_addr: SocketAddr) {
    use memberlist_machine::PushPullKind;
    let now = self.clock.now();
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.start_push_pull(peer_addr, PushPullKind::Refresh, now);
    }
  }

  // ── Legacy-API mutators (state injection — bypass network) ───────────────

  /// Inject an [`Alive`] message at `host` (legacy `Memberlist::alive_node`).
  /// Admission is applied inline by `host`'s installed [`AliveDelegate`]
  /// (none by default → admitted); resulting `NodeJoined` / `NodeUpdated` /
  /// `NodeConflict` stays queued for [`poll_event`](Cluster::poll_event).
  ///
  /// `bootstrap` distinguishes an initial-join (`true`) from a gossip-received
  /// message (`false`). The existing [`inject_alive`](Cluster::inject_alive)
  /// helper is kept as a convenience wrapper around this method.
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn alive_node(
    &mut self,
    host: SocketAddr,
    alive: Alive<SmolStr, SocketAddr>,
    bootstrap: bool,
  ) {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    // The bootstrap distinction is documentation-only in the sim — both
    // paths route through `handle_alive` identically.
    let _ = bootstrap;
    ep.handle_alive(host, alive, now);
  }

  /// Inject a [`Suspect`] message at `host` (legacy `Memberlist::suspect_node`).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn suspect_node(&mut self, host: SocketAddr, suspect: Suspect<SmolStr>) {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    ep.handle_suspect(host, suspect, now);
  }

  /// Inject a [`Dead`] message at `host` (legacy `Memberlist::dead_node`).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn dead_node(&mut self, host: SocketAddr, dead: Dead<SmolStr>) {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    ep.handle_dead(host, dead, now);
  }

  /// Merge a slice of [`PushNodeState`] entries directly into `host`'s view
  /// (legacy `Memberlist::merge_remote_state`).
  ///
  /// This calls `merge_state` directly (not the join push-pull stream path),
  /// so the [`MergeDelegate`] gate is intentionally bypassed — mirroring the
  /// legacy `merge_state`, which applies all transitions inline without an
  /// app decision gate. Per-entry Alive admission still goes through `host`'s
  /// installed [`AliveDelegate`]. Resulting events stay queued for
  /// [`poll_event`](Cluster::poll_event).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn merge_remote_state(
    &mut self,
    host: SocketAddr,
    remote_states: Vec<PushNodeState<SmolStr, SocketAddr>>,
  ) {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    ep.merge_state(&remote_states, now);
  }

  /// Roll back `peer`'s `state_change` timestamp by `delta` at `host`.
  ///
  /// This lets tests simulate an aged suspicion / dead state without sleeping
  /// (legacy `change_node` helper in `memberlist-core`).
  ///
  /// Does nothing if `host` or `peer` is unknown.
  pub fn age_node(&mut self, host: SocketAddr, peer: &SmolStr, delta: Duration) {
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.age_member(peer, delta);
    }
  }

  // ── Legacy-API observers ──────────────────────────────────────────────────

  /// Current [`State`] of `peer` from `host`'s perspective (alias for
  /// [`member_liveness`](Cluster::member_liveness); both names work in tests).
  ///
  /// Returns `None` if either `host` or `peer` is unknown.
  pub fn get_node_state(&self, host: SocketAddr, peer: &SmolStr) -> Option<State> {
    self.member_liveness(host, peer)
  }

  /// Timestamp of `peer`'s last state-change at `host`.
  ///
  /// Returns `None` if either `host` or `peer` is unknown.
  pub fn get_node_state_change(&self, host: SocketAddr, peer: &SmolStr) -> Option<Instant> {
    self.net.endpoints.get(&host)?.node_state_change(peer)
  }

  /// Incarnation number of `peer` at `host`.
  ///
  /// Returns `None` if either `host` or `peer` is unknown.
  pub fn get_node_incarnation(&self, host: SocketAddr, peer: &SmolStr) -> Option<u32> {
    self.net.endpoints.get(&host)?.node_incarnation(peer)
  }

  /// Total number of members visible to `host` (including itself).
  ///
  /// Returns `0` if `host` is unknown.
  pub fn num_members(&self, host: SocketAddr) -> usize {
    self
      .net
      .endpoints
      .get(&host)
      .map_or(0, |ep| ep.num_members())
  }

  /// Local node ID at `host`, or `None` if `host` is unknown.
  pub fn local_id(&self, host: SocketAddr) -> Option<SmolStr> {
    self
      .net
      .endpoints
      .get(&host)
      .map(|ep| ep.local_id_ref().clone())
  }

  /// Local node's advertise [`Node`] at `host`, or `None` if unknown.
  pub fn advertise_node(&self, host: SocketAddr) -> Option<Node<SmolStr, SocketAddr>> {
    self
      .net
      .endpoints
      .get(&host)
      .map(|ep| Node::new(ep.local_id_ref().clone(), *ep.advertise_ref()))
  }

  /// Incarnation number of the local node at `host`.
  ///
  /// Returns `None` if `host` is unknown.
  pub fn local_incarnation(&self, host: SocketAddr) -> Option<u32> {
    self
      .net
      .endpoints
      .get(&host)
      .map(|ep| ep.local_incarnation())
  }

  /// Number of gossip broadcasts currently queued at `host`.
  ///
  /// Returns `0` if `host` is unknown.
  pub fn broadcast_queue_len(&self, host: SocketAddr) -> usize {
    self
      .net
      .endpoints
      .get(&host)
      .map_or(0, |ep| ep.broadcast_queue_len())
  }

  /// Drain all queued broadcasts from `host` and return their messages.
  ///
  /// Useful for asserting that a specific message type (e.g. an Alive refute
  /// after the buddy-system Suspect piggyback) is present in the queue.
  /// Returns an empty `Vec` if `host` is unknown.
  pub fn drain_broadcasts(&mut self, host: SocketAddr) -> Vec<Message<SmolStr, SocketAddr>> {
    match self.net.endpoints.get_mut(&host) {
      Some(ep) => ep.drain_broadcasts(),
      None => Vec::new(),
    }
  }

  /// Drain one [`Event`] from `host`'s queue.
  ///
  /// Returns `None` if `host` is unknown or the queue is empty.
  pub fn poll_event(&mut self, host: SocketAddr) -> Option<Event<SmolStr, SocketAddr>> {
    self.net.endpoints.get_mut(&host)?.poll_event()
  }

  // ── Fault injection ───────────────────────────────────────────────────────

  /// Partition `group_a` from `group_b`: datagrams between the two groups are
  /// silently dropped. Replaces any existing partition.
  pub fn partition(&mut self, group_a: &[SocketAddr], group_b: &[SocketAddr]) {
    let mut map = HashMap::new();
    for &a in group_a {
      map.insert(a, 0usize);
    }
    for &b in group_b {
      map.insert(b, 1usize);
    }
    self.net.faults.partitions = Some(map);
  }

  /// Remove any active partition.
  pub fn heal(&mut self) {
    self.net.faults.partitions = None;
  }

  /// Drop the next datagram sent FROM `addr` (one-shot).
  pub fn drop_next_datagram_from(&mut self, addr: SocketAddr) {
    self.net.faults.drop_next.insert(addr);
  }

  /// Set a fixed latency added to every subsequently enqueued datagram.
  pub fn set_latency(&mut self, latency: Duration) {
    self.net.faults.latency = latency;
  }

  /// Trigger an application-level ping from `host` to `peer_id`/`peer_addr`
  /// (legacy `Memberlist::ping`).
  ///
  /// The ping result flows back as `Event::PingCompleted { node, rtt, payload }`
  /// when the peer's Ack arrives within the probe timeout.  The caller should
  /// run `step()` or `advance()` to let the simulated network carry the
  /// Ping/Ack exchange, then poll for the event with `poll_event(host)`.
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn trigger_ping(&mut self, host: SocketAddr, peer_id: SmolStr, peer_addr: SocketAddr) {
    let now = self.clock.now();
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.ping(Node::new(peer_id, peer_addr), now);
    }
  }

  /// Remove `Dead` and `Left` members from `host` whose `state_change` is
  /// older than `gossip_to_the_dead_time` (legacy `Memberlist::reset_nodes`).
  ///
  /// Call after [`advance`](Cluster::advance) to simulate the periodic GC pass
  /// that the legacy memberlist runs. Does nothing if `host` is unknown.
  pub fn reset_nodes(&mut self, host: SocketAddr) {
    let now = self.clock.now();
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.reset_nodes(now);
    }
  }

  // ── Lifecycle helpers (legacy `Memberlist::join / leave / set_meta / send`) ─

  /// Initiate a join push/pull from `host` to `peer_addr`
  /// (legacy `Memberlist::join`).
  ///
  /// Sends a `PushPullKind::Join` exchange so `host` bootstraps its view from
  /// `peer_addr`. The exchange is carried by the next call(s) to
  /// [`step`](Cluster::step).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn join(&mut self, host: SocketAddr, peer_addr: SocketAddr) {
    use memberlist_machine::PushPullKind;
    let now = self.clock.now();
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.start_push_pull(peer_addr, PushPullKind::Join, now);
    }
  }

  /// Initiate a graceful leave on `host` (legacy `Memberlist::leave`).
  ///
  /// Marks the local node `Left`, broadcasts a `Dead` self-message, and
  /// stops all periodic schedulers.  Returns `Err` if the endpoint is already
  /// left or not a known endpoint.
  ///
  /// The Dead self-broadcast is immediately fanned out to all known peers
  /// (bypassing the gossip scheduler, which is stopped by `leave`).  Callers
  /// should still run [`step`](Cluster::step) to let the datagrams propagate.
  ///
  /// Does nothing and returns `Ok(())` if `host` is not a known endpoint.
  pub fn leave(&mut self, host: SocketAddr) -> Result<(), memberlist_machine::Error> {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return Ok(());
    };
    // Collect peer addresses before calling leave (which takes &mut self).
    let local_id = ep.local_id_ref().clone();
    let peer_addrs: Vec<SocketAddr> = ep
      .members()
      .filter(|ns| ns.id_ref() != &local_id)
      .map(|ns| *ns.address_ref())
      .collect();
    let result = ep.leave(now);
    if result.is_ok() {
      // Drain the Dead broadcast from the gossip queue and fan it out to all
      // known peer addresses immediately.  The gossip scheduler won't fire
      // after leave (lifecycle = Left), so we do one manual gossip round here.
      let msgs = ep.drain_broadcasts();
      for peer_addr in peer_addrs {
        for msg in &msgs {
          self.net.enqueue(host, peer_addr, msg.clone(), now);
        }
      }
    }
    result
  }

  /// Update the local node's metadata at `host` and broadcast the change
  /// (legacy `Memberlist::update_node`).
  ///
  /// `meta` is broadcast as a new `Alive` message with the bumped incarnation,
  /// so peers learn the update via gossip after the next [`step`](Cluster::step).
  ///
  /// Does nothing and returns `Ok(())` if `host` is not a known endpoint.
  pub fn set_meta(
    &mut self,
    host: SocketAddr,
    meta: memberlist_wire::typed::Meta,
  ) -> Result<(), memberlist_machine::Error> {
    match self.net.endpoints.get_mut(&host) {
      Some(ep) => ep.update_meta(meta),
      None => Ok(()),
    }
  }

  /// Send an unreliable (UDP) user-data payload from `host` to `peer_addr`
  /// (legacy `Memberlist::send`).
  ///
  /// The payload is enqueued in the virtual network queue and delivered to the
  /// peer's endpoint as `Event::UserPacket { reliability: Unreliable }` within
  /// the next [`step`](Cluster::step) call.
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn send(&mut self, host: SocketAddr, peer_addr: SocketAddr, data: bytes::Bytes) {
    let now = self.clock.now();
    // Enqueue a UserData message directly into the simulated network.
    // The next `deliver_ready` call (inside `step`) will dispatch it to
    // `peer_addr`'s endpoint via `handle_user_data(Unreliable)`.
    self.net.enqueue(
      host,
      peer_addr,
      memberlist_wire::typed::Message::UserData(data),
      now,
    );
  }

  /// Send a reliable (stream) user-data payload from `host` to `peer_addr`
  /// (legacy `Memberlist::send_reliable`).
  ///
  /// Enqueues a `DialRequested` event that the next [`step`](Cluster::step)
  /// converts into a `VirtualStream` pair; once connected the payload bytes
  /// flow through the stream FSM to the peer as
  /// `Event::UserPacket { reliability: Reliable }`.
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn send_reliable(&mut self, host: SocketAddr, peer_addr: SocketAddr, data: bytes::Bytes) {
    let now = self.clock.now();
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.start_user_message(peer_addr, data, now);
    }
  }

  // ── Health / awareness helpers ───────────────────────────────────────────────

  /// Current Lifeguard health score at `host` (legacy `Memberlist::health_score`).
  ///
  /// `0` is fully healthy; higher values mean worse, capped at
  /// `awareness_max_multiplier - 1` (default 8 - 1 = 7).
  ///
  /// Returns `None` if `host` is not a known endpoint.
  pub fn health_score(&self, host: SocketAddr) -> Option<u32> {
    self.net.endpoints.get(&host).map(|ep| ep.health_score())
  }

  /// Force a degraded awareness score at `host` by recording `severity`
  /// failures. Used in tests to set up an initial degraded state without
  /// running a full failed probe.
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn degrade_health(&mut self, host: SocketAddr, severity: u32) {
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.degrade_health(severity);
    }
  }

  /// Improve health at `host` by recording one success (decrements score).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn improve_health(&mut self, host: SocketAddr) {
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.improve_health();
    }
  }
}

impl Default for Cluster {
  fn default() -> Self {
    Self::new()
  }
}
