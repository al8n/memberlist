//! [`Cluster`]: top-level simulation harness.

use std::{
  collections::{HashMap, HashSet},
  net::SocketAddr,
  sync::Arc,
  time::Duration,
};

use memberlist_proto::{
  AliveDelegate, EndpointOptions, Event, Instant, MergeDelegate,
  typed::{Alive, Dead, Message, Node, NodeState, PushNodeState, State, Suspect},
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
  /// Per-host configured Ack payload (the `PingDelegate::ack_payload`
  /// behavior). Like [`policies`](Self::policies), this is a delegate-level
  /// behavior configured at the application boundary, so it must survive a
  /// crash/restart: a real restarted process re-installs its delegates from
  /// config. The authoritative copy lives here and is reapplied onto the fresh
  /// endpoint by [`restart`](Cluster::restart).
  pub(crate) ack_payload: HashMap<SocketAddr, bytes::Bytes>,
  /// Registered node addresses, in insertion order (deterministic iteration).
  pub(crate) addrs: Vec<SocketAddr>,
  /// Registered node ids, in insertion order (parallel to `addrs`).
  pub(crate) ids: Vec<SmolStr>,
  /// Last recorded local incarnation for each address, captured at crash time.
  /// Used by `restart` to ensure the fresh endpoint starts at a higher
  /// incarnation than any incarnation the cluster has ever seen for this node.
  pub(crate) last_incarnation: HashMap<SocketAddr, u32>,
  /// Per-node final [`EndpointOptions`] as configured at `add_node_with` time
  /// (after the caller's `config_fn` was applied). Used by `restart` to rebuild
  /// the fresh endpoint with the SAME configuration — initial meta, stream
  /// timeouts, and every other knob — so a restart preserves the node's
  /// identity, not just its address.
  pub(crate) node_cfg: HashMap<SocketAddr, EndpointOptions<SmolStr, SocketAddr>>,
  /// `(observer, subject)` member entries that `reset_nodes` actually pruned
  /// during the most recent [`step`](Cluster::step). Recomputed every step:
  /// cleared at the start, then populated by snapshotting the terminal
  /// (`Dead`/`Left`) entries immediately before the `tick_all` phase (which
  /// runs each endpoint's `handle_timeout` → `reset_nodes`) and recording those
  /// that became absent immediately after. The terminal checkers read this via
  /// [`was_pruned_this_step`](Cluster::was_pruned_this_step) to expire a
  /// terminal baseline on the ACTUAL prune event rather than on elapsed time —
  /// the prune and the post-prune re-learn can land in separate steps with the
  /// intervening absence never sampled, so the prune signal is the only reliable
  /// discriminator between a legal re-join and a buggy in-place resurrection.
  pub(crate) pruned_this_step: HashSet<(SocketAddr, SmolStr)>,
  /// Per-mutation history transitions recorded during the most recent
  /// [`step`](Cluster::step): every distinct `(state, incarnation)` each
  /// `(observer, subject)` row passed through, in chronological order, with the
  /// `pruned` flag set on a `reset_nodes` removal. Cleared at the start of each
  /// step and read by the three history checkers (incarnation-monotonic,
  /// no-resurrection, illegal-pair) via
  /// [`history_transitions`](Cluster::history_transitions). Sampling at the
  /// per-mutation boundary — not the post-step snapshot — is what catches an
  /// illegal intermediate transition that a later same-step mutation would
  /// otherwise overwrite (e.g. an in-place resurrection refuted to a legal
  /// higher incarnation by a later message of the same compound datagram).
  pub(crate) history_transitions: Vec<crate::checker::Transition>,
}

impl Cluster {
  /// Create an empty cluster with no nodes.
  pub fn new() -> Self {
    Self {
      net: Network::new(),
      clock: Clock::new(),
      streams: Vec::new(),
      policies: HashMap::new(),
      ack_payload: HashMap::new(),
      addrs: Vec::new(),
      ids: Vec::new(),
      last_incarnation: HashMap::new(),
      node_cfg: HashMap::new(),
      pruned_this_step: HashSet::new(),
      history_transitions: Vec::new(),
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
      ep.set_ack_payload(payload.clone())
        .expect("sim ack payload must fit the gossip budget");
      // Record the authoritative copy so a later restart re-installs it onto
      // the fresh endpoint (a delegate-configured behavior, like the admission
      // policy). Only store after the endpoint accepted it, so an oversized
      // payload that panicked above is never recorded.
      self.ack_payload.insert(host, payload);
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
  /// The `config_fn` receives the default [`EndpointOptions`] and may chain
  /// builder methods (e.g. `.with_dead_node_reclaim_time(...)`) before
  /// returning it. Useful for tests that need non-default knobs such as
  /// `dead_node_reclaim_time`.
  pub fn add_node_with(
    &mut self,
    id: SmolStr,
    addr: SocketAddr,
    config_fn: impl FnOnce(EndpointOptions<SmolStr, SocketAddr>) -> EndpointOptions<SmolStr, SocketAddr>,
  ) -> SocketAddr {
    self.ids.push(id.clone());
    self.addrs.push(addr);
    let cfg = Self::endpoint_cfg(id, addr, 1);
    let cfg = config_fn(cfg);
    // Record the FINAL per-node config so a later `restart` rebuilds the
    // endpoint with the same meta / timeouts / every other knob, only bumping
    // the incarnation.
    self.node_cfg.insert(addr, cfg.clone());
    self.net.add_endpoint(cfg, self.clock.now());
    addr
  }

  /// Build the canonical `EndpointOptions` for a node with the given
  /// `initial_incarnation`. Used by both `add_node_with` and `restart` so the
  /// builder chain is never duplicated.
  fn endpoint_cfg(
    id: SmolStr,
    addr: SocketAddr,
    initial_incarnation: u32,
  ) -> EndpointOptions<SmolStr, SocketAddr> {
    EndpointOptions::new(id, addr)
      .with_initial_incarnation(initial_incarnation)
      .with_gossip_interval(Duration::from_millis(200))
      .with_push_pull_interval(Duration::from_secs(30))
      .with_probe_interval(Duration::from_millis(1000))
      .with_probe_timeout(Duration::from_millis(500))
      .with_suspicion_mult(4)
      .with_retransmit_mult(4)
  }

  // ── Crash / restart ───────────────────────────────────────────────────────

  /// Take the node at `addr` dark at the network layer: stop ticking it, stop
  /// delivering to it, and drop all queued datagrams and in-flight streams
  /// touching it. Does NOT record a restart incarnation. Models a process that
  /// has exited (e.g. after a graceful leave). Idempotent; no-op for an
  /// unknown address.
  pub fn shutdown(&mut self, addr: SocketAddr) {
    if !self.net.endpoints.contains_key(&addr) {
      return;
    }
    self.net.crashed.insert(addr);
    // A datagram already sent by a node is in the network and survives that
    // node crashing; only datagrams destined to the stopped node are purged.
    // Delivery-time checks (partitioned / crashed destination) decide whether
    // the in-flight datagrams from the crashed sender ultimately arrive.
    self.net.queue.retain(|d| d.to != addr);
    self
      .streams
      .retain(|vs| vs.initiator_addr != addr && vs.acceptor_addr != addr);
  }

  /// Simulate a hard crash of the node at `addr`.
  ///
  /// After this call the node's endpoint is frozen: it is not ticked, does
  /// not transmit, and drops all inbound datagrams. Queued datagrams to or
  /// from `addr` are purged immediately. Peers that keep probing `addr` will
  /// see their probes time out, suspect the node, and eventually mark it Dead.
  ///
  /// The local incarnation at the moment of crash is recorded so that
  /// [`restart`](Cluster::restart) can compute a superseding incarnation.
  ///
  /// Does nothing if `addr` is not a known endpoint or is already crashed.
  pub fn crash(&mut self, addr: SocketAddr) {
    if !self.net.endpoints.contains_key(&addr) {
      return;
    }
    // Snapshot the local incarnation before freezing.
    if let Some(inc) = self.local_incarnation(addr) {
      self.last_incarnation.insert(addr, inc);
    }
    self.shutdown(addr);
  }

  /// Restart the node at `addr` as a fresh endpoint at a strictly higher
  /// incarnation than any incarnation the cluster has seen for this node,
  /// WITHOUT initiating a rejoin — the caller drives the node back into the
  /// cluster. [`restart`](Cluster::restart) wraps this with an automatic rejoin.
  ///
  /// The stale endpoint is replaced in-place (the `addrs`/`ids` registration is
  /// unchanged). Once the node rejoins and enough [`step`](Cluster::step) calls
  /// pass, peers that declared it Dead accept the fresh Alive as superseding it.
  ///
  /// Returns `true` if the node was restarted. Returns `false` without
  /// modifying the cluster when `addr` is unregistered, or when the superseding
  /// incarnation (`max observed + 1`) would exceed the safe initial-incarnation
  /// range — `with_initial_incarnation` reserves the upper half of the `u32`
  /// space as wrap headroom. That case is unreachable under the VOPR's small
  /// incarnations; refusing it keeps an upper-half observed or pre-seeded
  /// incarnation from turning a restart into a panic.
  #[must_use]
  pub fn restart_without_join(&mut self, addr: SocketAddr) -> bool {
    // Find the node id (addrs/ids are parallel).
    let Some(i) = self.addrs.iter().position(|a| *a == addr) else {
      return false;
    };
    let id = self.ids[i].clone();

    // Compute an incarnation that supersedes every observed incarnation for
    // this node across the whole cluster, plus the pre-crash local value.
    let max_obs = self
      .addrs
      .iter()
      .copied()
      .filter_map(|o| self.get_node_incarnation(o, &id))
      .max()
      .unwrap_or(0);
    let prev = self.last_incarnation.get(&addr).copied().unwrap_or(0);
    // Supersede every observed and the pre-crash incarnation. Refuse (rather
    // than panic) if that would leave the safe initial range: the wrapping bump
    // paths make a near-overflow incarnation unrefutable, so the builder accepts
    // only the lower half. Unreachable for the VOPR's small incarnations;
    // reachable only via a pre-seeded or injected upper-half incarnation.
    let Some(next_inc) = max_obs.max(prev).checked_add(1) else {
      return false;
    };
    if next_inc > u32::MAX / 2 {
      return false;
    }

    self.net.crashed.remove(&addr);

    // Replace with a fresh endpoint at the superseding incarnation. Rebuild
    // from the node's STORED config (preserving its initial meta and every
    // other knob set at `add_node_with` time) rather than the bare default, so
    // a restart reincarnates the same node — not a differently-configured one.
    // Falls back to the canonical default config for a node added before the
    // per-node config map existed (none in practice).
    let cfg = self
      .node_cfg
      .get(&addr)
      .cloned()
      .unwrap_or_else(|| Self::endpoint_cfg(id.clone(), addr, 1))
      .with_initial_incarnation(next_inc);
    self.net.add_endpoint(cfg, self.clock.now());

    // Re-install delegate-configured behaviors onto the fresh endpoint. The
    // authoritative copies survive the crash in `Cluster` maps, but the
    // delegates themselves lived on the endpoint we just replaced. A real
    // restarted process re-installs its delegates from config, so anything
    // configured at the application boundary must be reapplied here:
    //   - admission policy (reject_alives / reject_merges) via `apply_policy`;
    //   - the `PingDelegate::ack_payload` via `ack_payload`.
    // Runtime VIEW state and runtime MUTATIONS are deliberately NOT preserved,
    // matching a fresh process: injected alives/suspects/deads and merged
    // remote state (a clean membership view), a runtime `set_meta` update (the
    // restart reverts to the configured initial meta from `node_cfg`, as a
    // restarted process re-reads its config), and the Lifeguard awareness score
    // from `degrade_health`/`improve_health` (a fresh node starts healthy).
    self.apply_policy(addr);
    if let Some(payload) = self.ack_payload.get(&addr).cloned() {
      if let Some(ep) = self.net.endpoints.get_mut(&addr) {
        ep.set_ack_payload(payload)
          .expect("sim ack payload must fit the gossip budget");
      }
    }

    true
  }

  /// Restart the node at `addr` and immediately initiate a rejoin push-pull
  /// toward the first live peer — the chaos-phase restart, modeling a process
  /// that comes back up and re-contacts a seed. The calm phase instead uses
  /// [`restart_without_join`](Cluster::restart_without_join) so the single
  /// post-settle reseed is a restarted node's only join, and a join that
  /// completes without converging the node cannot be masked by a second attempt.
  ///
  /// Returns `true` if the node was restarted (see `restart_without_join`).
  #[must_use]
  pub fn restart(&mut self, addr: SocketAddr) -> bool {
    if !self.restart_without_join(addr) {
      return false;
    }
    // Initiate a join toward the first live peer.
    let seed = self
      .addrs
      .iter()
      .copied()
      .find(|&s| s != addr && !self.net.crashed.contains(&s));
    if let Some(seed) = seed {
      self.join(addr, seed);
    }
    true
  }

  /// Run one simulation tick. Returns `true` if anything happened.
  pub fn step(&mut self) -> bool {
    let now = self.clock.now();

    // Reset the per-step prune record; it is repopulated below and read by the
    // history checkers after this step returns.
    //
    // The transition log is NOT cleared here. It records EVERY distinct
    // `(state, incarnation)` each (observer, subject) row passes through, in
    // order, so an illegal intermediate value overwritten by a later mutation is
    // observed rather than masked by the post-step value. `step` APPENDS to it;
    // the owner clears it at the tick boundary via
    // [`clear_history_transitions`](Cluster::clear_history_transitions) so the
    // log spans both the chooser-phase mutations (e.g. a graceful `leave`) and
    // this step's mutations, read together by the checkers, before the next
    // tick clears it.
    self.pruned_this_step.clear();

    // 1. Drain transmits from all endpoints → enqueue datagrams.
    self.net.drain_transmits(now);

    // 2. Process DialRequested events → establish stream pairs. Dial
    //    establishment/failure mints or retires streams; it never merges remote
    //    membership inline (the merge happens in phase 3 when the resulting
    //    push-pull EndpointEvent is routed), so it produces no history
    //    transition and needs no recording here.
    let new_streams = self.net.process_dial_requests(&mut self.streams, now);

    // 3. Pipe bytes between active streams, recording every per-event change to
    //    the receiving observer's view. A push-pull stream event merges remote
    //    state inline, so two same-step stream events to the same observer can
    //    each mutate the same (observer, subject) row; recording at the per-event
    //    boundary surfaces an illegal intermediate that a later same-step event
    //    would otherwise overwrite — the reliable-plane sibling of the
    //    per-message datagram recorder below.
    {
      let subjects = self.ids.clone();
      self.net.step_streams_recording(
        &mut self.streams,
        now,
        &subjects,
        &mut self.history_transitions,
      );
    }

    // 4. Drain remaining non-DialRequested events (admission already applied
    //    inline by each host's installed AliveDelegate / MergeDelegate). This
    //    only re-enqueues events for `poll_event` observation; it mutates no
    //    membership, so no transition is recorded.
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
      None => {
        // Same early-out as below. The stream-phase transitions were already
        // recorded per event by `step_streams_recording`, so there is nothing
        // further to record before returning.
        return events_fired || new_streams;
      }
    };
    if next > now {
      self.clock.advance(next - now);
    }
    let now = self.clock.now();

    // 6. Deliver matured datagrams, recording every per-message change to the
    //    receiving observer's view. A compound datagram is dispatched
    //    message-by-message with a snapshot between messages, so an illegal
    //    intermediate carried by one message and overwritten by a later one is
    //    captured — the masking class this whole log exists to catch.
    let delivered = {
      let subjects = self.ids.clone();
      self
        .net
        .deliver_ready_recording(now, &subjects, &mut self.history_transitions)
    };

    // Snapshot every (observer, subject) pair the observer currently sees as
    // terminal (Dead/Left), IMMEDIATELY before `tick_all` runs `reset_nodes`.
    // `reset_nodes` is the machine's only pruning path and removes only
    // terminal members, so a pair that is terminal here and absent right after
    // `tick_all` was pruned this step. Index loops (not iterator chains) so the
    // `member_liveness(&self.net, …)` reads don't conflict with a live borrow of
    // `self.addrs` / `self.ids`.
    let mut pre_terminal: Vec<(SocketAddr, SmolStr)> = Vec::new();
    for oi in 0..self.addrs.len() {
      let observer = self.addrs[oi];
      for si in 0..self.ids.len() {
        let subject = self.ids[si].clone();
        if matches!(
          self.member_liveness(observer, &subject),
          Some(State::Dead) | Some(State::Left)
        ) {
          pre_terminal.push((observer, subject));
        }
      }
    }

    // The row state right after datagram delivery (before `tick_all`). The
    // tick-phase transitions are diffed against this so they are not
    // double-counted with the per-message datagram transitions just recorded.
    let pre_tick = self.snapshot_views();

    // 7. Tick all endpoints (runs each endpoint's `handle_timeout`, which fires
    //    `reset_nodes` on a full probe round-robin pass).
    self.net.tick_all(now);

    // `tick_all` is the final phase of `step`, so this "after" snapshot is taken
    // before any subsequent `process_dial_requests` (which runs in phase 2 of
    // the NEXT step). A pair that was terminal before `tick_all` and is absent
    // now was pruned by `reset_nodes`; record it. This also captures the
    // intra-tick prune+re-learn case across the step boundary: the re-learn
    // arrives in a later step, so the absence recorded here is what the checker
    // keys its baseline clear on.
    for (observer, subject) in pre_terminal {
      if self.member_liveness(observer, &subject).is_none() {
        self.pruned_this_step.insert((observer, subject));
      }
    }

    // Record the tick-phase transitions (suspicion → Dead, and prunes). A pair
    // that went terminal → absent via `reset_nodes` carries `pruned == true`, so
    // the history checkers clear its baseline exactly as the post-step path did.
    self.record_transitions(&pre_tick);

    events_fired || new_streams || delivered > 0
  }

  /// Snapshot every `(observer, subject)` row's `(state, incarnation)` in
  /// `addrs`-major, `ids`-minor order. The flat layout is parallel to the same
  /// nested iteration used by [`record_transitions`](Self::record_transitions),
  /// so a diff walks the two snapshots index-for-index. The fixed order keeps
  /// the recorded transition order deterministic.
  fn snapshot_views(&self) -> Vec<(Option<State>, Option<u32>)> {
    let mut out = Vec::with_capacity(self.addrs.len() * self.ids.len());
    for &observer in &self.addrs {
      for subject in &self.ids {
        out.push((
          self.member_liveness(observer, subject),
          self.get_node_incarnation(observer, subject),
        ));
      }
    }
    out
  }

  /// Diff the current view against `before` (an earlier
  /// [`snapshot_views`](Self::snapshot_views)) and append a
  /// [`Transition`](crate::checker::Transition) for every `(observer, subject)`
  /// whose `(state, incarnation)` changed. A pair that went terminal → absent
  /// via `reset_nodes` this step is flagged `pruned`. Iterates in the same fixed
  /// `addrs`-major, `ids`-minor order as the snapshot (via indices, so the
  /// read-only `member_liveness` borrows don't conflict with appending to the
  /// log), so the appended transitions are deterministic.
  fn record_transitions(&mut self, before: &[(Option<State>, Option<u32>)]) {
    let mut idx = 0;
    for oi in 0..self.addrs.len() {
      let observer = self.addrs[oi];
      for si in 0..self.ids.len() {
        let subject = self.ids[si].clone();
        let now_state = self.member_liveness(observer, &subject);
        let now_inc = self.get_node_incarnation(observer, &subject);
        if before[idx] != (now_state, now_inc) {
          let pruned =
            now_state.is_none() && self.pruned_this_step.contains(&(observer, subject.clone()));
          self
            .history_transitions
            .push(crate::checker::Transition::new(
              observer, subject, now_state, now_inc, pruned,
            ));
        }
        idx += 1;
      }
    }
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

  /// Project `peer`'s full server tuple as seen by `host`:
  /// `(meta bytes, protocol_version, delegate_version)`.
  ///
  /// `None` if `host` or `peer` is unknown.
  pub fn member_meta(&self, host: SocketAddr, peer: &SmolStr) -> Option<(Vec<u8>, u8, u8)> {
    let ns = self.member(host, peer)?;
    Some((
      ns.meta_ref().as_bytes().to_vec(),
      u8::from(ns.protocol_version()),
      u8::from(ns.delegate_version()),
    ))
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
    ep.handle_packet(host, Message::Alive(alive), now);
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
    use memberlist_proto::typed::Suspect;
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    let s = Suspect::new(incarnation, target_id, from_id);
    ep.handle_packet(host, Message::Suspect(s), now);
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
  ) -> Option<memberlist_proto::typed::State> {
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
    use memberlist_proto::PushPullKind;
    let now = self.clock.now();
    if let Some(ep) = self.net.endpoints.get_mut(&host) {
      ep.start_push_pull(peer_addr, PushPullKind::Refresh, now);
    }
  }

  // ── Legacy-API mutators (state injection — bypass network) ───────────────
  //
  // VOPR history-checker contract: these direct state-injection mutators
  // ([`alive_node`](Self::alive_node) after bootstrap, [`suspect_node`](Self::suspect_node),
  // [`dead_node`](Self::dead_node), [`merge_remote_state`](Self::merge_remote_state),
  // and likewise [`set_meta`](Self::set_meta) and [`advance`](Self::advance))
  // mutate membership OUTSIDE the per-tick transition recorder, so the changes
  // they make are NOT appended to [`history_transitions`](Self::history_transitions)
  // and are therefore outside the VOPR history checkers' contract. Only the
  // `step`-phase mutations and the VOPR chooser's [`leave`](Self::leave) are
  // recorded. The VOPR campaign does not call these helpers (its bootstrap uses
  // `alive_node` BEFORE the baseline is seeded, captured by the seed snapshot),
  // so they stay deliberately recording-free; tests that drive the history
  // checkers must restrict themselves to recorded mutations.

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
    // paths route through `handle_packet` identically.
    let _ = bootstrap;
    ep.handle_packet(host, Message::Alive(alive), now);
  }

  /// Inject a [`Suspect`] message at `host` (legacy `Memberlist::suspect_node`).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn suspect_node(&mut self, host: SocketAddr, suspect: Suspect<SmolStr>) {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    ep.handle_packet(host, Message::Suspect(suspect), now);
  }

  /// Inject a [`Dead`] message at `host` (legacy `Memberlist::dead_node`).
  ///
  /// Does nothing if `host` is not a known endpoint.
  pub fn dead_node(&mut self, host: SocketAddr, dead: Dead<SmolStr>) {
    let now = self.clock.now();
    let Some(ep) = self.net.endpoints.get_mut(&host) else {
      return;
    };
    ep.handle_packet(host, Message::Dead(dead), now);
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

  // ── Checker / iteration helpers ──────────────────────────────────────────

  /// Whether `reset_nodes` pruned `observer`'s terminal entry for `subject`
  /// during the most recent [`step`](Cluster::step).
  ///
  /// Used by the terminal checkers to expire a `Dead`/`Left` baseline on the
  /// actual prune event rather than on elapsed time. See
  /// [`pruned_this_step`](Self::pruned_this_step).
  pub fn was_pruned_this_step(&self, observer: SocketAddr, subject: &SmolStr) -> bool {
    self.pruned_this_step.contains(&(observer, subject.clone()))
  }

  /// The per-mutation history transitions recorded since the last
  /// [`clear_history_transitions`](Cluster::clear_history_transitions), in
  /// chronological order.
  ///
  /// The owner clears the log at the tick boundary, so it accumulates every
  /// recorded mutation of the tick: the chooser-phase mutations applied before
  /// [`step`](Cluster::step) (a graceful [`leave`](Cluster::leave)) followed by
  /// the mutations `step` itself records. The three history checkers
  /// (incarnation-monotonic, no-resurrection, illegal-pair) consume this via
  /// [`observe_transitions`](crate::checker::IncarnationMonotonicChecker::observe_transitions)
  /// instead of sampling the post-step snapshot, so an illegal intermediate
  /// `(state, incarnation)` overwritten by a later same-tick mutation is still
  /// observed. See [`history_transitions`](Self::history_transitions) (the
  /// field) for the recording contract.
  pub fn history_transitions(&self) -> &[crate::checker::Transition] {
    &self.history_transitions
  }

  /// Clear the per-mutation history transition log.
  ///
  /// The log owner (the VOPR tick loop) calls this at the START of each tick,
  /// before applying chooser-phase mutations and running [`step`](Cluster::step),
  /// so the log it later reads via
  /// [`history_transitions`](Cluster::history_transitions) spans exactly one
  /// tick: the chooser mutations plus that tick's step mutations. `step` itself
  /// only APPENDS, so a chooser-phase membership mutation (a graceful
  /// [`leave`](Cluster::leave)) recorded before `step` is not wiped by it.
  pub fn clear_history_transitions(&mut self) {
    self.history_transitions.clear();
  }

  /// All registered node addresses, in insertion order.
  pub fn addrs(&self) -> &[SocketAddr] {
    &self.addrs
  }

  /// All registered node ids, in insertion order.
  pub fn ids(&self) -> &[SmolStr] {
    &self.ids
  }

  /// Number of queued datagrams destined for `addr` (test introspection).
  pub fn queued_to(&self, addr: SocketAddr) -> usize {
    self.net.queue.iter().filter(|d| d.to == addr).count()
  }

  /// Datagrams ACTUALLY probabilistically dropped over this cluster's lifetime
  /// (the `drop_per_mille` roll fired). Excludes crash/partition drops.
  pub fn fault_drops(&self) -> u64 {
    self.net.fault_drops
  }

  /// Datagrams ACTUALLY probabilistically duplicated over this cluster's
  /// lifetime (the `duplicate_per_mille` roll fired).
  pub fn fault_duplicates(&self) -> u64 {
    self.net.fault_duplicates
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

  /// Whether `host`'s endpoint is still in the Running lifecycle (not leaving or
  /// left). `false` for an unknown host.
  pub fn is_running(&self, host: SocketAddr) -> bool {
    self
      .net
      .endpoints
      .get(&host)
      .is_some_and(|ep| ep.is_running())
  }

  /// Whether `addr`'s endpoint is currently darkened in the fault model — a hard
  /// crash that has not been restarted, or a shutdown. Harness ground truth,
  /// independent of the membership views peers may still hold for the node.
  pub fn is_crashed(&self, addr: SocketAddr) -> bool {
    self.net.crashed.contains(&addr)
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

  /// Returns `true` if a one-shot datagram drop is still armed for `addr`.
  ///
  /// Useful in tests that need to confirm a stream dial did NOT consume the
  /// one-shot token.
  pub fn drop_next_armed(&self, addr: SocketAddr) -> bool {
    self.net.faults.drop_next.contains(&addr)
  }

  /// Set a fixed latency added to every subsequently enqueued datagram.
  pub fn set_latency(&mut self, latency: Duration) {
    self.net.faults.latency = latency;
  }

  /// Seed the network fault RNG (independent of per-endpoint RNGs).
  pub fn seed_faults(&mut self, seed: u64) {
    self.net.seed_faults(seed);
  }

  /// Set the probabilistic per-datagram drop rate (per mille).
  pub fn set_drop_per_mille(&mut self, v: u32) {
    self.net.faults.drop_per_mille = v;
  }

  /// Set a ONE-WAY (asymmetric) per-datagram drop rate (per mille) on the
  /// `from -> to` link, on top of the global rate. Setting `(a, b)` without
  /// `(b, a)` models a half-open failure: `a` hears `b` but `b` never hears `a`.
  pub fn set_directional_drop_per_mille(&mut self, from: SocketAddr, to: SocketAddr, v: u32) {
    self
      .net
      .faults
      .directional_drop_per_mille
      .insert((from, to), v);
  }

  /// Set the probabilistic per-datagram duplicate rate (per mille).
  pub fn set_duplicate_per_mille(&mut self, v: u32) {
    self.net.faults.duplicate_per_mille = v;
  }

  /// Set the max per-datagram delivery jitter (reorder source).
  pub fn set_jitter(&mut self, d: Duration) {
    self.net.faults.jitter = d;
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
      // Ignoring Err: a leaving/left host issues no ping; the harness treats
      // that as a no-op.
      let _ = ep.ping(Node::new(peer_id, peer_addr), now);
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
    use memberlist_proto::PushPullKind;
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
  pub fn leave(&mut self, host: SocketAddr) -> Result<(), memberlist_proto::Error> {
    let now = self.clock.now();
    if !self.net.endpoints.contains_key(&host) {
      return Ok(());
    }

    // Snapshot the host's OWN view of every subject before `ep.leave`, which
    // marks the local member `Left` (a membership mutation no `step` phase
    // records). Iterate `self.ids` in order so the appended transitions are
    // deterministic. The chooser calls `leave` BEFORE `step`, and the tick loop
    // clears the log only at the tick boundary, so a transition appended here is
    // read by the history checkers alongside the same tick's step transitions.
    // Without it the checkers never see the leaver's `Alive -> Left`, leaving a
    // stale `Alive` baseline against which a later same-incarnation resurrection
    // at the leaver's own row would be missed.
    let before: Vec<(Option<State>, Option<u32>)> = self
      .ids
      .iter()
      .map(|subject| {
        (
          self.member_liveness(host, subject),
          self.get_node_incarnation(host, subject),
        )
      })
      .collect();

    let ep = self
      .net
      .endpoints
      .get_mut(&host)
      .expect("endpoint presence checked above");
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

      // Record every `(host, subject)` row whose `(state, incarnation)` the
      // leave changed — in practice the host's own `Alive -> Left` row. `leave`
      // is not a prune, so `pruned == false`. Same fixed `self.ids` order as the
      // before-snapshot.
      for (subject, &before_row) in self.ids.iter().zip(before.iter()) {
        let now_state = self.member_liveness(host, subject);
        let now_inc = self.get_node_incarnation(host, subject);
        if before_row != (now_state, now_inc) {
          self
            .history_transitions
            .push(crate::checker::Transition::new(
              host,
              subject.clone(),
              now_state,
              now_inc,
              false,
            ));
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
    meta: memberlist_proto::typed::Meta,
  ) -> Result<(), memberlist_proto::Error> {
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
      memberlist_proto::typed::Message::UserData(data),
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
      // Ignoring Err: a leaving/left host starts no new reliable dial; the
      // harness treats that as a no-op.
      let _ = ep.start_user_message(peer_addr, data, now);
    }
  }

  // ── Health / awareness helpers ───────────────────────────────────────────────

  /// Current Lifeguard health score at `host` (legacy `Memberlist::health_score`).
  ///
  /// `0` is fully healthy; higher values mean worse, capped at
  /// `awareness_max_multiplier - 1` (default 8 - 1 = 7).
  ///
  /// Returns `None` if `host` is not a known endpoint.
  pub fn health_score(&self, host: SocketAddr) -> Option<usize> {
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
