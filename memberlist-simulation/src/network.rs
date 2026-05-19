//! Virtual network: datagram queue and virtual stream pipes.

// pub(crate) items below will be consumed by `cluster.rs`.
// Until then the dead_code lint would fire; suppress it for this module.
#![allow(dead_code)]

use std::{
  collections::{HashMap, VecDeque},
  net::SocketAddr,
  time::{Duration, Instant},
};

use memberlist_machine::{
  CompoundTransmit, Endpoint, EndpointConfig, Event, PacketTransmit, Stream, StreamCommand,
  StreamError, Transmit,
};
use smol_str::SmolStr;

use crate::faults::FaultConfig;

/// A pending datagram in the virtual network.
///
/// Carries the typed `Message<SmolStr, SocketAddr>` directly — no byte-level
/// encode/decode round-trip. The simulation exercises the state machine, not
/// the codec.
#[derive(Debug)]
pub(crate) struct PendingDatagram {
  /// The simulated time at which this datagram may be delivered.
  pub deliver_at: Instant,
  /// Source address.
  pub from: SocketAddr,
  /// Destination address.
  pub to: SocketAddr,
  /// The fully-typed messages this datagram carries, dispatched IN ORDER on
  /// delivery. A plain `Packet` is one message; a `Compound` is its inner
  /// messages. Modeling the bundle (not per-message rows) keeps the
  /// datagram boundary faithful: fault/latency is applied ONCE per
  /// datagram, so a one-shot drop drops the whole compound atomically —
  /// exactly what a real UDP/QUIC compound datagram does (a real driver
  /// `encode_outgoing_compound`s into ONE `send_to`).
  pub messages: Vec<memberlist_wire::typed::Message<SmolStr, SocketAddr>>,
}

/// A virtual in-progress stream exchange between two endpoints.
pub(crate) struct VirtualStream {
  /// Address of the initiating endpoint.
  pub initiator_addr: SocketAddr,
  /// Stream object on the initiating side.
  pub initiator: Stream<SmolStr, SocketAddr>,
  /// Address of the accepting endpoint.
  pub acceptor_addr: SocketAddr,
  /// Stream object on the accepting side.
  pub acceptor: Stream<SmolStr, SocketAddr>,
}

/// Virtual network owning a set of endpoints and a datagram queue.
pub struct Network {
  /// Endpoints keyed by their advertise address.
  pub(crate) endpoints: HashMap<SocketAddr, Endpoint<SmolStr, SocketAddr>>,
  /// Datagrams waiting for `deliver_at <= now`.
  pub(crate) queue: VecDeque<PendingDatagram>,
  /// Fault injection configuration.
  pub(crate) faults: FaultConfig,
}

impl Network {
  /// Create an empty network with no faults.
  pub fn new() -> Self {
    Self {
      endpoints: HashMap::new(),
      queue: VecDeque::new(),
      faults: FaultConfig::none(),
    }
  }

  /// Add an endpoint with the given config, returning the advertise address.
  pub fn add_endpoint(
    &mut self,
    cfg: EndpointConfig<SmolStr, SocketAddr>,
    now: Instant,
  ) -> SocketAddr {
    let addr = *cfg.advertise_addr();
    let mut ep = Endpoint::new(cfg);
    ep.start_scheduling(now);
    self.endpoints.insert(addr, ep);
    addr
  }

  /// Enqueue a single-message datagram for delivery (plain `Packet` / the
  /// stream + cluster callers). Applies fault injection at enqueue time.
  pub(crate) fn enqueue(
    &mut self,
    from: SocketAddr,
    to: SocketAddr,
    message: memberlist_wire::typed::Message<SmolStr, SocketAddr>,
    deliver_at: Instant,
  ) {
    self.enqueue_datagram(from, to, vec![message], deliver_at);
  }

  /// Enqueue ONE datagram carrying `messages` (a plain `Packet` is one; a
  /// `Compound` is its inner messages). Fault injection + latency are
  /// applied ONCE for the whole datagram, so a one-shot drop / partition
  /// drops the entire compound atomically and the inner messages are
  /// delivered together in order — faithfully modeling a real UDP/QUIC
  /// compound datagram (the interim driver `encode_outgoing_compound`s
  /// into a single `send_to`). Per-message enqueue would let
  /// `drop_next_datagram_from` consume on the first inner message and
  /// still deliver the rest — an impossible delivery a real datagram
  /// cannot produce.
  pub(crate) fn enqueue_datagram(
    &mut self,
    from: SocketAddr,
    to: SocketAddr,
    messages: Vec<memberlist_wire::typed::Message<SmolStr, SocketAddr>>,
    deliver_at: Instant,
  ) {
    if messages.is_empty() {
      return;
    }
    if !self.faults.should_deliver(from, to) {
      return;
    }
    let deliver_at = deliver_at + self.faults.latency;
    self.queue.push_back(PendingDatagram {
      deliver_at,
      from,
      to,
      messages,
    });
  }

  /// Drain all datagrams whose `deliver_at <= now` and deliver them to the
  /// target endpoint. Returns the number of datagrams delivered.
  pub(crate) fn deliver_ready(&mut self, now: Instant) -> usize {
    let mut delivered = 0;
    // Sort queue by deliver_at so we process in order (queue is appended
    // roughly in order, but latency injection can skew it).
    self
      .queue
      .make_contiguous()
      .sort_unstable_by_key(|d| d.deliver_at);

    let mut remaining = VecDeque::new();
    while let Some(d) = self.queue.pop_front() {
      if d.deliver_at > now {
        remaining.push_back(d);
        break;
      }
      if let Some(ep) = self.endpoints.get_mut(&d.to) {
        // One datagram ⇒ one delivery unit: dispatch every carried
        // message in order, count the datagram once.
        for m in d.messages {
          dispatch_message(ep, d.from, m, now);
        }
        delivered += 1;
      }
    }
    // Drain rest that are not yet ready.
    while let Some(d) = self.queue.pop_front() {
      if d.deliver_at <= now {
        if let Some(ep) = self.endpoints.get_mut(&d.to) {
          for m in d.messages {
            dispatch_message(ep, d.from, m, now);
          }
          delivered += 1;
        }
      } else {
        remaining.push_back(d);
      }
    }
    self.queue = remaining;
    delivered
  }

  /// Earliest delivery deadline among all queued datagrams.
  pub(crate) fn earliest_datagram_deadline(&self) -> Option<Instant> {
    self.queue.iter().map(|d| d.deliver_at).min()
  }

  /// Earliest `poll_timeout` across all endpoints.
  pub(crate) fn earliest_endpoint_deadline(&self) -> Option<Instant> {
    self
      .endpoints
      .values()
      .filter_map(|ep| ep.poll_timeout())
      .min()
  }

  /// The next instant the simulation must wake at.
  pub(crate) fn next_deadline(&self) -> Option<Instant> {
    [
      self.earliest_datagram_deadline(),
      self.earliest_endpoint_deadline(),
    ]
    .into_iter()
    .flatten()
    .min()
  }

  /// Drain `poll_transmit` from every endpoint and enqueue the resulting
  /// datagrams in the virtual network. Call this at the start of each
  /// simulation step.
  pub(crate) fn drain_transmits(&mut self, now: Instant) {
    let addrs: Vec<SocketAddr> = self.endpoints.keys().copied().collect();
    for addr in addrs {
      // Collect all pending transmits before calling enqueue to avoid
      // holding a mutable borrow on self.endpoints while mutating self.queue.
      let mut pending: Vec<(
        SocketAddr,
        Vec<memberlist_wire::typed::Message<SmolStr, SocketAddr>>,
      )> = Vec::new();
      let ep = self.endpoints.get_mut(&addr).unwrap();
      while let Some(tx) = ep.poll_transmit() {
        match tx {
          // A plain Packet is one single-message datagram.
          Transmit::Packet(PacketTransmit { to, message }) => pending.push((to, vec![message])),
          // INTERIM (compound workstream): the sim has no wire codec, but a
          // compound is still ONE datagram — its inner messages are
          // delivered (or dropped) together, in order, as a real driver's
          // single encode_outgoing_compound `send_to` would. Per-message
          // enqueue would mis-model the datagram boundary for fault
          // injection.
          Transmit::Compound(CompoundTransmit { to, messages }) => pending.push((to, messages)),
        }
      }
      for (to, messages) in pending {
        self.enqueue_datagram(addr, to, messages, now);
      }
    }
  }

  /// Drain `poll_event` from every endpoint so progress is observable.
  /// Admission (Alive / join merge) is applied inline by each host's
  /// installed `AliveDelegate` / `MergeDelegate`; this method only needs to
  /// re-enqueue non-`DialRequested` events so `Cluster::poll_event` callers
  /// can still observe `NodeJoined` / `NodeLeft` / … . Returns `true` if any
  /// event fired.
  pub(crate) fn drain_events(&mut self, now: Instant) -> bool {
    let _ = now; // reserved for future event handlers that need timestamps
    let addrs: Vec<SocketAddr> = self.endpoints.keys().copied().collect();
    let mut any = false;
    for addr in addrs {
      let ep = self.endpoints.get_mut(&addr).unwrap();
      // Re-enqueue every event (including DialRequested, which
      // `process_dial_requests` consumes separately) so Cluster::poll_event
      // callers can still observe NodeJoined / NodeLeft / … .
      let mut deferred = Vec::new();
      while let Some(ev) = ep.poll_event() {
        any = true;
        deferred.push(ev);
      }
      for ev in deferred {
        ep.requeue_event(ev);
      }
    }
    any
  }

  /// Fire `handle_timeout(now)` on every endpoint.
  pub(crate) fn tick_all(&mut self, now: Instant) {
    for ep in self.endpoints.values_mut() {
      ep.handle_timeout(now);
    }
  }

  /// Process all `Event::DialRequested` events from every endpoint.
  ///
  /// For each dial request:
  /// - If fault-injected or the peer is unknown, calls `dial_failed`.
  /// - Otherwise, calls `dial_succeeded` on the initiator and `accept_stream`
  ///   on the target to create a `VirtualStream` pair.
  ///
  /// Returns `true` if any new streams were established.
  pub(crate) fn process_dial_requests(
    &mut self,
    streams: &mut Vec<VirtualStream>,
    now: Instant,
  ) -> bool {
    let addrs: Vec<SocketAddr> = self.endpoints.keys().copied().collect();
    let mut any = false;
    for addr in addrs {
      // Collect DialRequested before mutably borrowing individual endpoints;
      // re-enqueue every other event so Cluster::poll_event callers can
      // observe NodeJoined / NodeUpdated / NodeConflict etc.
      let pending: Vec<_> = {
        let ep = self.endpoints.get_mut(&addr).unwrap();
        let mut v = Vec::new();
        let mut deferred = Vec::new();
        while let Some(ev) = ep.poll_event() {
          match ev {
            Event::DialRequested {
              id,
              peer,
              deadline: _,
            } => {
              v.push((id, peer));
            }
            other => {
              deferred.push(other);
            }
          }
        }
        for ev in deferred {
          ep.requeue_event(ev);
        }
        v
      };
      for (id, peer_addr) in pending {
        // Fault injection: treat partitioned connections as dial failures.
        if !self.faults.should_deliver(addr, peer_addr) {
          let ep = self.endpoints.get_mut(&addr).unwrap();
          ep.dial_failed(id, StreamError::Timeout, now);
          continue;
        }
        // Unknown peer → dial failed.
        if !self.endpoints.contains_key(&peer_addr) {
          let ep = self.endpoints.get_mut(&addr).unwrap();
          ep.dial_failed(id, StreamError::Timeout, now);
          continue;
        }
        // Establish the virtual stream pair.
        let initiator_stream = {
          let ep = self.endpoints.get_mut(&addr).unwrap();
          ep.dial_succeeded(id, now)
        };
        let Some(initiator_stream) = initiator_stream else {
          continue;
        };
        let acceptor_stream = {
          let ep = self.endpoints.get_mut(&peer_addr).unwrap();
          ep.accept_stream(addr, now)
        };
        streams.push(VirtualStream {
          initiator_addr: addr,
          initiator: initiator_stream,
          acceptor_addr: peer_addr,
          acceptor: acceptor_stream,
        });
        any = true;
      }
    }
    any
  }

  /// Pipe bytes between active virtual streams each tick.
  ///
  /// For each stream pair:
  /// 1. Drain initiator → acceptor bytes via `poll_transmit` / `handle_data`.
  /// 2. Drain acceptor → initiator bytes via `poll_transmit` / `handle_data`.
  /// 3. Route `EndpointEvent`s from each stream back into its owning endpoint
  ///    and apply any returned `StreamCommand`.
  /// 4. Fire `handle_timeout(now)` on each stream.
  ///
  /// Completed (both sides done) and failed streams are removed from `streams`.
  pub(crate) fn step_streams(&mut self, streams: &mut Vec<VirtualStream>, now: Instant) {
    let mut buf = Vec::with_capacity(4096);
    for vs in streams.iter_mut() {
      // ── Initiator → Acceptor ────────────────────────────────────────────
      buf.clear();
      if vs.initiator.poll_transmit(now, &mut buf).is_some() && !buf.is_empty() {
        let _ = vs.acceptor.handle_data(&buf, now);
      }
      // ── Acceptor → Initiator ────────────────────────────────────────────
      buf.clear();
      if vs.acceptor.poll_transmit(now, &mut buf).is_some() && !buf.is_empty() {
        let _ = vs.initiator.handle_data(&buf, now);
      }
      // ── Route EndpointEvents (initiator) ────────────────────────────────
      while let Some(ep_ev) = vs.initiator.poll_endpoint_event() {
        if let Some(ep) = self.endpoints.get_mut(&vs.initiator_addr) {
          let cmd = ep.handle_stream_event(ep_ev, now);
          if let Some(cmd) = cmd {
            apply_stream_command(&mut vs.initiator, cmd, now);
          }
        }
      }
      // ── Route EndpointEvents (acceptor) ─────────────────────────────────
      while let Some(ep_ev) = vs.acceptor.poll_endpoint_event() {
        if let Some(ep) = self.endpoints.get_mut(&vs.acceptor_addr) {
          let cmd = ep.handle_stream_event(ep_ev, now);
          if let Some(cmd) = cmd {
            apply_stream_command(&mut vs.acceptor, cmd, now);
          }
        }
      }
      // ── Timeouts ────────────────────────────────────────────────────────
      vs.initiator.handle_timeout(now);
      vs.acceptor.handle_timeout(now);
    }
    // Remove streams where both sides are done, or either side has failed.
    streams.retain(|vs| {
      if vs.initiator.is_failed().is_some() || vs.acceptor.is_failed().is_some() {
        return false;
      }
      !(vs.initiator.is_done() && vs.acceptor.is_done())
    });
  }
}

impl Default for Network {
  fn default() -> Self {
    Self::new()
  }
}

/// Apply a `StreamCommand` returned by `Endpoint::handle_stream_event` to the
/// appropriate `Stream`.
///
/// For `SendPushPullResponse`: encodes the response payload and loads it into
/// the stream's output buffer via `stream_load_response`.
fn apply_stream_command(
  stream: &mut Stream<SmolStr, SocketAddr>,
  cmd: StreamCommand<SmolStr, SocketAddr>,
  now: Instant,
) {
  match cmd {
    StreamCommand::SendPushPullResponse {
      local_states,
      user_data,
    } => {
      let encoded =
        Endpoint::<SmolStr, SocketAddr>::encode_push_pull_response(&local_states, user_data, false);
      let deadline = now + Duration::from_secs(5);
      Endpoint::<SmolStr, SocketAddr>::stream_load_response(stream, encoded, deadline);
    }
    StreamCommand::Close => {
      // Stream will transition to Done on its own after sending/receiving.
    }
  }
}

/// Route a typed `Message` to the appropriate `Endpoint` handler.
///
/// PushPull and ErrorResponse arrive via the stream layer and are
/// ignored here. For now the `_` arm is necessary to remain non-exhaustive as
/// new variants may be added.
fn dispatch_message(
  ep: &mut Endpoint<SmolStr, SocketAddr>,
  from: SocketAddr,
  msg: memberlist_wire::typed::Message<SmolStr, SocketAddr>,
  now: Instant,
) {
  use memberlist_machine::Reliability;
  use memberlist_wire::typed::Message;
  match msg {
    Message::Alive(a) => ep.handle_alive(from, a, now),
    Message::Suspect(s) => ep.handle_suspect(from, s, now),
    Message::Dead(d) => ep.handle_dead(from, d, now),
    Message::Ping(p) => ep.handle_ping(from, p, now),
    Message::IndirectPing(ip) => ep.handle_indirect_ping(from, ip, now),
    Message::Ack(a) => ep.handle_ack(from, a, now),
    Message::Nack(n) => ep.handle_nack(from, n, now),
    Message::UserData(data) => {
      ep.handle_user_data(from, data, Reliability::Unreliable);
    }
    // PushPull / ErrorResponse arrive via the stream layer.
    // Route them through stream simulation when that is implemented.
    // The wildcard arm also handles any future variants added to the
    // non-exhaustive Message enum.
    Message::PushPull(_) | Message::ErrorResponse(_) | _ => {}
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::clock::Clock;
  use memberlist_machine::EndpointConfig;
  use memberlist_wire::typed::{Message, Node, Ping};
  use smol_str::SmolStr;
  use std::time::Duration;

  fn make_cfg(id: &str, port: u16) -> EndpointConfig<SmolStr, SocketAddr> {
    EndpointConfig::new(
      SmolStr::new(id),
      format!("127.0.0.1:{port}").parse().unwrap(),
    )
    .with_gossip_interval(Duration::from_millis(50))
    .with_push_pull_interval(Duration::from_secs(30))
    .with_probe_interval(Duration::from_millis(200))
  }

  #[test]
  fn two_endpoints_created_and_have_deadlines() {
    let mut net = Network::new();
    let clk = Clock::new();
    let now = clk.now();
    net.add_endpoint(make_cfg("alice", 7001), now);
    net.add_endpoint(make_cfg("bob", 7002), now);
    // Both endpoints should have scheduler deadlines → next_deadline is Some.
    assert!(net.next_deadline().is_some());
  }

  #[test]
  fn datagram_without_latency_delivered_immediately() {
    let mut net = Network::new();
    let clk = Clock::new();
    let now = clk.now();
    let a1: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    let a2: SocketAddr = "127.0.0.1:7002".parse().unwrap();
    net.add_endpoint(make_cfg("alice", 7001), now);
    net.add_endpoint(make_cfg("bob", 7002), now);

    // Construct a minimal Ping message so we have a real typed message.
    let ping = Ping::new(
      1,
      Node::new(SmolStr::new("alice"), a1),
      Node::new(SmolStr::new("bob"), a2),
    );
    let msg: Message<SmolStr, SocketAddr> = Message::Ping(ping);
    net.enqueue(a1, a2, msg, now);
    let delivered = net.deliver_ready(now);
    // The Ping is dispatched to bob's endpoint; delivered count must be 1.
    assert_eq!(delivered, 1);
  }

  #[test]
  fn datagram_with_latency_not_delivered_before_deadline() {
    let mut net = Network::new();
    let mut clk = Clock::new();
    let now = clk.now();
    let a1: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    let a2: SocketAddr = "127.0.0.1:7002".parse().unwrap();
    net.add_endpoint(make_cfg("alice", 7001), now);
    net.add_endpoint(make_cfg("bob", 7002), now);

    net.faults.latency = Duration::from_millis(50);

    let ping = Ping::new(
      2,
      Node::new(SmolStr::new("alice"), a1),
      Node::new(SmolStr::new("bob"), a2),
    );
    let msg: Message<SmolStr, SocketAddr> = Message::Ping(ping);
    net.enqueue(a1, a2, msg, now);

    // Before latency elapses: nothing delivered.
    assert_eq!(net.deliver_ready(now), 0);
    clk.advance(Duration::from_millis(50));
    assert_eq!(net.deliver_ready(clk.now()), 1);
  }

  /// A compound is ONE datagram: a one-shot `drop_next` must drop the WHOLE
  /// compound atomically — never deliver some inner messages while dropping
  /// others (an impossible delivery a real UDP/QUIC datagram cannot produce).
  #[test]
  fn compound_datagram_dropped_atomically_under_one_shot_drop() {
    let mut net = Network::new();
    let clk = Clock::new();
    let now = clk.now();
    let a1: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    let a2: SocketAddr = "127.0.0.1:7002".parse().unwrap();
    net.add_endpoint(make_cfg("alice", 7001), now);
    net.add_endpoint(make_cfg("bob", 7002), now);

    // Arm the one-shot drop for the next datagram FROM alice.
    net.faults.drop_next.insert(a1);

    // A compound datagram (2 inner messages) alice -> bob.
    let p1 = Ping::new(
      1,
      Node::new(SmolStr::new("alice"), a1),
      Node::new(SmolStr::new("bob"), a2),
    );
    let p2 = Ping::new(
      2,
      Node::new(SmolStr::new("alice"), a1),
      Node::new(SmolStr::new("bob"), a2),
    );
    net.enqueue_datagram(a1, a2, vec![Message::Ping(p1), Message::Ping(p2)], now);

    // The WHOLE compound is dropped — NONE of its inner messages
    // delivered, and the queue holds nothing (atomic drop, one datagram).
    assert_eq!(
      net.deliver_ready(now),
      0,
      "the one-shot drop must drop the entire compound, not just its first message"
    );
    assert!(net.queue.is_empty(), "dropped compound left nothing queued");

    // The one-shot was consumed by THAT compound (exactly once): a
    // subsequent datagram from alice is delivered.
    let p3 = Ping::new(
      3,
      Node::new(SmolStr::new("alice"), a1),
      Node::new(SmolStr::new("bob"), a2),
    );
    net.enqueue(a1, a2, Message::Ping(p3), now);
    assert_eq!(
      net.deliver_ready(now),
      1,
      "drop_next must have been consumed by the compound, not still pending"
    );
  }
}
