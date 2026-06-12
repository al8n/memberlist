use super::*;
use crate::clock::Clock;
use memberlist_proto::{
  EndpointOptions,
  typed::{Message, Node, Ping},
};
use smol_str::SmolStr;
use std::time::Duration;

fn make_cfg(id: &str, port: u16) -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(
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

/// The per-message recorder must surface EVERY distinct value an observer's
/// view of a subject passes through inside one compound datagram — not just
/// the final one. A compound carrying `[Alive@5, Alive@6]` about the same
/// subject drives the observer Alive@1 -> Alive@5 -> Alive@6 within one
/// delivery; the intermediate Alive@5 is exactly what a post-step snapshot
/// would mask, so the recorder must record BOTH transitions, in order.
#[test]
fn recorder_captures_compound_internal_intermediate() {
  use memberlist_proto::typed::{Alive, State};
  let mut net = Network::new();
  let clk = Clock::new();
  let now = clk.now();
  let bob: SocketAddr = "127.0.0.1:7102".parse().unwrap();
  let carol: SocketAddr = "127.0.0.1:7103".parse().unwrap();
  net.add_endpoint(make_cfg("bob", 7102), now);
  let carol_id = SmolStr::new("carol");

  // Seed bob's view of carol at Alive@1 (the baseline the compound supersedes).
  {
    let ep = net.endpoints.get_mut(&bob).unwrap();
    ep.handle_packet(
      carol,
      Message::Alive(Alive::new(1, Node::new(carol_id.clone(), carol))),
      now,
    );
  }
  assert_eq!(net.endpoints[&bob].node_incarnation(&carol_id), Some(1));

  // A compound FROM carol carrying two superseding Alives about carol.
  net.enqueue_datagram(
    carol,
    bob,
    vec![
      Message::Alive(Alive::new(5, Node::new(carol_id.clone(), carol))),
      Message::Alive(Alive::new(6, Node::new(carol_id.clone(), carol))),
    ],
    now,
  );

  let subjects = vec![carol_id.clone()];
  let mut transitions = Vec::new();
  let delivered = net.deliver_ready_recording(now, &subjects, &mut transitions);
  assert_eq!(delivered, 1, "the compound is one delivery unit");

  // BOTH intermediate values recorded, in dispatch order — the masked Alive@5
  // is present, not collapsed into the final Alive@6.
  assert_eq!(
    transitions.len(),
    2,
    "the per-message recorder must record both Alive@5 and Alive@6, not just the final value"
  );
  assert_eq!(transitions[0].observer(), bob);
  assert_eq!(transitions[0].subject(), &carol_id);
  assert_eq!(transitions[0].state(), Some(State::Alive));
  assert_eq!(
    transitions[0].incarnation(),
    Some(5),
    "the masked intermediate must be recorded"
  );
  assert!(!transitions[0].pruned(), "a datagram never prunes");
  assert_eq!(transitions[1].state(), Some(State::Alive));
  assert_eq!(transitions[1].incarnation(), Some(6));
}

/// The reliable-plane sibling of `recorder_captures_compound_internal_intermediate`.
/// Two same-step push-pull stream events to the SAME observer each merge the
/// same subject inline (`Endpoint::handle_stream_event`), so the observer's row
/// passes through two distinct values in one `step_streams` pump. A single
/// post-phase diff would collapse them to the final value — exactly the
/// masking on the reliable stream path. `step_streams_recording` snapshots
/// each routed event, so BOTH intermediates are recorded, in order; were the
/// first an illegal value (a resurrection / regression) masked by the second,
/// it would be surfaced to the history checkers rather than hidden.
#[test]
fn stream_recorder_captures_two_same_step_push_pull_intermediates() {
  use memberlist_proto::{
    PushPullKind,
    typed::{Alive, Node, State},
  };
  let mut net = Network::new();
  let clk = Clock::new();
  let now = clk.now();
  // Ports chosen so the BTreeMap-ordered dial processing routes c1 (carol@5)
  // before c2 (carol@6): the recorded intermediate order is deterministic.
  let bob: SocketAddr = "127.0.0.1:7202".parse().unwrap();
  let c1: SocketAddr = "127.0.0.1:7203".parse().unwrap();
  let c2: SocketAddr = "127.0.0.1:7204".parse().unwrap();
  net.add_endpoint(make_cfg("bob", 7202), now);
  net.add_endpoint(make_cfg("c1", 7203), now);
  net.add_endpoint(make_cfg("c2", 7204), now);
  let carol_id = SmolStr::new("carol");
  let carol_addr: SocketAddr = "127.0.0.1:7299".parse().unwrap();

  // c1 knows carol@5; c2 knows carol@6. carol is only a gossiped subject, not
  // a live endpoint. bob does not know carol yet.
  {
    let ep = net.endpoints.get_mut(&c1).unwrap();
    ep.handle_packet(
      carol_addr,
      Message::Alive(Alive::new(5, Node::new(carol_id.clone(), carol_addr))),
      now,
    );
  }
  {
    let ep = net.endpoints.get_mut(&c2).unwrap();
    ep.handle_packet(
      carol_addr,
      Message::Alive(Alive::new(6, Node::new(carol_id.clone(), carol_addr))),
      now,
    );
  }
  assert!(net.endpoints[&bob].node_incarnation(&carol_id).is_none());

  // Both peers initiate a Refresh push-pull toward bob in the same instant.
  // (Refresh, not Join, so no MergeDelegate gate intercepts the merge.)
  net
    .endpoints
    .get_mut(&c1)
    .unwrap()
    .start_push_pull(bob, PushPullKind::Refresh, now);
  net
    .endpoints
    .get_mut(&c2)
    .unwrap()
    .start_push_pull(bob, PushPullKind::Refresh, now);

  // Establish both stream pairs (c1→bob, c2→bob) from the queued dials.
  let mut streams = Vec::new();
  net.process_dial_requests(&mut streams, now);
  assert_eq!(
    streams.len(),
    2,
    "both push-pull dials must establish a stream pair"
  );

  // One pump routes BOTH requests' PushPullRequestReceived into bob: bob's
  // view of carol passes 5 -> 6 within this single call.
  let subjects = vec![carol_id.clone()];
  let mut transitions = Vec::new();
  net.step_streams_recording(&mut streams, now, &subjects, &mut transitions);

  // bob ends knowing carol@6 (the higher, final value).
  assert_eq!(
    net.endpoints[&bob].node_incarnation(&carol_id),
    Some(6),
    "bob's view converges to the higher incarnation"
  );

  // BOTH intermediates recorded, in dispatch order — the masked carol@5 is
  // present, not collapsed into the final carol@6. A single post-phase diff
  // would have recorded only carol@6.
  let carol_transitions: Vec<_> = transitions
    .iter()
    .filter(|t| t.subject() == &carol_id)
    .collect();
  assert_eq!(
    carol_transitions.len(),
    2,
    "the per-event stream recorder must record both carol@5 and carol@6, not just the final value: {transitions:?}"
  );
  assert_eq!(carol_transitions[0].observer(), bob);
  assert_eq!(carol_transitions[0].state(), Some(State::Alive));
  assert_eq!(
    carol_transitions[0].incarnation(),
    Some(5),
    "the masked intermediate must be recorded"
  );
  assert!(!carol_transitions[0].pruned(), "a stream never prunes");
  assert_eq!(carol_transitions[1].observer(), bob);
  assert_eq!(carol_transitions[1].state(), Some(State::Alive));
  assert_eq!(carol_transitions[1].incarnation(), Some(6));
  assert!(!carol_transitions[1].pruned(), "a stream never prunes");
}
