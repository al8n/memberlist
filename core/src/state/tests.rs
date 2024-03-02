use std::{
  net::SocketAddr,
  ops::Sub,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{lock::Mutex, Future, FutureExt, Stream};
use nodecraft::{resolver::AddressResolver, CheapClone, Id, Node};

use crate::{
  broadcast::Broadcast,
  delegate::{
    CompositeDelegate, Delegate, EventDelegate, EventKind, EventSubscriber,
    SubscribleEventDelegate, VoidDelegate,
  },
  error::Error,
  state::{AckManager, LocalNodeState},
  tests::get_memberlist,
  transport::Transport,
  types::{Ack, Alive, Dead, Epoch, Message, Nack, PushNodeState, State, Suspect},
  Memberlist, Options,
};

async fn host_memberlist<T, R>(
  t: T,
  opts: Options,
) -> Result<
  Memberlist<T>,
  Error<T, VoidDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
>
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  Memberlist::new_in(t, None, opts).await.map(|(_, _, t)| t)
}

async fn host_memberlist_with_delegate<D, T, R>(
  t: T,
  d: D,
  opts: Options,
) -> Result<Memberlist<T, D>, Error<T, D>>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  Memberlist::new_in(t, Some(d), opts)
    .await
    .map(|(_, _, t)| t)
}

/// Unit test to test the probe functionality
pub async fn probe<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(1000)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  // should ping addr2
  m1.probe().await;

  // Should not be marked suspect
  let nodes = m1.inner.nodes.read().await;
  let idx = *nodes.node_map.get(&m2.inner.id).unwrap();
  let n = &nodes.nodes[idx];
  assert_eq!(n.state.state, State::Alive);

  // Should increment seqno
  let sequence_number = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(sequence_number, 1, "bad seq no: {sequence_number}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe node suspect functionality
pub async fn probe_node_suspect<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t3: T,
  suspect_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let m3: Memberlist<T> = host_memberlist(t3, Options::lan()).await.unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  let a3 = Alive::new(1, m3.advertise_node());
  m1.alive_node(a3, None, false).await;

  let a4 = Alive::new(1, suspect_node.cheap_clone());
  m1.alive_node(a4, None, false).await;

  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(suspect_node.id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  let state = m1
    .inner
    .nodes
    .read()
    .await
    .get_state(suspect_node.id())
    .unwrap()
    .state;
  // Should be marked suspect.
  assert_eq!(state, State::Suspect, "bad state: {state}");
  R::sleep(Duration::from_millis(1000)).await;

  // One of the peers should have attempted an indirect probe.
  let s2 = m2.inner.hot.sequence_num.load(Ordering::SeqCst);
  let s3 = m3.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert!(
    s2 == 1 && s3 == 1,
    "bad seqnos, expected both to be 1: {s2}, {s3}"
  );

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

struct DogpileTestCase {
  name: &'static str,
  num_peers: usize,
  comfirmations: usize,
  expected: Duration,
}

/// Unit test to test the probe node dogpile functionality
pub async fn probe_node_dogpile<F, T, R>(
  mut get_transport: impl FnMut(usize) -> F,
  bad_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  F: Future<Output = T>,
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  const CASES: &[DogpileTestCase] = &[
    DogpileTestCase {
      name: "n=2, k=3 (max timeout disabled)",
      num_peers: 1,
      comfirmations: 0,
      expected: Duration::from_millis(500),
    },
    DogpileTestCase {
      name: "n=3, k=3",
      num_peers: 2,
      comfirmations: 0,
      expected: Duration::from_millis(500),
    },
    DogpileTestCase {
      name: "n=4, k=3",
      num_peers: 3,
      comfirmations: 0,
      expected: Duration::from_millis(500),
    },
    DogpileTestCase {
      name: "n=5, k=3 (max timeout starts to take effect)",
      num_peers: 4,
      comfirmations: 0,
      expected: Duration::from_millis(1000),
    },
    DogpileTestCase {
      name: "n=6, k=3 (5 0 1000)",
      num_peers: 5,
      comfirmations: 0,
      expected: Duration::from_millis(1000),
    },
    DogpileTestCase {
      name: "n=6, k=3 (confirmations start to lower timeout)",
      num_peers: 5,
      comfirmations: 1,
      expected: Duration::from_millis(750),
    },
    DogpileTestCase {
      name: "n=6, k=3 (5 2 604)",
      num_peers: 5,
      comfirmations: 2,
      expected: Duration::from_millis(604),
    },
    DogpileTestCase {
      name: "n=6, k=3 (timeout driven to nominal value)",
      num_peers: 5,
      comfirmations: 3,
      expected: Duration::from_millis(500),
    },
    DogpileTestCase {
      name: "n=6, k=3 (5 4 500)",
      num_peers: 5,
      comfirmations: 4,
      expected: Duration::from_millis(500),
    },
  ];

  for c in CASES.iter() {
    let t = get_transport(0).await;

    let m = host_memberlist(
      t,
      Options::lan()
        .with_probe_timeout(Duration::from_millis(1))
        .with_probe_interval(Duration::from_millis(100))
        .with_suspicion_mult(5)
        .with_suspicion_max_timeout_mult(2),
    )
    .await
    .unwrap();

    let a = Alive::new(1, m.advertise_node());

    m.alive_node(a, None, true).await;

    // Make all but one peer be an real, alive instance.
    let mut peers = vec![];
    for j in 0..c.num_peers - 1 {
      let t = get_transport(j + 1).await;
      let peer = host_memberlist(t, Options::lan()).await.unwrap();
      let a = Alive::new(1, peer.advertise_node());
      m.alive_node(a, None, false).await;
      peers.push(peer);
    }

    // Just use a bogus address for the last peer so it doesn't respond
    // to pings, but tell the memberlist it's alive.
    let a = Alive::new(1, bad_node.cheap_clone());
    m.alive_node(a, None, false).await;

    // Force a probe, which should start us into the suspect state.
    {
      let n = m.inner.nodes.read().await.get_state(bad_node.id()).unwrap();
      m.probe_node(&n).await;
    }

    let state = m.get_node_state(bad_node.id()).await.unwrap();
    assert_eq!(
      state,
      State::Suspect,
      "case {}: expected node to be suspect",
      c.name
    );

    // Add the requested number of confirmations.
    for peer in peers.iter().take(c.comfirmations) {
      let s = Suspect::new(1, bad_node.id().clone(), peer.local_id().clone());
      m.suspect_node(s).await.unwrap();
    }

    // Wait until right before the timeout and make sure the timer
    // hasn't fired.
    let fudge = Duration::from_millis(25);
    R::sleep(c.expected - fudge).await;

    let state = m.get_node_state(bad_node.id()).await.unwrap();
    assert_eq!(
      state,
      State::Suspect,
      "case {}: expected node to still be suspect",
      c.name
    );

    // Wait through the timeout and a little after to make sure the
    // timer fires.
    R::sleep(Duration::from_millis(1500)).await;

    let state = m.get_node_state(bad_node.id()).await.unwrap();
    assert_eq!(
      state,
      State::Dead,
      "case {}: expected node to be dead",
      c.name
    );

    m.shutdown().await.unwrap();
    for peer in peers {
      peer.shutdown().await.unwrap();
    }
  }
}

/// Unit test to test the probe node awareness degraded functionality
pub async fn probe_node_awareness_degraded<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t3: T,
  node4: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let probe_time_min = Duration::from_millis(200) * 2 - Duration::from_millis(50);

  let m2: Memberlist<T> = host_memberlist(
    t2,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let m3: Memberlist<T> = host_memberlist(
    t3,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  let a3 = Alive::new(1, m3.advertise_node());

  m1.alive_node(a3, None, false).await;

  // Node 4 never gets started.
  let a4 = Alive::new(1, node4.cheap_clone());
  m1.alive_node(a4, None, false).await;

  // Start the health in a degraded state.
  m1.inner.awareness.apply_delta(1);
  let score = m1.health_score();
  assert_eq!(score, 1, "bad: {score}");

  // Have node m1 probe m4.
  let start_probe = {
    let n = m1.inner.nodes.read().await.get_state(node4.id()).unwrap();
    let start = Instant::now();
    m1.probe_node(&n).await;
    start
  };

  let probe_time = start_probe.elapsed();

  // Node should be reported suspect.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(node4.id())
      .unwrap()
      .state;
    assert_eq!(state, State::Suspect, "expect node to be suspect");
  }

  // Make sure we timed out approximately on time (note that we accounted
  // for the slowed-down failure detector in the probeTimeMin calculation.
  assert!(
    probe_time >= probe_time_min,
    "probed too quickly: {}s",
    probe_time.as_secs_f64()
  );

  // Confirm at least one of the peers attempted an indirect probe.
  let s2 = m2.inner.hot.sequence_num.load(Ordering::SeqCst);
  let s3 = m3.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert!(s2 == 1 && s3 == 1, "bad seqnos: {s2}, {s3}");

  // We should have gotten all the nacks, so our score should remain the
  // same, since we didn't get a successful probe.
  let score = m1.health_score();
  assert_eq!(score, 1, "bad: {score}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

/// Unit test to test the probe node awareness improved functionality
pub async fn probe_node_awareness_improved<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(
    t2,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  // Start the health in a degraded state.
  m1.inner.awareness.apply_delta(1);
  let score = m1.health_score();
  assert_eq!(score, 1, "bad: {score}");

  // Have node m1 probe m2.
  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  // Node should be reported alive.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap()
      .state;
    assert_eq!(state, State::Alive, "expect node to be alive");
  }

  // Our score should have improved since we did a good probe.
  let score = m1.health_score();
  assert_eq!(score, 0, "bad: {score}");

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe node awareness missed nack functionality
pub async fn probe_node_awareness_missed_nack<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t2_opts: Options,
  node3: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  node4: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(
    t2,
    t2_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  // Node 3 and node 4 never get started.
  let a3 = Alive::new(1, node3);

  let a4 = Alive::new(1, node4.cheap_clone());
  // Node 3 and node 4 never get started.
  m1.alive_node(a3, None, false).await;
  m1.alive_node(a4, None, false).await;

  // Make sure health looks good
  let health = m1.health_score();
  assert_eq!(health, 0, "bad: {health}");

  // Have node m1 probe m4.
  {
    let n = m1.inner.nodes.read().await.get_state(node4.id()).unwrap();
    m1.probe_node(&n).await;
  };

  // Node should be reported suspect.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(node4.id())
      .unwrap()
      .state;
    assert_eq!(state, State::Suspect, "expect node to be suspect");
  }

  for i in 0..50 {
    let score = m1.health_score();
    if score == 1 {
      break;
    }
    if i == 49 {
      panic!("expected health score to decrement on missed nack. want 1 got {score}");
    }
    R::sleep(Duration::from_millis(100)).await;
  }

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe node buddy functionality
pub async fn probe_node_buddy<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2.cheap_clone(), None, false).await;
  m2.alive_node(a2, None, true).await;

  // Force the state to suspect so we piggyback a suspect message with the ping.
  // We should see this get refuted later, and the ping will succeed.
  {
    let mut members = m1.inner.nodes.write().await;
    let id = m2.local_id();
    members.set_state(id, State::Suspect);
    let n = members.get_state(id).unwrap();
    drop(members);
    m1.probe_node(&n).await;
  };

  // Make sure a ping was sent.
  let sequence_number = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(sequence_number, 1, "bad seq no: {sequence_number}");

  // Check a broadcast is queued.
  let num = m2.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Should be alive msg
  let broadcasts = m2.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe functionality
pub async fn probe_node<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(1000)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  // Should be marked alive
  let state = m1
    .inner
    .nodes
    .read()
    .await
    .get_state(m2.local_id())
    .unwrap()
    .state;
  assert_eq!(state, State::Alive, "expect node to be alive: {state}");

  // Should increment seqno
  let sequence_number = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(sequence_number, 1, "bad seq no: {sequence_number}");
}

/// Unit test to test the ping functionality
pub async fn ping<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  bad_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_secs(1))
      .with_probe_interval(Duration::from_secs(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  // Do a legit ping.
  let rtt = {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.ping(n.node()).await.unwrap()
  };

  assert!(rtt > Duration::ZERO, "bad: {rtt:?}");

  // This ping has a bad node name so should timeout.
  let err = m1.ping(bad_node.cheap_clone()).await.unwrap_err();
  assert!(matches!(err, Error::Lost(_)), "bad: {err}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the reset nodes functionality
pub async fn reset_nodes<T, R>(
  t1: T,
  t1_opts: Options,
  n1: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  n2: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  n3: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts.with_gossip_to_the_dead_time(Duration::from_millis(100)),
  )
  .await
  .unwrap();

  let a1 = Alive::new(1, n1);

  m1.alive_node(a1, None, false).await;

  let a2 = Alive::new(1, n2.cheap_clone());

  m1.alive_node(a2, None, false).await;

  let a3 = Alive::new(1, n3);

  m1.alive_node(a3, None, false).await;

  let d = Dead::new(1, n2.id().cheap_clone(), m1.local_id().cheap_clone());

  {
    let mut members = m1.inner.nodes.write().await;
    m1.dead_node(&mut *members, d).await.unwrap();
  }

  m1.reset_nodes().await;

  {
    let nodes = m1.inner.nodes.read().await;
    assert_eq!(nodes.node_map.len(), 3, "bad: {}", nodes.node_map.len());
    assert!(
      nodes.node_map.contains_key(n2.id()),
      "{} should not be unmapped",
      n2
    );
  }

  R::sleep(Duration::from_millis(200)).await;
  m1.reset_nodes().await;
  {
    let nodes = m1.inner.nodes.read().await;
    assert_eq!(nodes.node_map.len(), 2, "bad: {}", nodes.node_map.len());
    assert!(
      !nodes.node_map.contains_key(n2.id()),
      "{} should be unmapped",
      n2
    );
  }

  m1.shutdown().await.unwrap();
}

async fn ack_handler_exists(m: &AckManager, idx: u32) -> bool {
  let acks = m.0.lock();
  acks.contains_key(&idx)
}

/// Unit test to test the set probe channels functionality
pub async fn set_probe_channels<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m = AckManager::new();

  let (tx, _rx) = async_channel::bounded(1);

  m.set_probe_channels::<R>(0, tx, None, Instant::now(), Duration::from_millis(10));

  assert!(ack_handler_exists(&m, 0).await, "missing handler");

  R::sleep(Duration::from_millis(50)).await;

  assert!(!ack_handler_exists(&m, 0).await, "non-reaped handler");
}

/// Unit test to test the set ack handler functionality
pub async fn set_ack_handler<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m1 = AckManager::new();

  m1.set_ack_handler::<_, R>(0, Duration::from_millis(10), |_1, _2| {
    Box::pin(async move {})
  });

  assert!(ack_handler_exists(&m1, 0).await, "missing handler");

  R::sleep(Duration::from_millis(50)).await;

  assert!(!ack_handler_exists(&m1, 0).await, "non-reaped handler");
}

/// Unit test to test the invoke ack handler functionality.
pub async fn invoke_ack_handler<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m1 = AckManager::new();

  // Does nothing
  m1.invoke_ack_handler(Ack::new(0), Instant::now()).await;

  let b = Arc::new(AtomicBool::new(false));
  let b1 = b.clone();
  m1.set_ack_handler::<_, R>(0, Duration::from_millis(10), |_, _| {
    Box::pin(async move {
      b1.store(true, Ordering::SeqCst);
    })
  });

  // Should set b
  m1.invoke_ack_handler(Ack::new(0), Instant::now()).await;
  assert!(b.load(Ordering::SeqCst), "b not set");
}

/// Unit test to test the invoke ack handler channel ack functionality
pub async fn invoke_ack_handler_channel_ack<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m = AckManager::new();

  let ack = Ack::new(0).with_payload(Bytes::from_static(&[0, 0, 0]));

  // Does nothing
  m.invoke_ack_handler(ack.clone(), Instant::now()).await;

  let (ack_tx, ack_rx) = async_channel::bounded(1);
  let (nack_tx, nack_rx) = async_channel::bounded(1);
  m.set_probe_channels::<R>(
    0,
    ack_tx,
    Some(nack_tx),
    Instant::now(),
    Duration::from_millis(10),
  );

  // Should send message
  m.invoke_ack_handler(ack.clone(), Instant::now()).await;

  loop {
    futures::select! {
      v = ack_rx.recv().fuse() => {
        let v = v.unwrap();
        assert!(v.complete, "bad value");
        assert_eq!(&v.payload, ack.payload(), "wrong payload. expected: {:?}; actual: {:?}", ack.payload(), v.payload);
        break;
      },
      res = nack_rx.recv().fuse() => {
        if res.is_ok() {
          panic!("should not get a nack")
        }
      },
      default => {
        panic!("message not sent");
      }
    }
  }

  assert!(!ack_handler_exists(&m, 0).await, "non-reaped handler");
}

/// Unit test to test the invoke ack handler channel nack functionality
pub async fn invoke_ack_handler_channel_nack<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m1 = AckManager::new();

  // Does nothing
  let nack = Nack::new(0);
  m1.invoke_nack_handler(nack).await;

  let (ack_tx, ack_rx) = async_channel::bounded(1);
  let (nack_tx, nack_rx) = async_channel::bounded(1);
  m1.set_probe_channels::<R>(
    0,
    ack_tx,
    Some(nack_tx),
    Instant::now(),
    Duration::from_millis(10),
  );

  // Should send message
  m1.invoke_nack_handler(nack).await;

  futures::select! {
    _ = ack_rx.recv().fuse() => panic!("should not get an ack"),
    _ = nack_rx.recv().fuse() => {
      // Good
    },
    default => {
      panic!("message not sent");
    }
  }

  // Getting a nack doesn't reap the handler so that we can still forward
  // an ack up to the reap time, if we get one.
  assert!(
    ack_handler_exists(&m1, 0).await,
    "handler should not be reaped"
  );

  let ack = Ack::new(0).with_payload(Bytes::from_static(&[0, 0, 0]));
  m1.invoke_ack_handler(ack.clone(), Instant::now()).await;

  loop {
    futures::select! {
      v = ack_rx.recv().fuse() => {
        let v = v.unwrap();
        assert!(v.complete, "bad value");
        assert_eq!(&v.payload, ack.payload(), "wrong payload. expected: {:?}; actual: {:?}", ack.payload(), v.payload);
        break;
      },
      res = nack_rx.recv().fuse() => {
        if res.is_ok() {
          panic!("should not get a nack")
        }
      },
      default => {
        panic!("message not sent");
      }
    }
  }

  assert!(!ack_handler_exists(&m1, 0).await, "non-reaped handler");
}

/// Unit test to test the alive node new node functionality
pub async fn alive_node_new_node<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();
  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(1, test_node.clone());

  m.alive_node(a, None, false).await;

  let len = m.inner.nodes.read().await.node_map.len();
  assert_eq!(len, 1, "bad: {len}");

  {
    let state = m
      .inner
      .nodes
      .read()
      .await
      .get_state(test_node.id())
      .unwrap();
    assert_eq!(state.state, State::Alive, "bad state");
    assert_eq!(
      state.incarnation.load(Ordering::Relaxed),
      1,
      "bad incarnation"
    );

    assert!(
      Epoch::now() - state.state_change < Duration::from_millis(1),
      "bad change delta"
    );
  }

  // Check for a join message
  futures::select! {
    ev = subscriber.recv().fuse() => {
      let ev = ev.unwrap();
      let node = ev.node_state();
      let kind = ev.kind();
      assert_eq!(kind, EventKind::Join, "bad state: {kind:?}");
      assert_eq!(node.id(), test_node.id(), "bad node: {}", node.id());
    },
    default => {
      panic!("no join message");
    }
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  m.shutdown().await.unwrap();
}

struct ToggledEventDelegate<I, A> {
  real: SubscribleEventDelegate<I, A>,
  enabled: Mutex<bool>,
}

impl<I, A> ToggledEventDelegate<I, A> {
  fn new(enabled: bool) -> (Self, EventSubscriber<I, A>) {
    let (real, subscriber) = SubscribleEventDelegate::unbounded();
    (
      Self {
        real,
        enabled: Mutex::new(enabled),
      },
      subscriber,
    )
  }

  async fn toggle(&self, enabled: bool) {
    *self.enabled.lock().await = enabled;
  }
}

impl<I, A> EventDelegate for ToggledEventDelegate<I, A>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
{
  type Id = I;
  type Address = A;

  async fn notify_join(&self, node: Arc<crate::types::NodeState<Self::Id, Self::Address>>) {
    let mu = self.enabled.lock().await;
    if *mu {
      self.real.notify_join(node).await;
    }
  }

  async fn notify_leave(&self, node: Arc<crate::types::NodeState<Self::Id, Self::Address>>) {
    let mu = self.enabled.lock().await;
    if *mu {
      self.real.notify_leave(node).await;
    }
  }

  async fn notify_update(&self, node: Arc<crate::types::NodeState<Self::Id, Self::Address>>) {
    let mu = self.enabled.lock().await;
    if *mu {
      self.real.notify_update(node).await;
    }
  }
}

/// Unit test to test the alive node suspect node functionality
pub async fn alive_node_suspect_node<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = ToggledEventDelegate::new(false);

  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let mut a = Alive::new(1, test_node.clone());

  m.alive_node(a.clone(), None, false).await;

  // Listen only after first join
  m.delegate
    .as_ref()
    .unwrap()
    .event_delegate()
    .toggle(true)
    .await;

  // Make suspect
  m.change_node(test_node.id(), |state| {
    *state = LocalNodeState {
      state: State::Suspect,
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  // Old incarnation number, should not change
  m.alive_node(a.clone(), None, false).await;
  let state = m.get_node_state(test_node.id()).await.unwrap();
  assert_eq!(state, State::Suspect, "update with old incarnation!");

  // Should reset to alive now
  a.set_incarnation(2);
  m.alive_node(a, None, false).await;

  let state = m.get_node_state(test_node.id()).await.unwrap();
  assert_eq!(state, State::Alive, "no update with new incarnation!");

  let change = m.get_node_state_change(test_node.id()).await.unwrap();
  assert!(
    Epoch::now() - change < Duration::from_millis(1),
    "bad change delta"
  );

  // Check for a join message
  futures::select! {
    ev = subscriber.recv().fuse() => {
      panic!("got bad event {ev:?}");
    },
    default => {}
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the alive node idempotent functionality
pub async fn alive_node_idempotent<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = ToggledEventDelegate::new(false);

  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let mut a = Alive::new(1, test_node.clone());

  m.alive_node(a.clone(), None, false).await;

  // Listen only after first join
  m.delegate
    .as_ref()
    .unwrap()
    .event_delegate()
    .toggle(true)
    .await;

  // Make suspect
  let change = m.get_node_state_change(test_node.id()).await.unwrap();

  // Should reset to alive now
  a.set_incarnation(2);
  m.alive_node(a, None, false).await;

  let state = m.get_node_state(test_node.id()).await.unwrap();
  assert_eq!(state, State::Alive, "non idempotent");
  let new_change = m.get_node_state_change(test_node.id()).await.unwrap();
  assert_eq!(change, new_change, "should not change state");

  // Check for a join message
  futures::select! {
    ev = subscriber.recv().fuse() => {
      panic!("got bad event {ev:?}");
    },
    default => {}
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");
}

/// Unit test to test the alive node change meta functionality
pub async fn alive_node_change_meta<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = ToggledEventDelegate::new(false);

  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let mut a = Alive::new(1, test_node.clone()).with_meta("val1".try_into().unwrap());

  m.alive_node(a.clone(), None, false).await;

  // Listen only after first join
  m.delegate
    .as_ref()
    .unwrap()
    .event_delegate()
    .toggle(true)
    .await;

  a.set_incarnation(2);
  a.set_meta("val2".try_into().unwrap());
  m.alive_node(a.clone(), None, false).await;

  // check updates
  {
    let state = m
      .inner
      .nodes
      .read()
      .await
      .get_state(test_node.id())
      .unwrap();
    assert_eq!(state.state, State::Alive, "bad state");
    assert_eq!(state.meta(), a.meta(), "meta did not update");
  }

  // Check for a notify update message
  futures::select! {
    ev = subscriber.recv().fuse() => {
      let ev = ev.unwrap();
      let node = ev.node_state();
      let kind = ev.kind();
      assert_eq!(kind, EventKind::Update, "bad state: {kind:?}");
      assert_eq!(node.id(), test_node.id(), "bad node: {}", node.id());
      assert_eq!(node.meta(), a.meta(), "bad meta: {:?}", node.meta().as_ref());
    },
    default => {
      panic!("missing event!");
    }
  }

  m.shutdown().await.unwrap();
}

/// Unit test to test the alive node refute functionality
pub async fn alive_node_refute<T, R>(t1: T, t1_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(1, m.advertise_node());

  m.alive_node(a, None, true).await;

  // Clear queue
  m.inner.broadcast.reset().await;

  // Conflicting alive
  let a = Alive::new(2, m.advertise_node()).with_meta("foo".try_into().unwrap());

  m.alive_node(a, None, false).await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(m.local_id()).unwrap();
    assert_eq!(n.state, State::Alive, "should still be alive");
    assert!(n.meta().is_empty(), "meta should still be empty");
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Should be alive msg
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the alive node conflict functionality
pub async fn alive_node_conflict<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts.with_dead_node_reclaim_time(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive::new(1, test_node1.clone());
  m.alive_node(a, None, true).await;

  // Clear queue
  m.inner.broadcast.reset().await;

  // Conflicting alive
  let test_node2 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.2:9000".parse().unwrap(),
  );
  let a = Alive::new(2, test_node2.clone()).with_meta("foo".try_into().unwrap());
  m.alive_node(a, None, false).await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Alive, "should still be alive");
    assert!(n.meta().is_empty(), "meta should still be empty");
    assert_eq!(n.id(), &test_node_id, "id should not be update");
    assert_eq!(
      n.address(),
      test_node1.address(),
      "addr should not be updated"
    );
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 0, "expected 0 queued message: {num}");

  // Change the node to dead
  let d = Dead::new(2, test_node_id.clone(), m.local_id().cheap_clone());

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut *members, d).await.unwrap();
  }

  m.inner.broadcast.reset().await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Dead, "should be dead");
  }

  R::sleep(m.inner.opts.dead_node_reclaim_time).await;

  // New alive node
  let a = Alive::new(3, test_node2.clone()).with_meta("foo".try_into().unwrap());

  m.alive_node(a, None, false).await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Alive, "should still be alive");
    assert_eq!(n.meta().as_bytes(), b"foo", "meta should be updated");
    assert_eq!(n.address(), test_node2.address(), "addr should be updated");
  }

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node no node functionality
pub async fn suspect_node_no_node<T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let s = Suspect::new(1, test_node_id, m.local_id().cheap_clone());

  m.suspect_node(s).await.unwrap();

  {
    let nodes = m.inner.nodes.read().await.node_map.len();
    assert_eq!(nodes, 0, "don't expect nodes");
  }

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node functionality
pub async fn suspect_node<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts
      .with_probe_interval(Duration::from_millis(1))
      .with_suspicion_mult(1),
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive::new(1, test_node1.clone());

  m.alive_node(a, None, false).await;

  m.change_node(&test_node_id, |state| {
    *state = LocalNodeState {
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  let s = Suspect::new(1, test_node_id.clone(), m.local_id().cheap_clone());

  m.suspect_node(s).await.unwrap();

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Suspect, "bad state");

  let change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert!(
    Epoch::now() - change <= Duration::from_secs(1),
    "bad change delta"
  );

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Check its a suspect message
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Suspect(_)), "bad message: {msg:?}");

  // Wait for the timeout
  R::sleep(Duration::from_millis(100)).await;

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Dead, "bad state");

  let new_change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert!(
    Epoch::now() - new_change <= Duration::from_secs(1),
    "bad change delta"
  );

  assert!(
    new_change.checked_duration_since(change).is_some(),
    "should increment time"
  );

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Check its a suspect message
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Dead(_)), "bad message: {msg:?}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node double suspect functionality
pub async fn suspect_node_double_suspect<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive::new(1, test_node1.clone());

  m.alive_node(a, None, false).await;

  m.change_node(&test_node_id, |state| {
    *state = LocalNodeState {
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  let s = Suspect::new(1, test_node_id.clone(), m.local_id().cheap_clone());

  m.suspect_node(s.clone()).await.unwrap();

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Suspect, "bad state");

  let change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert!(
    Epoch::now() - change <= Duration::from_secs(1),
    "bad change delta"
  );

  // clear the broadcast queue
  m.inner.broadcast.reset().await;

  // suspect again
  m.suspect_node(s).await.unwrap();

  let new_change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert_eq!(new_change, change, "unexpected change");

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 0, "expected no queued message: {num}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node old suspect functionality
pub async fn suspect_node_old_suspect<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let now = Epoch::now();
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive::new(1, test_node1.clone());

  m.alive_node(a, None, false).await;

  m.change_node(&test_node_id, |state| {
    *state = LocalNodeState {
      state_change: now,
      ..state.clone()
    };
  })
  .await;

  // clear queue
  m.inner.broadcast.reset().await;

  let s = Suspect::new(1, test_node_id.clone(), m.local_id().cheap_clone());

  m.suspect_node(s.clone()).await.unwrap();

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Alive, "bad state");

  // check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 0, "expected 0 queued message: {num}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node refute functionality
pub async fn suspect_node_refute<T, R>(t1: T, t1_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(1, m.advertise_node());

  m.alive_node(a, None, true).await;

  // clear queue
  m.inner.broadcast.reset().await;

  // make sure health is in a good state
  let health = m.health_score();
  assert_eq!(health, 0, "bad: {health}");

  let s = Suspect::new(1, m.local_id().cheap_clone(), m.local_id().cheap_clone());

  m.suspect_node(s).await.unwrap();

  let state = m.get_node_state(m.local_id()).await.unwrap();
  assert_eq!(state, State::Alive, "should still be alive");

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only 1 queued message: {num}");

  // should be alive msg
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");

  // Health should have been dinged
  let health = m.health_score();
  assert_eq!(health, 1, "bad: {health}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node no node functionality
pub async fn dead_node_no_node<T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let d = Dead::new(1, test_node_id, m.local_id().cheap_clone());

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  {
    let nodes = m.inner.nodes.read().await.node_map.len();
    assert_eq!(nodes, 0, "don't expect nodes");
  }

  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node left functionality
pub async fn dead_node_left<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();
  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let test_node = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive::new(1, test_node.clone());
  m.alive_node(a, None, false).await;

  // Read the join event
  subscriber.recv().await.unwrap();

  let d = Dead::new(1, test_node_id.clone(), m.local_id().cheap_clone());

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  // Read the dead event
  subscriber.recv().await.unwrap();

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Left, "bad state");
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Check its a dead message
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Dead(_)), "expected queued dead msg");

  // Clear queue

  // New alive node
  let test_node1 = Node::new(test_node_id.clone(), "127.0.0.2:9000".parse().unwrap());
  let a = Alive::new(3, test_node1.clone()).with_meta("foo".try_into().unwrap());

  m.alive_node(a, None, false).await;

  // Read the join event
  subscriber.recv().await.unwrap();

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Alive, "bad state");
    assert_eq!(n.meta().as_bytes(), b"foo", "meta should be updated");
    assert_eq!(n.address(), test_node1.address(), "addr should be updated");
  }
  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node functionality
pub async fn dead_node<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();
  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(1, test_node.clone());
  m.alive_node(a, None, false).await;

  // Read the join event
  subscriber.recv().await.unwrap();

  m.change_node(test_node.id(), |state| {
    *state = LocalNodeState {
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  let d = Dead::new(1, test_node.id().clone(), m.local_id().cheap_clone());

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  let state = m.get_node_state(test_node.id()).await.unwrap();
  assert_eq!(state, State::Dead, "bad state");

  let change = m.get_node_state_change(test_node.id()).await.unwrap();
  assert!(
    Epoch::now() - change <= Duration::from_secs(1),
    "bad change delta"
  );

  futures::select! {
    event = subscriber.recv().fuse() => {
      let event = event.unwrap();
      let kind = event.kind();
      assert!(matches!(kind, EventKind::Leave), "bad event: {kind:?}");
    },
    default => {
      panic!("no leave message");
    }
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Check its a dead message
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Dead(_)), "expected queued dead msg");
  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node double functionality
pub async fn dead_node_double<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();

  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(1, test_node.clone());

  m.alive_node(a, None, false).await;

  m.change_node(test_node.id(), |state| {
    *state = LocalNodeState {
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  let mut d = Dead::new(1, test_node.id().clone(), m.local_id().cheap_clone());

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d.clone()).await.unwrap();
  }

  // Clear queue
  m.inner.broadcast.reset().await;

  // Consume events
  while !subscriber.is_empty() {
    subscriber.recv().await.unwrap();
  }

  // should do nothing
  d.set_incarnation(2);

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  futures::select! {
    event = subscriber.recv().fuse() => {
      let event = event.unwrap();
      let kind = event.kind();
      assert!(matches!(kind, EventKind::Leave), "should not get leave: {kind:?}");
    },
    default => {}
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 0, "expected 0 queued message: {num}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node old dead functionality
pub async fn dead_node_old_dead<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(10, test_node.clone());

  m.alive_node(a, None, false).await;

  m.change_node(test_node.id(), |state| {
    *state = LocalNodeState {
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  let d = Dead::new(1, test_node.id().clone(), m.local_id().cheap_clone());

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  let state = m.get_node_state(test_node.id()).await.unwrap();
  assert_eq!(state, State::Alive, "bad state");

  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node alive replay functionality
pub async fn dead_node_alive_replay<T, R>(
  t1: T,
  t1_opts: Options,
  test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(10, test_node.clone());

  m.alive_node(a.clone(), None, false).await;

  let d = Dead::new(10, test_node.id().clone(), m.local_id().cheap_clone());

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  // Replay alive at same incarnation
  m.alive_node(a, None, false).await;

  // Should remain dead
  let state = m.get_node_state(test_node.id()).await.unwrap();
  assert_eq!(state, State::Dead, "bad state");

  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node refute functionality
pub async fn dead_node_refute<T, R>(t1: T, t1_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive::new(1, m.advertise_node());
  m.alive_node(a, None, true).await;

  // Clear queue
  m.inner.broadcast.reset().await;

  // Make sure health is in a good state
  let health = m.health_score();
  assert_eq!(health, 0, "bad: {health}");

  let d = Dead::new(1, m.local_id().cheap_clone(), m.local_id().cheap_clone());
  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  let state = m.get_node_state(m.local_id()).await.unwrap();
  assert_eq!(state, State::Alive, "should still be Alive");

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Sould be alive msg
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");

  // We should have been dinged
  let health = m.health_score();
  assert_eq!(health, 1, "bad: {health}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the merge state functionality
pub async fn merge_state<A, T, R>(
  t1: T,
  t1_opts: Options,
  node_id1: T::Id,
  node_id2: T::Id,
  node_id3: T::Id,
  node_id4: T::Id,
) where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();
  let m = get_memberlist(
    t1,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t1_opts,
  )
  .await
  .unwrap();

  let node1 = Node::new(node_id1.clone(), "127.0.0.1:8000".parse().unwrap());
  let a1 = Alive::new(1, node1.clone());
  m.alive_node(a1, None, false).await;

  let node2 = Node::new(node_id2.clone(), "127.0.0.2:8000".parse().unwrap());
  let a2 = Alive::new(1, node2.clone());
  m.alive_node(a2, None, false).await;

  let node3 = Node::new(node_id3.clone(), "127.0.0.3:8000".parse().unwrap());
  let a3 = Alive::new(1, node3.clone());
  m.alive_node(a3, None, false).await;

  let s = Suspect::new(1, node_id1.clone(), m.local_id().cheap_clone());
  m.suspect_node(s).await.unwrap();

  while !subscriber.is_empty() {
    subscriber.recv().await.unwrap();
  }

  let node4: Node<_, SocketAddr> = Node::new(node_id4.clone(), "127.0.0.4:8000".parse().unwrap());

  let remote = vec![
    PushNodeState::new(2, node1.id().clone(), *node1.address(), State::Alive),
    PushNodeState::new(1, node2.id().clone(), *node2.address(), State::Suspect),
    PushNodeState::new(1, node3.id().clone(), *node3.address(), State::Dead),
    PushNodeState::new(2, node4.id().clone(), *node4.address(), State::Alive),
  ];

  // Merge remote state
  m.merge_state(remote.as_slice()).await;

  // Check the state
  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(node1.id()).unwrap();
    assert_eq!(n.state, State::Alive, "bad state");
    assert_eq!(n.incarnation.load(Ordering::Relaxed), 2, "bad incarnation");

    let n = nodes.get_state(node2.id()).unwrap();
    assert_eq!(n.state, State::Suspect, "bad state");
    assert_eq!(n.incarnation.load(Ordering::Relaxed), 1, "bad incarnation");

    let n = nodes.get_state(node3.id()).unwrap();
    assert_eq!(n.state, State::Suspect, "bad state");

    let n = nodes.get_state(node4.id()).unwrap();
    assert_eq!(n.state, State::Alive, "bad state");
    assert_eq!(n.incarnation.load(Ordering::Relaxed), 2, "bad incarnation");
  }

  // Check the channels
  futures::select! {
    event = subscriber.recv().fuse() => {
      let event = event.unwrap();
      let kind = event.kind();
      assert!(matches!(kind, EventKind::Join), "bad event: {kind:?}");
      assert_eq!(event.node_state().id(), node4.id(), "bad node");
    },
    default => {
      panic!("Expect join");
    }
  }

  futures::select! {
    _ = subscriber.recv().fuse() => {
      panic!("unexpected event");
    },
    default => {}
  }

  m.shutdown().await.unwrap();
}

/// Unit test to gossip functionality
pub async fn gossip<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options, t3: T, t3_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R as agnostic::Runtime>::Sleep: Send + Sync,
  <R::Sleep as Future>::Output: Send + Sync,
  <R::Interval as Stream>::Item: Send + Sync,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();
  let m1 = host_memberlist(t1, t1_opts.with_gossip_interval(Duration::from_millis(10)))
    .await
    .unwrap();

  let m2 = host_memberlist_with_delegate(
    t2,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t2_opts.with_gossip_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m3 = host_memberlist(t3, t3_opts).await.unwrap();

  let a1 = Alive::new(1, m1.advertise_node());
  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  let a3 = Alive::new(1, m3.advertise_node());

  m1.alive_node(a3, None, false).await;

  // Gossip should send all this to m2. Retry a few times because it's UDP and
  // timing and stuff makes this flaky without.

  for idx in 1..=15 {
    m1.gossip().await;

    R::sleep(Duration::from_millis(3)).await;

    if subscriber.len() < 3 && idx == 15 {
      panic!("expected 3 events, got {}", subscriber.len());
    }

    R::sleep(Duration::from_millis(250)).await;
  }

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

/// Unit test to test the gossip to dead functionality
pub async fn gossip_to_dead<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R as agnostic::Runtime>::Sleep: Send + Sync,
  <R::Sleep as Future>::Output: Send + Sync,
  <R::Interval as Stream>::Item: Send + Sync,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();
  let m1 = host_memberlist(
    t1,
    t1_opts
      .with_gossip_interval(Duration::from_millis(1))
      .with_gossip_to_the_dead_time(Duration::from_millis(100)),
  )
  .await
  .unwrap();

  let m2 = host_memberlist_with_delegate(
    t2,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t2_opts,
  )
  .await
  .unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  // Shouldn't send anything to m2 here, node has been dead for 2x the GossipToTheDeadTime
  m1.change_node(m2.local_id(), |state| {
    *state = LocalNodeState {
      state: State::Dead,
      state_change: state.state_change.sub(Duration::from_millis(200)),
      ..state.clone()
    };
  })
  .await;

  m1.gossip().await;

  futures::select! {
    _ = R::sleep(Duration::from_millis(50)).fuse() => {
      assert_eq!(subscriber.len(), 0, "expected no events");
    },
    ev = subscriber.recv().fuse() => {
      panic!("unexpected event: {:?}", ev);
    }
  }

  // Should gossip to m2 because its state has changed within GossipToTheDeadTime
  m1.change_node(m2.local_id(), |state| {
    *state = LocalNodeState {
      state_change: Epoch::now().sub(Duration::from_millis(20)),
      ..state.clone()
    };
  })
  .await;

  for idx in 1..=5 {
    m1.gossip().await;

    R::sleep(Duration::from_millis(3)).await;

    if subscriber.len() < 2 && idx == 5 {
      panic!("expected 2 messages from gossip, got {}", subscriber.len());
    }

    R::sleep(Duration::from_millis(10)).await;
  }

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the push pull functionality
pub async fn push_pull<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R as agnostic::Runtime>::Sleep: Send + Sync,
  <R::Sleep as Future>::Output: Send + Sync,
  <R::Interval as Stream>::Item: Send + Sync,
{
  let (event_delegate, subscriber) = SubscribleEventDelegate::unbounded();
  let m1 = host_memberlist(
    t1,
    t1_opts
      .with_push_pull_interval(Duration::from_millis(1))
      .with_gossip_interval(Duration::from_secs(10)),
  )
  .await
  .unwrap();

  let m2 = host_memberlist_with_delegate(
    t2,
    CompositeDelegate::new().with_event_delegate(event_delegate),
    t2_opts.with_gossip_interval(Duration::from_secs(10)),
  )
  .await
  .unwrap();

  let a1 = Alive::new(1, m1.advertise_node());

  m1.alive_node(a1, None, true).await;

  let a2 = Alive::new(1, m2.advertise_node());

  m1.alive_node(a2, None, false).await;

  for idx in 1..=5 {
    m1.gossip().await;

    R::sleep(Duration::from_millis(3)).await;

    if subscriber.len() < 2 && idx == 5 {
      panic!(
        "expected 2 messages from push pull, got {}",
        subscriber.len()
      );
    }

    R::sleep(Duration::from_millis(10)).await;
  }

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}
