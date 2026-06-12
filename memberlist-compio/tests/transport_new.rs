//! Transport::new construction-path tests — `MaybeResolved` Resolved vs
//! Unresolved advertise addresses, and resolved vs unresolved join seeds.
//!
//! Resolved inputs (`MaybeResolved::Resolved(socket)`) bypass the resolver
//! entirely; unresolved inputs (`MaybeResolved::Unresolved(hostaddr)`) are run
//! through the supplied `Resolver` (here `OsResolver`) and, for the advertise
//! address, narrowed to one candidate by the `AdvertiseAddrResolver`
//! (`FirstAddrResolver`).

#![cfg(feature = "tcp")]

use std::{
  net::SocketAddr,
  time::{Duration, Instant},
};

use bytes::Bytes;
use memberlist_compio::{
  Address, FirstAddrResolver, MaybeResolved, MemberlistError, MemberlistOptions, Options,
  OsResolver, SocketAddrResolver, StreamTransportOptions, TcpMemberlist, TcpTransportOptions,
  VoidDelegate,
};
use smol_str::SmolStr;

/// Spin-wait up to `deadline` for `predicate` to return true; returns the
/// final value. compio has no async assertion harness, so polling the
/// lock-free snapshot is the documented observability path.
async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

/// Build a `TcpMemberlist` whose membership-input address type is
/// `SocketAddr` (seeds are concrete sockets). The advertise address is
/// already-`Resolved`, so the `SocketAddrResolver` is never invoked at
/// construction.
async fn make_tcp_socket(id: &str, advertise: SocketAddr) -> TcpMemberlist<SmolStr, SocketAddr> {
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(advertise)),
  );
  TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct tcp memberlist (resolved advertise)")
}

/// Build a `TcpMemberlist` whose membership-input address type is the
/// crate's `HostAddr`-backed [`Address`] (seeds + advertise are hostnames
/// resolved by [`OsResolver`]). The advertise address is handed in
/// `Unresolved`, exercising the resolve-then-pick path at construction.
async fn make_tcp_hostaddr(id: &str, advertise: Address) -> TcpMemberlist {
  let opts = Options::new(
    TcpTransportOptions::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Unresolved(advertise)),
  );
  TcpMemberlist::new(
    opts,
    VoidDelegate::default(),
    &OsResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct tcp memberlist (unresolved advertise)")
}

/// `MaybeResolved::Resolved(socket)` is used verbatim — the resolver is
/// never consulted. Binding `127.0.0.1:0` yields a concrete OS-assigned
/// port, observable on the published snapshot's local node.
#[compio::test]
async fn tcp_new_with_resolved_advertise() {
  let m = make_tcp_socket("resolved-adv", "127.0.0.1:0".parse().unwrap()).await;
  let local = m.local_node();
  assert_eq!(local.id_ref().as_str(), "resolved-adv");
  assert!(
    local.addr_ref().ip().is_loopback(),
    "advertise stayed on loopback, got {}",
    local.addr_ref()
  );
  assert_ne!(
    local.addr_ref().port(),
    0,
    "ephemeral :0 must resolve to a concrete OS-assigned port"
  );
  m.shutdown().await.expect("shutdown");
}

/// `MaybeResolved::Unresolved(hostaddr)` is resolved via `OsResolver`,
/// narrowed by `FirstAddrResolver`, and bound. `127.0.0.1:0` resolves to a
/// loopback candidate; the bound socket's concrete port surfaces on the
/// local node.
#[compio::test]
async fn tcp_new_with_unresolved_advertise() {
  let advertise: Address = "127.0.0.1:0".parse().expect("parse host advertise");
  let m = make_tcp_hostaddr("unresolved-adv", advertise).await;
  let local = m.local_node();
  assert_eq!(local.id_ref().as_str(), "unresolved-adv");
  assert!(
    local.addr_ref().ip().is_loopback(),
    "resolved advertise must stay on loopback, got {}",
    local.addr_ref()
  );
  assert_ne!(
    local.addr_ref().port(),
    0,
    "resolved + bound advertise must carry a concrete OS-assigned port"
  );
  m.shutdown().await.expect("shutdown");
}

/// `join` with a `MaybeResolved::Resolved(seed)` contacts the seed
/// directly (no resolver call). Two ephemeral-bound nodes converge.
#[compio::test]
async fn tcp_join_with_resolved_seed() {
  let seed = make_tcp_socket("rseed", "127.0.0.1:0".parse().unwrap()).await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let joiner = make_tcp_socket("rjoiner", "127.0.0.1:0".parse().unwrap()).await;
  let contacted = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join resolved seed");
  assert_eq!(contacted, 1, "exactly one seed contacted");

  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// `join` with a `MaybeResolved::Unresolved(hostaddr)` resolves the seed
/// through `OsResolver` first, then contacts it. The seed's concrete bound
/// socket is wrapped back into a `HostAddr` (an IP+port `Address`) that
/// `OsResolver` resolves to that same socket.
#[compio::test]
async fn tcp_join_with_unresolved_seed() {
  // Seed advertises an unresolved loopback host so its membership-input
  // address type matches the joiner's `OsResolver`-driven `Address`.
  let seed_advertise: Address = "127.0.0.1:0".parse().expect("parse seed advertise");
  let seed = make_tcp_hostaddr("useed", seed_advertise).await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let joiner_advertise: Address = "127.0.0.1:0".parse().expect("parse joiner advertise");
  let joiner = make_tcp_hostaddr("ujoiner", joiner_advertise).await;

  // Wrap the seed's concrete socket back into an unresolved host address;
  // OsResolver resolves the loopback IP+port to itself.
  let seed_host = Address::from(seed_addr);
  let contacted = joiner
    .join(&OsResolver, &[MaybeResolved::Unresolved(seed_host)])
    .await
    .expect("join unresolved seed");
  assert_eq!(contacted, 1, "exactly one seed contacted after resolution");

  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// A `gossip_mtu` whose wire datagram cannot fit a single UDP packet is an
/// impossible configuration (gossip is sent as one UDP datagram, hard-capped
/// at 65507 bytes after the 30-byte encryption and 10-byte checksum wrappers,
/// i.e. a 65467-byte plaintext ceiling). `Memberlist::new` must reject it
/// fail-fast with `InvalidGossipMtu` BEFORE binding any socket, while a value
/// just under the ceiling constructs successfully.
#[compio::test]
async fn transport_new_rejects_impossible_gossip_mtu() {
  // 1 MiB plaintext gossip_mtu — far above the 65467-byte ceiling. A
  // near-MTU gossip packet built at this size would always exceed the
  // 65507-byte UDP datagram limit and be silently dropped.
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("reject"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(1 << 20));
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  // `Memberlist` is not `Debug`, so match rather than `{res:?}`-format the
  // whole `Result`.
  match res {
    Err(MemberlistError::InvalidGossipMtu(e)) => {
      assert_eq!(e.configured(), 1 << 20, "carries the configured value");
      assert_eq!(
        e.ceiling(),
        65507 - 30 - 10,
        "carries the 65467-byte ceiling"
      );
    }
    Err(other) => panic!("expected InvalidGossipMtu, got {other:?}"),
    Ok(_) => panic!("1 MiB gossip_mtu must be rejected, but construction succeeded"),
  }

  // Just under the ceiling (65466 < 65467) must construct successfully.
  let ok_opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("accept"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(65507 - 30 - 10 - 1));
  let m = TcpMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("gossip_mtu just under the ceiling must construct");
  m.shutdown().await.expect("shutdown");
}

/// A `gossip_mtu` below the floor needed to carry the mandatory single-datagram
/// control packets (probe Ping / Ack / minimal self-Alive) makes normal probes
/// deterministically rejected on the receive side → false suspicion.
/// `Memberlist::new` must reject a too-small `gossip_mtu` fail-fast with
/// `GossipMtuTooSmall` BEFORE binding any socket, while a value just at the
/// minimum constructs successfully.
#[compio::test]
async fn transport_new_rejects_too_small_gossip_mtu() {
  // 1 byte cannot frame even an Ack; far below the mandatory-control-packet
  // floor.
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("tiny"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(1));
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::GossipMtuTooSmall(e)) => {
      assert_eq!(e.configured(), 1, "carries the configured value");
      assert_eq!(e.minimum(), 512, "carries the 512-byte floor");
    }
    Err(other) => panic!("expected GossipMtuTooSmall, got {other:?}"),
    Ok(_) => panic!("gossip_mtu=1 must be rejected, but construction succeeded"),
  }

  // A `gossip_mtu` above the identity-aware floor must construct. With a small
  // id and the default `meta_max_size` (512), the largest mandatory control
  // packet is the self-Alive at the meta cap (~559 bytes), so 1024 clears the
  // floor (and the fixed 512 fast pre-check) comfortably.
  let ok_opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("min-ok"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(1024));
  let m = TcpMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("gossip_mtu above the identity-aware floor must construct");
  m.shutdown().await.expect("shutdown");
}

/// The fixed 512-byte floor ignores the local id size, but `T::Id` is unbounded:
/// a node whose local id is large enough that its OWN mandatory single-datagram
/// control packets (probe Ping carrying two id-sized nodes / self-Alive / Ack)
/// no longer fit the gossip budget would have those packets silently unsendable
/// and peers would falsely suspect it. `Memberlist::new` must reject such a
/// config fail-fast (identity-aware floor), while a normal-sized id at the SAME
/// `gossip_mtu` must construct.
#[compio::test]
async fn gossip_mtu_rejected_for_oversized_local_id() {
  // A 2000-byte id: the worst-case Ping (two id-sized nodes) frames to ~4 KiB,
  // far above this 2000-byte gossip_mtu, so the node's own probes would be
  // unsendable. The SAME gossip_mtu easily holds a small id's ~559-byte
  // worst-case Alive, so the rejection is driven purely by the id size.
  let huge_id = SmolStr::new("a".repeat(2000));
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(huge_id)
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(2000));
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::GossipMtuTooSmall(e)) => {
      assert_eq!(e.configured(), 2000, "carries the effective gossip_mtu");
      assert!(
        e.minimum() > 2000,
        "the id-driven required minimum must exceed the configured gossip_mtu, got {}",
        e.minimum()
      );
    }
    Err(other) => panic!("expected GossipMtuTooSmall, got {other:?}"),
    Ok(_) => panic!("an oversized local id must be rejected, but construction succeeded"),
  }

  // A normal small id at the SAME gossip_mtu constructs successfully.
  let ok_opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("small-id"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(2000));
  let m = TcpMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("a small local id at the same gossip_mtu must construct");
  m.shutdown().await.expect("shutdown");
}

/// An `initial_local_state` snapshot whose framed PushPull would exceed the
/// reliable-stream frame budget rides every push/pull exchange, so it would be
/// rejected by every receiver's frame-length gate and the application state
/// would never reach a peer. `Memberlist::new` must reject it fail-fast as
/// `LocalStateExceedsFrame` BEFORE binding any socket, while a reasonable
/// snapshot constructs successfully.
///
/// The frame budget is `max_stream_frame_size` (default 64 MiB) minus the
/// `LOCAL_STATE_FRAME_BUDGET` (1 MiB) reserve, i.e. ~63 MiB. A 64 MiB snapshot
/// is unambiguously over it.
#[compio::test]
async fn transport_new_rejects_oversized_initial_local_state() {
  let oversized = Bytes::from(vec![0u8; 64 * 1024 * 1024]);
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("big-state"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_initial_local_state(oversized));
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::Proto(memberlist_proto::Error::LocalStateExceedsFrame(..))) => {}
    Err(other) => panic!("expected LocalStateExceedsFrame, got {other:?}"),
    Ok(_) => panic!("a 64 MiB initial_local_state must be rejected, but construction succeeded"),
  }

  // A reasonable snapshot constructs successfully.
  let ok_opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("ok-state"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_initial_local_state(Bytes::from_static(b"hello")));
  let m = TcpMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("a small initial_local_state must construct");
  m.shutdown().await.expect("shutdown");
}

/// A zero `close_timeout` makes the post-`StreamAction::Close` graceful drain
/// fire its per-write timeout immediately, so a graceful close abandons (RSTs)
/// its queued push/pull response bytes instead of draining them — truncating
/// reliable exchanges. `Transport::new` (via `StreamTransportOptions::validate`)
/// must reject it fail-fast with `InvalidOption("close_timeout", _)`, while the
/// default (and any nonzero) `close_timeout` constructs successfully. The check
/// lives in the shared stream-options `validate()`, so it covers both the TCP
/// and TLS backends; this exercises it on TCP.
#[compio::test]
async fn transport_new_rejects_zero_close_timeout() {
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("zero-close"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()))
      .with_stream(StreamTransportOptions::new().with_close_timeout(Duration::ZERO)),
  );
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::InvalidOption(e)) => {
      assert_eq!(
        e.option(),
        "close_timeout",
        "carries the rejected knob name"
      );
      assert!(!e.reason().is_empty(), "carries a reason");
    }
    Err(other) => panic!("expected InvalidOption(close_timeout), got {other:?}"),
    Ok(_) => panic!("a zero close_timeout must be rejected, but construction succeeded"),
  }

  // A nonzero close_timeout (here the explicit default) must construct.
  let ok_opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("nonzero-close"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()))
      .with_stream(StreamTransportOptions::new().with_close_timeout(Duration::from_secs(10))),
  );
  let m = TcpMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("a nonzero close_timeout must construct");
  m.shutdown().await.expect("shutdown");
}

/// Binding the wildcard `0.0.0.0:0` and forgetting to set a concrete advertise
/// is a realistic mistake: the gossip socket's `local_addr()` read back in
/// `T::new` is then an UNSPECIFIED IP. That address encodes fine on the wire,
/// so the node would construct `Ok`, join a cluster, and publish `0.0.0.0` —
/// an address no peer can route probes/dials to. `Memberlist::new` must reject
/// the resolved unspecified advertise fail-fast with `InvalidAdvertiseAddr`,
/// while a concrete loopback advertise (with an ephemeral port resolved to a
/// concrete value) constructs successfully.
#[compio::test]
async fn transport_new_rejects_unspecified_advertise() {
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("wildcard"))
      .with_advertise_addr(MaybeResolved::Resolved("0.0.0.0:0".parse().unwrap())),
  );
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::InvalidAdvertiseAddr(e)) => {
      assert!(
        e.addr().ip().is_unspecified(),
        "carries the rejected unspecified addr, got {}",
        e.addr()
      );
      assert!(!e.reason().is_empty(), "carries a reason");
    }
    Err(other) => panic!("expected InvalidAdvertiseAddr, got {other:?}"),
    Ok(_) => panic!("an unspecified advertise must be rejected, but construction succeeded"),
  }

  // A concrete loopback advertise (ephemeral port → concrete OS port) is a
  // usable unicast contact and must construct.
  let ok_opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("concrete"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  );
  let m = TcpMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("a concrete loopback advertise must construct");
  m.shutdown().await.expect("shutdown");
}
