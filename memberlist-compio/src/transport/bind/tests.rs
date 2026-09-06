use super::*;

/// An ephemeral advertise yields a listener and a gossip socket on the SAME
/// OS-assigned port, and the reported address is that concrete port.
///
/// This is the contract the whole membership identity rests on: peers reach
/// this node at one address, so a pair that landed on two ports would advertise
/// a port only half the traffic can reach.
#[compio::test]
async fn an_ephemeral_pair_lands_on_one_concrete_port() {
  let advertise = "127.0.0.1:0".parse().expect("loopback");
  let (listener, bound, gossip) = bind_stream_pair(advertise)
    .await
    .expect("ephemeral pair binds");

  assert_ne!(bound.port(), 0, "the OS-assigned port must be reported");
  assert_eq!(listener.local_addr().expect("listener addr"), bound);
  assert_eq!(gossip.local_addr().expect("gossip addr"), bound);
}

/// A fixed port is honoured exactly, on both protocols.
#[compio::test]
async fn a_fixed_port_is_claimed_on_both_protocols() {
  // Take an ephemeral pair, release it, and re-claim the same concrete port —
  // the only way to name a port that is known to be free.
  let (listener, bound, gossip) = bind_stream_pair("127.0.0.1:0".parse().expect("loopback"))
    .await
    .expect("probe pair binds");
  // Ignoring Err: the probe sockets exist only to reserve a port number, and a
  // close error would surface as the bind failure below if it mattered.
  let _ = listener.close().await;
  let _ = gossip.close().await;

  let (listener, again, gossip) = bind_stream_pair(bound).await.expect("fixed pair binds");
  assert_eq!(again, bound, "a fixed port is not re-assigned");
  assert_eq!(listener.local_addr().expect("listener addr"), bound);
  assert_eq!(gossip.local_addr().expect("gossip addr"), bound);
}

/// A fixed port already held by a listener is a genuine conflict, surfaced
/// rather than retried.
///
/// The retry budget exists to walk an ephemeral allocator past a port the other
/// protocol will not accept. A caller who NAMED the port has no other port to
/// be walked to, so looping would only turn a clear error into a slow one.
#[compio::test]
async fn a_taken_fixed_port_fails_instead_of_looping() {
  let (held, bound, held_gossip) = bind_stream_pair("127.0.0.1:0".parse().expect("loopback"))
    .await
    .expect("probe pair binds");

  let started = std::time::Instant::now();
  let res = bind_stream_pair(bound).await;
  let elapsed = started.elapsed();

  assert!(res.is_err(), "the port is held, so the bind must fail");
  assert!(
    elapsed < std::time::Duration::from_secs(1),
    "a fixed-port conflict took {elapsed:?}, so it was retried instead of surfaced",
  );

  drop(held);
  drop(held_gossip);
}

/// A UDP-led attempt claims the same pair a TCP-led one does.
///
/// The two strategies alternate inside the retry loop, so the UDP-led branch
/// runs only after a failure — which a test cannot stage without a reserved
/// port range. Exercising it directly is what keeps it from rotting into a
/// branch that has never run.
#[compio::test]
async fn the_udp_led_attempt_claims_a_matching_pair() {
  let (listener, bound, gossip) = bind_udp_first("127.0.0.1:0".parse().expect("loopback"))
    .await
    .expect("udp-led pair binds");

  assert_ne!(bound.port(), 0);
  assert_eq!(listener.local_addr().expect("listener addr"), bound);
  assert_eq!(gossip.local_addr().expect("gossip addr"), bound);
}

/// Only the conflicts another port can resolve are retryable.
///
/// `PermissionDenied` is on the list because Windows returns `WSAEACCES` for a
/// bind inside a platform-reserved port block — the failure that makes the
/// alternating strategy necessary in the first place.
#[test]
fn port_conflicts_are_the_only_retryable_bind_failures() {
  use std::io::{Error, ErrorKind};

  assert!(is_port_conflict(&Error::from(ErrorKind::AddrInUse)));
  assert!(is_port_conflict(&Error::from(ErrorKind::PermissionDenied)));
  assert!(!is_port_conflict(&Error::from(ErrorKind::AddrNotAvailable)));
  assert!(!is_port_conflict(&Error::from(ErrorKind::OutOfMemory)));
}

/// A TCP-led attempt whose UDP half is refused releases the TCP port it had
/// already claimed, and hands the UDP error back.
///
/// Releasing it is what keeps a run of attempts from walking the ephemeral pool
/// dry: a merely DROPPED listener closes asynchronously on Windows, so the port
/// this attempt gave up on would still be held while the next attempt asks for
/// another one.
#[compio::test]
async fn a_tcp_led_attempt_releases_its_listener_when_udp_refuses_the_port() {
  // A port known free on BOTH protocols, of which only the UDP half is kept:
  // the attempt's TCP bind then succeeds and its UDP bind cannot.
  let (listener, bound, gossip) = bind_stream_pair("127.0.0.1:0".parse().expect("loopback"))
    .await
    .expect("probe pair binds");
  // Ignoring Err: the probe listener exists only to reserve the port number,
  // and a close error would surface as the bind outcome asserted below.
  let _ = listener.close().await;

  let err = bind_tcp_first(bound)
    .await
    .expect_err("the UDP half is held, so the attempt must fail");
  assert_eq!(
    err.kind(),
    io::ErrorKind::AddrInUse,
    "a held UDP port is the ordinary collision",
  );
  assert!(
    is_port_conflict(&err),
    "the failure must be one the alternating retry can get past",
  );

  // The attempt's own listener is gone, so the TCP half is claimable again.
  let reclaimed = bind_tcp_exclusive(bound)
    .await
    .expect("the failed attempt must have released its listener");
  // Ignoring Err: the reclaimed listener exists only to prove the port came
  // back, and is discarded either way.
  let _ = reclaimed.close().await;
  let _ = gossip.close().await;
}

/// A UDP-led attempt whose TCP half is refused releases the gossip socket it
/// had already claimed, and hands the TCP error back.
///
/// This is the failure the alternation exists for: a TCP port still in
/// `TIME_WAIT` refuses the UDP-led attempt's second step, and the port the
/// attempt claimed for UDP must not stay held while the next one walks on.
#[compio::test]
async fn a_udp_led_attempt_releases_its_gossip_socket_when_tcp_refuses_the_port() {
  let (listener, bound, gossip) = bind_stream_pair("127.0.0.1:0".parse().expect("loopback"))
    .await
    .expect("probe pair binds");
  // Free the UDP half and keep the TCP half held: the mirror of the case above.
  // Ignoring Err: the probe gossip socket only reserved the port number.
  let _ = gossip.close().await;

  let err = bind_udp_first(bound)
    .await
    .expect_err("the TCP half is held, so the attempt must fail");
  assert_eq!(
    err.kind(),
    io::ErrorKind::AddrInUse,
    "a held TCP port is the ordinary collision",
  );
  assert!(
    is_port_conflict(&err),
    "the failure must be one the alternating retry can get past",
  );

  let reclaimed = bind_udp_exclusive(bound)
    .await
    .expect("the failed attempt must have released its gossip socket");
  // Ignoring Err: the reclaimed socket exists only to prove the port came back.
  let _ = reclaimed.close().await;
  let _ = listener.close().await;
}

/// A pair cannot be claimed on a TCP port an unrelated listener already owns.
///
/// This is the direct check that a claim never SHARES a port. The listener here
/// belongs to no memberlist node, so a claim that succeeded against it would
/// have split that owner's incoming connections between two accept queues —
/// and, on Windows, would have done so silently, because there an address-reuse
/// option on the second bind is enough to take the port from its owner.
#[compio::test]
async fn an_occupied_tcp_port_cannot_be_claimed_by_a_pair() {
  let held = std::net::TcpListener::bind("127.0.0.1:0").expect("a plain listener binds");
  let occupied = held.local_addr().expect("the held listener addr");

  let claimed = bind_stream_pair(occupied).await.map(|(_, bound, _)| bound);
  assert!(
    claimed.is_err(),
    "a pair took a TCP port an unrelated listener owns: {claimed:?}",
  );

  drop(held);
}

/// A pair cannot be claimed on a UDP port an unrelated socket already owns.
///
/// The gossip mirror of the case above, and the worse one: two sockets sharing
/// a UDP port receive each other's datagrams in an order no platform defines,
/// so a claim that succeeded here would lose gossip with nothing to observe it.
#[compio::test]
async fn an_occupied_udp_port_cannot_be_claimed_by_a_pair() {
  let held = std::net::UdpSocket::bind("127.0.0.1:0").expect("a plain socket binds");
  let occupied = held.local_addr().expect("the held socket addr");

  let claimed = bind_stream_pair(occupied).await.map(|(_, bound, _)| bound);
  assert!(
    claimed.is_err(),
    "a pair took a UDP port an unrelated socket owns: {claimed:?}",
  );

  drop(held);
}

/// An ephemeral bind that fails for a reason no other port can fix surfaces at
/// once instead of spending the retry budget.
///
/// The budget exists to walk PAST a port one protocol will not take. An address
/// this host does not hold refuses every port on it identically, so retrying
/// would only turn one error into sixteen — and delay it by however long
/// sixteen binds take.
#[compio::test]
async fn a_non_conflict_failure_surfaces_without_spending_the_budget() {
  // TEST-NET-1 is reserved for documentation, so it is configured on no
  // interface and the bind is refused for the ADDRESS rather than for the port.
  let advertise = "192.0.2.1:0".parse().expect("test-net-1");

  let started = std::time::Instant::now();
  let res = bind_stream_pair(advertise).await;
  let elapsed = started.elapsed();

  let MemberlistError::Io(err) = res.expect_err("an address this host does not hold") else {
    panic!("a refused bind surfaces as the io error it was");
  };
  assert!(
    !is_port_conflict(&err),
    "an unheld address fails for the address, not the port: {err:?}",
  );
  assert!(
    elapsed < std::time::Duration::from_secs(1),
    "the failure took {elapsed:?}, so it was retried instead of surfaced",
  );
}
