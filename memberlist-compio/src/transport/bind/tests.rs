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
