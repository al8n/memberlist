use super::*;
use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

fn addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

fn entry(deadline: Instant, kind: AckKind<SocketAddr>) -> AckEntry<SocketAddr> {
  AckEntry::new(deadline - Duration::from_millis(50), deadline, kind)
}

#[test]
fn empty_registry() {
  let r: AckRegistry<SocketAddr> = AckRegistry::new();
  assert_eq!(r.len(), 0);
  assert!(r.is_empty());
  assert!(r.next_deadline().is_none());
}

#[test]
fn register_then_handle_ack_returns_and_removes() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  let deadline = now + Duration::from_millis(500);
  r.register(7, entry(deadline, AckKind::Probe));
  assert_eq!(r.len(), 1);

  let payload = Bytes::from_static(b"pong");
  let resolution = r.handle_ack(7, payload.clone(), now).expect("ack found");
  assert_eq!(resolution.seq(), 7);
  assert_eq!(resolution.payload(), Some(payload.as_ref()));
  assert!(matches!(resolution.entry_ref().kind_ref(), AckKind::Probe));
  assert_eq!(r.len(), 0);
}

#[test]
fn handle_ack_for_unknown_seq_returns_none() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  assert!(r.handle_ack(7, Bytes::new(), Instant::now()).is_none());
}

#[test]
fn handle_nack_does_not_remove() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  let deadline = now + Duration::from_millis(500);
  r.register(7, entry(deadline, AckKind::Probe));
  let e = r.handle_nack(7);
  assert!(e.is_some());
  assert_eq!(r.len(), 1, "nack should not remove the entry");
}

#[test]
fn poll_expired_returns_none_before_deadline() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  r.register(7, entry(now + Duration::from_secs(1), AckKind::Probe));
  assert!(r.poll_expired(now).is_none());
  assert_eq!(r.len(), 1);
}

#[test]
fn poll_expired_pops_oldest_first() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  r.register(1, entry(now - Duration::from_millis(100), AckKind::Probe));
  r.register(2, entry(now - Duration::from_millis(200), AckKind::Probe));
  r.register(3, entry(now + Duration::from_secs(10), AckKind::Probe));

  let first = r.poll_expired(now).expect("entry 2 should expire first");
  assert_eq!(first.seq(), 2);
  let second = r.poll_expired(now).expect("entry 1 should expire next");
  assert_eq!(second.seq(), 1);
  assert!(r.poll_expired(now).is_none(), "entry 3 not yet expired");
  assert_eq!(r.len(), 1);
}

/// `remove(seq)` is targeted — it must not disturb other
/// entries the way `poll_expired` (global oldest) would. Models the
/// `fire_expired_forwards` case: an older, still-registered direct-probe
/// entry must survive when a newer indirect-forward entry is cleaned up.
#[test]
fn remove_is_targeted_not_oldest() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  // Older probe entry (intentionally left registered past its deadline so
  // a relayed Ack can still match) + a newer forward entry that expired.
  r.register(1, entry(now - Duration::from_millis(200), AckKind::Probe));
  r.register(
    2,
    entry(
      now - Duration::from_millis(50),
      AckKind::Forward(ForwardAck { reply_to: addr(9) }),
    ),
  );

  // Targeted removal of the forward must NOT evict the older probe
  // (poll_expired would have popped seq 1, the global oldest).
  let removed = r.remove(2).expect("forward entry removed");
  assert!(matches!(removed.kind_ref(), AckKind::Forward(_)));
  assert_eq!(r.len(), 1);
  assert!(r.handle_nack(1).is_some(), "probe entry must survive");
  assert!(r.remove(999).is_none(), "removing an absent seq is a no-op");
}

#[test]
fn next_deadline_returns_minimum() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  r.register(1, entry(now + Duration::from_secs(5), AckKind::Probe));
  r.register(2, entry(now + Duration::from_secs(2), AckKind::Probe));
  r.register(3, entry(now + Duration::from_secs(10), AckKind::Probe));
  assert_eq!(r.next_deadline(), Some(now + Duration::from_secs(2)));
}

#[test]
fn forward_kind_carries_reply_addr() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  let reply = addr(7000);
  r.register(
    7,
    entry(
      now + Duration::from_millis(500),
      AckKind::Forward(ForwardAck { reply_to: reply }),
    ),
  );

  let resolution = r.handle_ack(7, Bytes::new(), now).expect("ack");
  match resolution.into_entry().into_kind() {
    AckKind::Forward(ForwardAck { reply_to }) => assert_eq!(reply_to, reply),
    _ => panic!("expected Forward kind"),
  }
}

#[test]
fn forward_ack_constructor_and_accessors() {
  let fa = ForwardAck::new(addr(8000));
  assert_eq!(fa.reply_to_ref(), &addr(8000));
  assert_eq!(fa.into_reply_to(), addr(8000));
}

#[test]
fn default_registry_is_empty() {
  let r: AckRegistry<SocketAddr> = AckRegistry::default();
  assert!(r.is_empty());
  assert_eq!(r.len(), 0);
}

#[test]
fn register_replaces_and_returns_old_entry() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  let first = entry(now + Duration::from_secs(1), AckKind::Probe);
  assert!(r.register(7, first).is_none(), "fresh seq returns None");
  let second = entry(now + Duration::from_secs(2), AckKind::Ping);
  let old = r.register(7, second).expect("replaced entry returned");
  assert!(matches!(old.into_kind(), AckKind::Probe));
  assert_eq!(r.len(), 1, "replacement keeps a single entry");
  assert!(matches!(r.get(7).unwrap().kind_ref(), AckKind::Ping));
}

#[test]
fn ack_entry_exposes_sent_at_and_deadline() {
  let now = Instant::now();
  let deadline = now + Duration::from_millis(500);
  let e = entry(deadline, AckKind::Ping);
  assert_eq!(e.sent_at(), deadline - Duration::from_millis(50));
  assert_eq!(e.deadline(), deadline);
  assert!(matches!(e.kind_ref(), AckKind::Ping));
}

#[test]
fn get_peeks_without_removing() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  r.register(7, entry(now + Duration::from_secs(1), AckKind::Probe));
  assert!(r.get(7).is_some());
  assert_eq!(r.len(), 1, "get must not remove");
  assert!(r.get(999).is_none(), "absent seq peeks None");
}

#[test]
fn handle_nack_for_unknown_seq_returns_none() {
  let r: AckRegistry<SocketAddr> = AckRegistry::new();
  assert!(r.handle_nack(7).is_none());
}

#[test]
fn ack_resolution_payload_accessors() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  r.register(7, entry(now + Duration::from_secs(1), AckKind::Probe));
  let mut resolution = r
    .handle_ack(7, Bytes::from_static(b"pong"), now)
    .expect("ack");
  // received_at carries the ack timestamp; payload_bytes is a cheap clone.
  assert_eq!(resolution.received_at(), Some(now));
  assert_eq!(resolution.payload_bytes().as_deref(), Some(&b"pong"[..]));
  // take_payload moves the buffer out, leaving None behind.
  assert_eq!(resolution.take_payload().as_deref(), Some(&b"pong"[..]));
  assert!(resolution.payload().is_none(), "payload taken");
  assert!(resolution.payload_bytes().is_none());
}

#[test]
fn timeout_resolution_has_no_payload_or_timestamp() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  r.register(7, entry(now - Duration::from_millis(10), AckKind::Probe));
  let resolution = r.poll_expired(now).expect("expired");
  // A timeout resolution carries neither payload nor recv timestamp.
  assert!(resolution.payload().is_none());
  assert!(resolution.received_at().is_none());
  assert!(matches!(resolution.entry_ref().kind_ref(), AckKind::Probe));
}

#[test]
fn poll_expired_includes_entry_exactly_at_deadline() {
  let mut r: AckRegistry<SocketAddr> = AckRegistry::new();
  let now = Instant::now();
  // deadline == now must count as expired (`deadline <= now`).
  r.register(
    7,
    AckEntry::new(now - Duration::from_secs(1), now, AckKind::Probe),
  );
  assert!(r.poll_expired(now).is_some(), "deadline == now is expired");
}
