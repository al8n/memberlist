//! AckRegistry — bookkeeping for outstanding ack/nack handlers keyed by
//! sequence number. Pure data structure; the `Endpoint` wires it into the probe FSM.

use crate::Instant;

use crate::FxHashMap;
use bytes::Bytes;

/// What category of probe / ping originated this ack-awaiting entry. Used by
/// `Endpoint` to dispatch the eventual ack/nack/timeout to the right
/// callback path.
#[derive(Debug, Clone)]
pub enum AckKind<A> {
  /// Direct probe ping issued by the local probe FSM.
  Probe,
  /// We're forwarding an indirect ping on behalf of another node; on ack,
  /// reply with an Ack to the carried `reply_to`. On timeout, the probe
  /// FSM treats this as a missed ack and may send a Nack.
  Forward(ForwardAck<A>),
  /// Application-level ping (via `Memberlist::ping`); on ack, emit a
  /// `PingCompleted` event with the RTT.
  Ping,
}

/// Payload of [`AckKind::Forward`]. Extracted as a named struct because
/// the codebase forbids struct enum variants — newtype variants only.
#[derive(Debug, Clone)]
pub struct ForwardAck<A> {
  reply_to: A,
}

impl<A> ForwardAck<A> {
  /// Construct a new forward-ack payload.
  #[inline(always)]
  pub const fn new(reply_to: A) -> Self {
    Self { reply_to }
  }

  /// Borrow the reply-to address.
  #[inline(always)]
  pub const fn reply_to_ref(&self) -> &A {
    &self.reply_to
  }

  /// Consume and return the reply-to address.
  #[inline(always)]
  pub fn into_reply_to(self) -> A {
    self.reply_to
  }
}

/// One pending ack-awaiting entry.
#[derive(Debug, Clone)]
pub struct AckEntry<A> {
  sent_at: Instant,
  deadline: Instant,
  kind: AckKind<A>,
}

impl<A> AckEntry<A> {
  /// Construct a new pending entry.
  #[inline(always)]
  pub const fn new(sent_at: Instant, deadline: Instant, kind: AckKind<A>) -> Self {
    Self {
      sent_at,
      deadline,
      kind,
    }
  }

  /// When this entry was registered (used to compute RTT on ack).
  #[inline(always)]
  pub const fn sent_at(&self) -> Instant {
    self.sent_at
  }

  /// Wall-clock deadline after which this entry is considered timed-out.
  #[inline(always)]
  pub const fn deadline(&self) -> Instant {
    self.deadline
  }

  /// Discriminator for downstream dispatch.
  #[inline(always)]
  pub const fn kind_ref(&self) -> &AckKind<A> {
    &self.kind
  }

  /// Consume the entry and return its `kind`.
  #[inline(always)]
  pub fn into_kind(self) -> AckKind<A> {
    self.kind
  }
}

/// Resolution of a pending ack-awaiting entry: either an ack arrival (with
/// payload + recv timestamp) or a timeout. Returned by
/// [`AckRegistry::handle_ack`] and [`AckRegistry::poll_expired`].
#[derive(Debug, Clone)]
pub struct AckResolution<A> {
  seq: u32,
  entry: AckEntry<A>,
  payload: Option<Bytes>,
  received_at: Option<Instant>,
}

impl<A> AckResolution<A> {
  /// The sequence number this resolution relates to.
  #[inline(always)]
  pub const fn seq(&self) -> u32 {
    self.seq
  }

  /// The registered entry.
  #[inline(always)]
  pub const fn entry_ref(&self) -> &AckEntry<A> {
    &self.entry
  }

  /// Consume the resolution and return its inner [`AckEntry`].
  #[inline(always)]
  pub fn into_entry(self) -> AckEntry<A> {
    self.entry
  }

  /// `Some(payload)` for acks (may be empty); `None` for timeouts.
  #[inline(always)]
  pub fn payload(&self) -> Option<&[u8]> {
    self.payload.as_deref()
  }

  /// Return a cheap clone of the payload buffer if present.
  #[inline(always)]
  pub fn payload_bytes(&self) -> Option<Bytes> {
    self.payload.clone()
  }

  /// Take ownership of the payload, leaving `None` behind.
  #[inline(always)]
  pub fn take_payload(&mut self) -> Option<Bytes> {
    self.payload.take()
  }

  /// `Some(now)` if this came from an ack with a recv timestamp;
  /// `None` for timeouts.
  #[inline(always)]
  pub const fn received_at(&self) -> Option<Instant> {
    self.received_at
  }
}

/// Registry of outstanding ack-awaiting entries keyed by sequence number.
///
/// Sync, single-threaded. Expiry is driven per-`seq` by the owning `Endpoint`:
/// a terminating probe or an expiring indirect-forward removes its own slot via
/// [`remove`](Self::remove), so removal is exactly paired with the registering
/// path and no orphan can accumulate. [`poll_expired`](Self::poll_expired) and
/// [`next_deadline`](Self::next_deadline) are global-sweep helpers retained for
/// callers that want a deadline-ordered backstop drain; the `Endpoint` does NOT
/// currently call them, so do not assume a slot is reaped by anything other than
/// its paired `remove`.
#[derive(Debug)]
pub struct AckRegistry<A> {
  pending: FxHashMap<u32, AckEntry<A>>,
}

impl<A> Default for AckRegistry<A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<A> AckRegistry<A> {
  /// Construct an empty registry.
  #[inline(always)]
  pub fn new() -> Self {
    Self {
      pending: FxHashMap::default(),
    }
  }

  /// Number of outstanding entries.
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.pending.len()
  }

  /// True iff no entries are outstanding.
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.pending.is_empty()
  }

  /// True iff `seq` is currently registered. Used by sequence allocation to
  /// skip a still-live slot on a `u32` wrap.
  #[inline(always)]
  pub fn contains(&self, seq: u32) -> bool {
    self.pending.contains_key(&seq)
  }

  /// Register a new pending entry. If `seq` was already present, the old
  /// entry is replaced and returned.
  #[inline(always)]
  pub fn register(&mut self, seq: u32, entry: AckEntry<A>) -> Option<AckEntry<A>> {
    self.pending.insert(seq, entry)
  }

  /// Process an incoming Ack. Returns the registered entry (now removed) and
  /// the payload, or `None` if `seq` was not pending.
  pub fn handle_ack(
    &mut self,
    seq: u32,
    payload: Bytes,
    received_at: Instant,
  ) -> Option<AckResolution<A>> {
    let entry = self.pending.remove(&seq)?;
    Some(AckResolution {
      seq,
      entry,
      payload: Some(payload),
      received_at: Some(received_at),
    })
  }

  /// Process an incoming Nack. Returns a *reference* to the entry without
  /// removing it (the entry stays pending until ack or timeout).
  /// Returns `None` if `seq` was not pending.
  pub fn handle_nack(&self, seq: u32) -> Option<&AckEntry<A>> {
    self.pending.get(&seq)
  }

  /// Peek the pending entry for `seq` WITHOUT removing it. Used by
  /// `handle_ack` to read the entry kind and validate the Ack's source
  /// address before consuming the slot — a spoofed Ack for a guessed seq
  /// must not be able to evict the entry the genuine responder still
  /// needs. Distinct from [`handle_nack`](Self::handle_nack)
  /// only in intent/name.
  pub fn get(&self, seq: u32) -> Option<&AckEntry<A>> {
    self.pending.get(&seq)
  }

  /// Remove the entry for exactly `seq`, if present. Targeted cleanup for
  /// callers that know which outstanding record to drop (an expiring
  /// indirect-forward, a terminating probe) and must NOT disturb other
  /// still-registered entries. Unlike [`poll_expired`](Self::poll_expired),
  /// which pops the globally-oldest expired entry, this only touches `seq`.
  pub fn remove(&mut self, seq: u32) -> Option<AckEntry<A>> {
    self.pending.remove(&seq)
  }

  /// Pop one entry whose deadline has passed (relative to `now`).
  /// Repeated calls drain all expired entries. Returns `None` once none remain.
  pub fn poll_expired(&mut self, now: Instant) -> Option<AckResolution<A>> {
    // Find the smallest-deadline expired entry. O(n) per call; acceptable
    // since the registry is typically small (bounded by indirect_checks +
    // a handful of in-flight pings). Switch to a heap if profiling demands.
    let seq = self
      .pending
      .iter()
      .filter(|(_, e)| e.deadline <= now)
      .min_by_key(|(_, e)| e.deadline)
      .map(|(s, _)| *s)?;

    let entry = self.pending.remove(&seq).expect("just found");
    Some(AckResolution {
      seq,
      entry,
      payload: None,
      received_at: None,
    })
  }

  /// Earliest pending deadline, or `None` if the registry is empty.
  /// Used to inform `Endpoint::poll_timeout()`.
  pub fn next_deadline(&self) -> Option<Instant> {
    self.pending.values().map(|e| e.deadline).min()
  }
}

#[cfg(test)]
mod tests {
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
}
