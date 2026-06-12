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
mod tests;
