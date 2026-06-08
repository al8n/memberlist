//! Deterministic virtual TCP: per-connection ordered byte pipes with
//! clock-driven delivery latency and fault injection.
//!
//! Shared by the `tls`-gated and `tcp`-gated conformance harnesses. Models a
//! single direction of one TCP connection — the PEER writes into `inbound`,
//! the local side reads from it. FIFO, no reordering, no loss within a
//! connection (TCP is reliable + ordered); latency is the clock-driven
//! `deliver_at` instant attached to each chunk.
//!
//! The harness keeps two `TcpPipe`s per connection (one per direction), keyed
//! by the reader's `(addr, exchange)`. Fault hooks attach to a pipe and are
//! consumed at delivery time so a one-shot can fire exactly once per pipe.

use memberlist_proto::Instant;
use std::{collections::VecDeque, time::Duration};

/// One side's view of a virtual TCP connection: an ordered byte queue the PEER
/// writes into and this side reads from, plus a half-close marker. No
/// reordering, no loss within a connection (TCP is reliable + ordered); latency
/// is the clock-driven `deliver_at` on each chunk.
pub(crate) struct TcpPipe {
  /// Ciphertext chunks in flight to the reader, FIFO, each with a delivery
  /// instant (clock-driven latency). Ordered: a later chunk's `deliver_at` is
  /// `>=` the prior chunk's (monotone enqueue under one latency, plus the
  /// receive-window fragmentation which only ever pushes a chunk later).
  pub(crate) inbound: VecDeque<(Instant, bytes::Bytes)>,
  /// `Some(instant)` once the peer half-closed its write side (TCP FIN). The
  /// FIN is a SEPARATE segment that arrives strictly AFTER the last buffered
  /// data chunk — modeled as its own delivery instant — so the reader observes
  /// read==0 on a later tick than the final data, never back-to-back in the
  /// same delivery pass (which would deny the peer a tick to generate its
  /// push/pull response before honoring the FIN).
  pub(crate) fin_at: Option<Instant>,
  /// `true` once this connection was refused / reset (no further delivery).
  pub(crate) reset: bool,
  /// `true` once the read==0 anchor (the empty-slice `handle_transport_data`)
  /// has been delivered to the reader — fired exactly once after a clean
  /// half-close drains.
  pub(crate) eof_anchor_sent: bool,
  /// `true` once the `reset` anchor (the empty-slice surface of a refused /
  /// mid-stream-reset connection) has been delivered exactly once.
  pub(crate) reset_anchor_sent: bool,
  /// One-shot half-close-after-first-record arming: once the reader has been
  /// delivered one chunk, arm the FIN so the next read is read==0 (truncation
  /// mid-frame). `false` once consumed.
  pub(crate) half_close_after_first: bool,
  /// `true` to NEVER deliver the read==0 FIN anchor on this pipe even when a
  /// FIN is armed: models a peer that has logically half-closed its send
  /// direction (via an in-band TLS `close_notify`, or via a coordinator-side
  /// EOF latch on plain TCP) but whose TCP write-side FIN is deferred — so
  /// the reader must act on the buffered data on the SAME tick it is
  /// delivered, leaning only on the in-band/latched close signal, not on a
  /// later TCP read==0. The small-coalesced post-promotion drain regression
  /// knob; used by both the TLS and TCP harnesses.
  pub(crate) withhold_fin: bool,
  /// Per-connection receive-window cap in bytes (the `large_state` knob). A
  /// buffer larger than this cap is fragmented into `window`-sized pieces
  /// released one round-trip apart (NOT dropped — TCP never drops
  /// in-connection), so a large transfer drains across many ticks. `None` =
  /// unbounded (one chunk per write).
  pub(crate) window: Option<usize>,
}

impl TcpPipe {
  pub(crate) fn new(window: Option<usize>) -> Self {
    Self {
      inbound: VecDeque::new(),
      fin_at: None,
      reset: false,
      eof_anchor_sent: false,
      reset_anchor_sent: false,
      half_close_after_first: false,
      withhold_fin: false,
      window,
    }
  }

  /// Arm the FIN segment (idempotent — keeps the earliest arm): it arrives one
  /// `stride` after the latest already-queued data chunk (or `now`), so the
  /// read==0 anchor lands strictly after the last data byte on a later instant.
  pub(crate) fn arm_fin(&mut self, now: Instant, stride: Duration) {
    if self.fin_at.is_some() {
      return;
    }
    let after = self.inbound.back().map(|(t, _)| *t).unwrap_or(now).max(now);
    self.fin_at = Some(after + stride);
  }
}
