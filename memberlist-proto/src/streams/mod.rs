//! Composed stream-transport ⊕ memberlist Sans-I/O super-state-machine.
//!
//! `StreamEndpoint<I, A, R>` runs memberlist over a record-layer-shaped
//! reliable transport (`R: StreamTransport`) for reliable exchanges and
//! plain UDP for gossip. `R` is the per-transport plug: `tls::TlsRecords`
//! for TLS-over-TCP; `tcp::RawRecords` for plain TCP; a future
//! `EncryptedRecords` for the cross-transport keyring workstream.
//!
//! Carries reliable exchanges over a per-exchange transport connection;
//! plain UDP carries unreliable gossip. The reliable and unreliable paths are
//! separate sockets, so there is no first-byte demux. Two ingress paths:
//! gossip via [`StreamEndpoint::handle_gossip`] (buffered raw for the
//! codec-owning driver), and per-exchange bytes via
//! [`StreamEndpoint::handle_transport_data`] (routed into the owning bridge,
//! then a coordinator tick). Outbound, the coordinator surfaces bytes keyed by
//! exchange handle + peer ([`StreamEndpoint::poll_transport_transmit`]),
//! connect / half-close / teardown signals
//! ([`StreamEndpoint::poll_action`]), and the unreliable gossip [`Transmit`]
//! stream ([`StreamEndpoint::poll_memberlist_transmit`]).
//!
//! The fixed per-tick step order keeps the load-bearing invariant — the
//! stream-endpoint-event drain STRICTLY precedes `Endpoint::handle_timeout`
//! (else a reliable-fallback ping ack that lands the same tick the probe
//! cumulative deadline expires is lost and the peer is wrongly Suspected).
//!
//! # Transport half-close anchors
//!
//! A record layer with an in-band close (TLS `close_notify`) anchors its
//! send-half close on the in-band alert its `send_close_notify()` queues; a
//! record layer with no in-band close (plain TCP) emits no bytes there and the
//! send-half close is the out-of-band transport FIN (`shutdown(write)`) the
//! driver issues. Either way the coordinator emits one [`StreamAction::Shutdown`]
//! per exchange once the bridge retires its send half: for an in-band-close
//! transport that action is the transport-level companion to the already-queued
//! alert; for plain TCP it is the ONLY signal the driver gets that the FIN is
//! owed.
//!
//! # Driver output-drain ordering
//!
//! The coordinator self-orders its outputs so a driver doing the natural
//! "drain actions, drain transmits, drain memberlist, drain ingress, sleep
//! until `poll_timeout`, repeat" loop is correct:
//!
//! - [`StreamEndpoint::poll_action`] returns [`StreamAction::Connect`] first.
//! - Once all `Connect`s are drained, [`StreamAction::Shutdown`] /
//!   [`StreamAction::Close`] / [`StreamAction::Abort`] for an exchange is
//!   withheld while [`StreamEndpoint::poll_transport_transmit`] still holds
//!   bytes tagged with that exchange — a teardown applied before its exchange's
//!   last bytes are written would orphan them (a transport `shutdown(write)`
//!   closes the send
//!   half, so any later write fails).
//! - Once the matching bytes drain, the teardown surfaces.
//!
//! The driver does NOT need to partition actions itself; the natural drain
//! loop, repeated until no method makes progress, sequences
//! `Connect` → bytes → `Shutdown` / `Close` for each exchange correctly.

pub(crate) mod action;
pub(crate) mod bridge;
pub(crate) mod conn;
pub(crate) mod label;
pub(crate) mod labeled;
pub(crate) mod phase;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod transport;

pub use action::{ConnectInfo, ExchangeRef, StreamAction};
pub use conn::ExchangeId;
pub use labeled::{LabelOptions, LabelOptionsError, Labeled, Passthrough};
// `Intake` is part of the `StreamTransport` surface a record-layer impl
// returns; re-exported alongside the trait though the coordinator itself
// drives it only through the bridge.
pub use transport::{Intake, StreamTransport};

use crate::{FxHashMap, Instant};
use core::{net::SocketAddr, time::Duration};
#[cfg(not(feature = "std"))]
use std::{boxed::Box, string::String, vec::Vec};

use bytes::Bytes;

use crate::{
  endpoint::Endpoint,
  event::{Event, ExchangeCompleted, ExchangeOutcome, PushPullKind, StreamId, Transmit},
};
use bridge::StreamBridge;
use conn::StreamConns;

/// Handshake completion budget for an inbound (accepted) exchange.
///
/// Historical default for the server-side accept handshake deadline,
/// retained as documentation. The runtime value is read from
/// [`crate::config::EndpointOptions::accept_handshake_deadline`] —
/// operators tune via `EndpointOptions::with_accept_handshake_deadline`;
/// see [`crate::config::DEFAULT_ACCEPT_HANDSHAKE_DEADLINE`].
///
/// An accepted connection has no `Event::DialRequested` exchange
/// deadline (only outbound dials carry that); the coordinator bounds
/// its label / handshake step with this per-endpoint budget. A label /
/// handshake step that has not settled by `now +
/// accept_handshake_deadline` is reaped by the bridge's
/// `poll_timeout` / `pump_out` deadline path with no `Stream` minted.
#[allow(dead_code)]
const ACCEPT_HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// One pending dial intent the coordinator owes a `service_dials` attempt to.
///
/// `attempted` distinguishes a freshly-sieved entry (never yet processed by
/// `service_dials`) from one that has been processed at least once. Freshly-
/// sieved entries get an immediate-due wake out of `poll_timeout` so a caller
/// that advances solely by `poll_timeout` cannot orphan them. Once
/// `service_dials` attempts the entry, `attempted` becomes `true` and stays
/// `true`. The dial always succeeds in surfacing a `Connect` action and
/// inserting a `Handshaking` bridge on its first attempt (there is no pooled-
/// connection credit to wait on), so an attempted entry never requeues; the
/// `attempted` bit is retained verbatim from the sibling QUIC coordinator for
/// the immediate-due wake discipline.
struct PendingDial<A> {
  id: StreamId,
  peer: A,
  deadline: Instant,
  attempted: bool,
}

/// How the coordinator mints the `Stream` for an exchange once its label /
/// handshake step settles. The membership `Endpoint` stays the sole `Stream`
/// factory; the bridge spends the whole label / handshake window with
/// `stream = None`. Newtype variants over the existing key types (no
/// multi-field variants).
enum PendingMint<A> {
  /// Outbound dial: mint via `Endpoint::dial_succeeded(stream_id, now)` using
  /// the `StreamId` the inner endpoint allocated for the originating `start_*`.
  Outbound(StreamId),
  /// Inbound accept: mint via `Endpoint::accept_stream(peer, now)`.
  Inbound(A),
}

/// Per-exchange metadata the coordinator holds for the whole lifetime of one
/// reliable exchange (the bridge itself lives in [`StreamConns`], keyed by the
/// same [`ExchangeId`]). Accessor-only — private fields, read directly within
/// this module.
struct ExchangeMeta<A> {
  /// The peer `SocketAddr` every `poll_transport_transmit` for this exchange is
  /// tagged with so the driver writes the bytes on the right transport
  /// connection.
  peer_socket: SocketAddr,
  /// The membership address (`A`) of the peer. Carried alongside
  /// `peer_socket` so the bridge-reap path can emit the generic
  /// [`Event::ExchangeCompleted`] payload without a back-conversion from
  /// `SocketAddr` to `A`.
  peer: A,
  /// `Some` until the label / handshake step settles and the coordinator mints
  /// + promotes the `Stream`; `None` afterwards (an `Established` bridge needs
  ///   no further minting decision).
  mint: Option<PendingMint<A>>,
  /// `true` once the coordinator has emitted this exchange's one
  /// [`StreamAction::Shutdown`] (graceful transport write-side half-close after
  /// the bridge retired its send half). Guards a second emission. For a record
  /// layer with an in-band close the half-close is the `close_notify` alert and
  /// the latch records that the transport-level FIN companion is owed; for
  /// plain TCP the half-close is the out-of-band `shutdown(write)` and the
  /// latch records that the FIN signal itself is owed to the driver.
  fin_emitted: bool,
  /// Which `start_*` produced this exchange — populated only for outbound
  /// exchanges by the `start_push_pull` / `start_reliable_ping` /
  /// `start_user_message` wrappers via `pending_outbound_kinds`. `None`
  /// for inbound (server-side) exchanges accepted via
  /// [`Self::accept_connection`]; the bridge label-decode step settles
  /// the inbound kind but the coordinator does not re-emit
  /// kind-specific terminal events for the inbound side (drivers that
  /// initiate join_with against a peer rely only on their own outbound
  /// exchange's terminal event). Gates the
  /// [`Event::ExchangeCompleted`] emission in `reap_bridge` — when
  /// `Some`, the kind carried on the emitted payload lets consumers
  /// filter (e.g. sync-join consumes only `ExchangeKind::PushPull`).
  kind: Option<crate::event::ExchangeKind>,
  /// `true` for an outbound exchange — one WE dialed, via any `start_*` wrapper
  /// OR the scheduler's anti-entropy push/pull — and `false` for an inbound
  /// exchange accepted from a peer. Set explicitly at creation and never
  /// inferred from `kind` (which only the public wrappers populate, so a
  /// scheduler-created push/pull would otherwise look inbound). `leave()` uses
  /// this to cancel exactly the outbound exchanges whose request is not yet on
  /// the wire.
  outbound: bool,
}

/// The [`ExchangeId`] a teardown directive ([`StreamAction::Shutdown`] /
/// [`StreamAction::Close`] / [`StreamAction::Abort`]) refers to. Panics on a
/// `Connect` — the caller must hand only teardown actions in.
fn teardown_exchange(action: &StreamAction) -> ExchangeId {
  match action {
    StreamAction::Shutdown(r) | StreamAction::Close(r) | StreamAction::Abort(r) => r.id(),
    StreamAction::Connect(_) => unreachable!("teardown_exchange called on a Connect action"),
  }
}

/// Coordinator: `memberlist::Endpoint` (unreliable gossip + membership)
/// composed with a per-exchange record-layer-shaped reliable transport
/// (`R: StreamTransport`). Pure Sans-I/O — inject `now`.
///
/// The coordinator consults the stored [`Self::sni_provider`] closure for the
/// per-dial verification identity (unused on transports — plain TCP — that
/// need no record-layer certificate verification) and the stored
/// [`Self::peer_to_socket`] closure to derive a transport `SocketAddr` for a
/// membership `A`.
// Storage-shape bound: `cfg: R::Options` requires `R: StreamTransport` for the
// field type to be well-formed (rule §8 intrinsic exception). `ep: Endpoint<I, A>`
// carries no I/A bounds at the struct level — those bounds appear only on the impl
// blocks whose methods call `R::dial_context::<A>` and read the closures.
pub struct StreamEndpoint<I, A, R: StreamTransport> {
  ep: Endpoint<I, A>,
  cfg: R::Options,
  /// Per-peer SNI provider for the TLS reliable path. Returns `None` for
  /// transports that do not require SNI (plain TCP) or for peers whose
  /// verification identity is unknown.
  ///
  /// Stored as a boxed closure so the provider can be supplied at construction
  /// without an extra generic parameter on [`StreamEndpoint`]. Production
  /// drivers wire a deployment-aware lookup; the sim harness and test fixtures
  /// hard-code `Some("localhost".to_string())` to match the localhost-SAN test
  /// certs.
  sni_provider: Box<dyn Fn(&A) -> Option<String> + Send + Sync>,
  /// Maps a membership address `A` to the transport `SocketAddr` the driver
  /// uses to write outbound bytes (and that tags entries on
  /// [`Self::out_transmit`] + the [`StreamAction::Connect`] payload). Drivers
  /// where `A = SocketAddr` pass `Box::new(|addr: &SocketAddr| *addr)`;
  /// drivers with a non-`SocketAddr` membership address supply a deployment-
  /// aware resolver. Stored as a boxed closure for the same reason as
  /// [`Self::sni_provider`] (no extra generic on the struct).
  peer_to_socket: Box<dyn Fn(&A) -> SocketAddr + Send + Sync>,
  /// Cross-transport compression configuration. The coordinator is the single
  /// compress/decompress point on both the gossip and reliable paths; a
  /// disabled `CompressionOptions` makes both paths identity.
  compression: crate::CompressionOptions,
  /// Cross-transport encryption configuration. Applied across the unsecure
  /// paths (UDP gossip on every coordinator; the plain-TCP reliable path).
  /// On TLS the reliable path skips encryption (`R::is_secure() == true`);
  /// gossip is still encrypted (gossip is always plain UDP). A disabled
  /// configuration reduces all codec paths to identity.
  encryption: crate::EncryptionOptions,
  /// Checksum configuration for the gossip (unreliable) plane. A checksum
  /// guards the connectionless UDP datagram path — the only path without
  /// transport-level integrity — so it is applied in [`Self::checksum_gossip`]
  /// and not on the reliable stream bridge, which relies on the stream
  /// transport's own end-to-end integrity (matching the original Go memberlist
  /// and the legacy port, where the CRC rides the UDP packet path only). A
  /// disabled `ChecksumOptions` makes the gossip path identity.
  checksum: crate::ChecksumOptions,
  /// In-flight reliable exchanges (one bridge each), keyed by [`ExchangeId`].
  /// Connection-per-exchange — no pool, slab, or drained-reap.
  conns: StreamConns<I, A, R>,
  /// Per-exchange coordinator metadata, keyed in lockstep with `conns`. An
  /// outbound exchange carries its originating `StreamId` in
  /// [`PendingMint::Outbound`] here, so the mint-on-label-settled step maps an
  /// exchange to its `dial_succeeded` `StreamId` by iterating these directly —
  /// no separate `StreamId -> ExchangeId` reverse index is needed.
  exchanges: FxHashMap<ExchangeId, ExchangeMeta<A>>,
  /// Outbound per-exchange bytes produced this tick (the one-time label
  /// prefix, then application bytes), tagged with the exchange handle + peer
  /// so the driver writes them on the right transport connection. Drained via
  /// [`Self::poll_transport_transmit`].
  out_transmit: std::collections::VecDeque<(ExchangeId, SocketAddr, Bytes)>,
  /// Outbound [`StreamAction::Connect`] directives, in producer order. Drained
  /// first by [`Self::poll_action`] so a fresh dial's connection always opens
  /// before any same-tick `Shutdown` / `Close` targets an existing bridge's
  /// connection.
  pending_connects: std::collections::VecDeque<StreamAction>,
  /// Outbound [`StreamAction::Shutdown`] / [`StreamAction::Close`] /
  /// [`StreamAction::Abort`] directives, in producer order. Drained by
  /// [`Self::poll_action`] only after [`Self::pending_connects`] is exhausted
  /// AND the targeted exchange's bytes have left [`Self::out_transmit`] —
  /// withholding a teardown behind its own transmit prevents a transport
  /// `shutdown(write)` from orphaning bytes the exchange still owes. Teardowns
  /// retain their producer order.
  pending_teardowns: std::collections::VecDeque<StreamAction>,
  /// Raw inbound gossip datagrams. `memberlist-proto` has no umbrella
  /// `codec` dependency, so the coordinator cannot decode them in-crate and
  /// MUST NOT silently drop them (that would lose every UDP
  /// ping/ack/alive/suspect on the composed unit's public ingress). They are
  /// buffered here and surfaced via [`Self::poll_memberlist_ingress`] for the
  /// codec-owning layer to unwrap and feed back through [`Self::handle_packet`].
  mem_ingress: std::collections::VecDeque<(A, Bytes)>,
  /// Private queue of pending dial intents. `memberlist::Endpoint::poll_event`
  /// emits `Event::DialRequested { id, peer, deadline }` for an external
  /// driver to dial — but in the composed design `StreamEndpoint` IS the
  /// driver: `service_dials` surfaces the `Connect` action and builds the
  /// bridge itself. If `DialRequested` leaked through [`Self::poll_event`] an
  /// external caller draining events between `handle_timeout` and the next
  /// `service_dials` would pop the intent and silently drop it — the bridge
  /// would never open and the exchange would never run. The coordinator
  /// therefore sieves `Event::DialRequested` into this private deque; external
  /// pollers only ever observe application-visible events. Each entry carries
  /// an `attempted` bit so a freshly-sieved intent surfaces in
  /// [`Self::poll_timeout`] as an immediate-due wake — see [`PendingDial`].
  dial_pending: std::collections::VecDeque<PendingDial<A>>,
  /// Tags each outbound `StreamId` with the originating
  /// [`ExchangeKind`] so `service_dials` can stamp the resulting
  /// `ExchangeMeta` with the right kind, and `reap_bridge` can carry
  /// that kind on the uniform [`Event::ExchangeCompleted`] terminal
  /// event. The reap fires for ALL outbound kinds (push/pull,
  /// reliable ping, reliable user message); consumers filter on the
  /// payload's `kind()` to focus on the bridges they care about.
  /// Populated by the `start_*` wrappers above the call to
  /// `service_dials`; drained at the matching `ExchangeMeta`
  /// allocation inside `service_dials`. Strictly outbound-only —
  /// inbound (server-side) exchanges are not assigned a kind by the
  /// initiator and never appear in this table.
  pending_outbound_kinds: FxHashMap<StreamId, crate::event::ExchangeKind>,
  /// Most recent `now: Instant` injected by any `handle_*` / `start_*` wrapper.
  /// Used by [`Self::poll_timeout`] as the known-past anchor for the
  /// immediate-due wake of an unattempted `dial_pending` entry: the only way
  /// to signal "fire as soon as possible" out of an `Option<Instant>`
  /// Sans-I/O API is to return an `Instant <= caller's now`, and the only
  /// such anchor we may hold is one we observed from a prior `handle_*` call
  /// (Sans-I/O forbids `Instant::now()`). Stays `None` only before the very
  /// first `handle_*` / `start_*` call.
  last_now: Option<Instant>,
  /// Latch set by [`Self::set_encryption_options`] when it failed at least
  /// one bridge as part of a runtime policy change. A terminal bridge
  /// returns no per-bridge timeout, and an idle endpoint may have no
  /// scheduler timeout at all — without this latch the failed bridges
  /// would sit in [`Self::conns`] indefinitely (the `Close` already
  /// surfaces from [`Self::poll_action`], but the bridge state lingers
  /// until the next external tick reaps it).
  ///
  /// While the latch is set, [`Self::poll_timeout`] folds [`Self::last_now`]
  /// into the returned `min` so the driver wakes immediately and calls
  /// [`Self::handle_timeout`], whose `pump_bridges` reaps every terminal
  /// bridge in the same tick that clears the latch. The latch can only
  /// be set after a bridge exists in `conns`, and a bridge only exists
  /// after a `start_*` / `handle_*` call has anchored `last_now`, so the
  /// wake is always reachable.
  policy_reap_pending: bool,
}

// Accessors whose bodies touch only non-generic fields (`compression`,
// `encryption`, `out_transmit`, `pending_connects`, `pending_teardowns`,
// `mem_ingress`) or delegate to `Endpoint`'s own accessor surface
// (`endpoint()`, `gossip_mtu()`). Re-states only the struct's
// well-formedness bag — no method-side additions, so the heavier
// `I: Debug + Display + Send + Sync + 'static` constraint carried by the
// impl blocks below is NOT required to call any of these.
impl<I, A, R> StreamEndpoint<I, A, R>
where
  I: crate::Id + crate::Data + crate::CheapClone,
  A: crate::Data
    + crate::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  /// Borrow the inner membership endpoint (members / queue_user_broadcast / …).
  #[inline(always)]
  pub fn endpoint_ref(&self) -> &Endpoint<I, A> {
    &self.ep
  }

  /// Resolve a peer's membership address to its transport `SocketAddr`,
  /// using the coordinator's per-peer `peer_to_socket` closure supplied at
  /// construction. The driver consults this when it holds a peer `A` (e.g.
  /// the peer carried on an outbound `Transmit`) and needs the
  /// `SocketAddr` to drive the underlying socket.
  pub fn resolve_peer_socket(&self, peer: &A) -> SocketAddr {
    (self.peer_to_socket)(peer)
  }

  /// The configured plaintext-byte ceiling for an outbound gossip datagram.
  /// Sourced from [`crate::config::EndpointOptions::gossip_mtu`] (default
  /// [`crate::config::DEFAULT_GOSSIP_MTU`]). The on-wire datagram may
  /// exceed this by [`crate::ENCRYPTED_WRAPPER_OVERHEAD`] when
  /// encryption is enabled.
  pub fn gossip_mtu(&self) -> usize {
    self.ep.gossip_mtu()
  }

  /// The configured cross-transport compression options.
  pub fn compression(&self) -> crate::CompressionOptions {
    self.compression
  }

  /// Compress one outbound gossip datagram for the wire. The codec-owning
  /// driver calls this on the bytes it produced from a [`Transmit`] before
  /// handing them to the UDP socket. When compression is disabled, or the
  /// datagram does not benefit, the original bytes are returned.
  pub fn compress_gossip(&self, datagram: &[u8]) -> Vec<u8> {
    match self.compression.apply(datagram) {
      Ok(crate::CompressionOutcome::Compressed(packed)) => {
        let wrapped = crate::encode_compressed_frame(
          self
            .compression
            .algorithm()
            .expect("a Compressed outcome implies an algorithm is set"),
          datagram.len(),
          &packed,
        );
        // The wrapper header (tag + algorithm + `orig_len` varint) is overhead
        // on top of the raw compressed bytes; if it pushes the wrapped
        // datagram to `datagram`'s size or larger, send `datagram` plain so
        // compressed gossip can never inflate past the uncompressed datagram
        // (and never cross a gossip MTU the plain datagram already fit
        // within). The receiver's `unwrap_transforms` passes a non-wrapper
        // buffer through unchanged.
        if wrapped.len() < datagram.len() {
          wrapped
        } else {
          datagram.to_vec()
        }
      }
      // Plain outcome, or a backend error: emit the datagram uncompressed. A
      // backend compress error is non-fatal — the uncompressed datagram is
      // always valid and the decoder's leading tag tells it which form it got.
      _ => datagram.to_vec(),
    }
  }

  /// The configured cross-transport checksum options.
  pub fn checksum(&self) -> crate::ChecksumOptions {
    self.checksum
  }

  /// Wrap one outbound gossip datagram in a checksum frame for the wire. The
  /// codec-owning driver calls this on the already-compressed gossip bytes
  /// (from [`Self::compress_gossip`]) BEFORE [`Self::encrypt_gossip`], so the
  /// on-wire order is `[Encrypted[Checksumed[Compressed[frame]]]]`. When
  /// checksumming is disabled the bytes are returned unchanged.
  ///
  /// Returns `Err` when a checksum algorithm is configured but its backend was
  /// not built into this binary; the driver MUST drop the gossip rather than
  /// emit an unverifiable datagram, mirroring [`Self::encrypt_gossip`].
  pub fn checksum_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::ChecksumError> {
    match self.checksum.apply(datagram)? {
      crate::ChecksumOutcome::Checksumed(framed) => Ok(framed),
      crate::ChecksumOutcome::Plain => Ok(datagram.to_vec()),
    }
  }

  /// The configured cross-transport encryption options.
  pub fn encryption_options(&self) -> &crate::EncryptionOptions {
    &self.encryption
  }

  /// Encrypt one outbound gossip datagram for the wire. The codec-owning
  /// driver calls this on the already-compressed-and-checksummed gossip bytes
  /// (from [`Self::compress_gossip`] → [`Self::checksum_gossip`]) before
  /// handing them to the UDP socket. When encryption is disabled the bytes are
  /// returned unchanged.
  ///
  /// The on-wire byte order with the full stack is
  /// `[Encrypted[Checksumed[Compressed[frame]]]]`; each disabled layer
  /// collapses to identity.
  ///
  /// Returns `Err` when encryption is configured but the backend rejects the
  /// request — typically [`crate::EncryptionError::UnsupportedAlgorithm`]
  /// for a primary key whose backend was not built into this binary. The
  /// driver MUST drop the gossip in that case; emitting unencrypted bytes
  /// on an encrypted-cluster path would bypass authentication silently.
  pub fn encrypt_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::EncryptionError> {
    let keyring = match self.encryption.keyring() {
      Some(kr) => kr,
      None => return Ok(datagram.to_vec()),
    };
    let key = keyring.primary_ref();
    crate::encode_encrypted_frame(key.algorithm(), key, datagram)
  }

  /// Unwrap one inbound gossip datagram. The codec-owning driver calls this
  /// on the raw bytes from [`Self::poll_memberlist_ingress`] BEFORE decoding
  /// frames — it strips the Encrypted-then-Checksumed-then-Compressed wrapper
  /// stack in one pass (each layer identity when its wrapper is absent, the
  /// checksum layer verifying the digest as it strips). A datagram with no
  /// Encrypted wrapper is returned unchanged when no keyring is configured;
  /// when a keyring IS configured the strict-mode entry check rejects any
  /// non-Encrypted leading tag. A corrupt or unknown-algorithm wrapper, a
  /// checksum mismatch, or a frame the keyring cannot decrypt, is an `Err` —
  /// the driver drops the datagram (gossip is lossy and self-healing).
  ///
  /// This is the SINGLE canonical ingress unwrap on the coordinator. The
  /// outbound side uses [`Self::compress_gossip`] → [`Self::checksum_gossip`]
  /// → [`Self::encrypt_gossip`] so the on-wire order is
  /// `[Encrypted[Checksumed[Compressed[frame]]]]`; this helper reverses all
  /// layers, so authentication and integrity never depend on integration
  /// discipline.
  pub fn decrypt_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::FrameError> {
    // Ceiling is the gossip MTU — the maximum size any compliant gossip
    // datagram decompresses to. A wrapper claiming more is not a compliant
    // datagram and is rejected. The encryption-aware unwrap consumes an
    // Encrypted wrapper through the keyring, then verifies and strips a
    // Checksumed wrapper, then strips a Compressed wrapper if present; a
    // non-Encrypted-led datagram is returned unchanged when no keyring is
    // configured (the strict-mode entry check is gated on
    // `encryption.is_enabled()`).
    crate::unwrap_transforms_with_encryption(datagram, self.ep.gossip_mtu(), &self.encryption)
      .map(|cow| cow.into_owned())
  }

  /// Next outbound per-exchange bytes `(exchange, peer, bytes)`, if any.
  /// The driver writes `bytes` on the transport connection for `exchange` (to
  /// `peer`).
  pub fn poll_transport_transmit(&mut self) -> Option<(ExchangeId, SocketAddr, Bytes)> {
    self.out_transmit.pop_front()
  }

  /// Next outbound transport directive ([`StreamAction`]), if any.
  ///
  /// The coordinator self-orders its outputs so a driver doing the natural
  /// "drain actions, drain transmits, repeat until idle" loop is correct:
  ///
  /// - Every queued [`StreamAction::Connect`] surfaces before any queued
  ///   [`StreamAction::Shutdown`] / [`StreamAction::Close`] /
  ///   [`StreamAction::Abort`], so a fresh dial's connection opens before a
  ///   same-tick teardown targets an existing bridge's connection.
  /// - A `Shutdown` / `Close` / `Abort` for an exchange is withheld while
  ///   [`Self::poll_transport_transmit`] still holds bytes tagged with that
  ///   exchange's [`ExchangeId`]. Applying the teardown before its last bytes
  ///   are written would orphan them — a transport `shutdown(write)` closes the
  ///   send half and subsequent writes fail — so the driver MUST drain the
  ///   transmit queue first. The gate makes the natural drain loop correct
  ///   without burdening the driver with an explicit phase contract. (A failed
  ///   reap purges the exchange's queued bytes before enqueuing its `Abort`, so
  ///   the gate releases the `Abort` immediately — the bytes the driver must
  ///   discard are the ones it already buffered locally on earlier ticks.)
  pub fn poll_action(&mut self) -> Option<StreamAction> {
    if let Some(connect) = self.pending_connects.pop_front() {
      return Some(connect);
    }
    // Find the first teardown whose exchange has no pending transmit bytes;
    // skip past (but retain in producer order) any whose bytes are still
    // queued. The retained teardowns will surface once the driver drains the
    // matching bytes via `poll_transport_transmit`.
    let idx = self
      .pending_teardowns
      .iter()
      .position(|action| !self.exchange_has_pending_bytes(teardown_exchange(action)))?;
    self.pending_teardowns.remove(idx)
  }

  /// `true` iff [`Self::out_transmit`] holds at least one chunk tagged with
  /// the given exchange handle. Used by [`Self::poll_action`] to withhold a
  /// teardown for an exchange whose last bytes have not yet been drained.
  pub(crate) fn exchange_has_pending_bytes(&self, id: ExchangeId) -> bool {
    self.out_transmit.iter().any(|(eid, _, _)| *eid == id)
  }

  /// Drop any [`Self::out_transmit`] chunks tagged with `exchange`. Called
  /// from the reap path BEFORE [`Self::collect_bridge_transmits`] so a
  /// Failed bridge does not leak stale bytes through
  /// [`Self::poll_transport_transmit`] after the per-exchange teardown gate
  /// releases its `Close`. A bridge can sit on queued `out_transmit` bytes
  /// (its label prefix, a request, …) from an earlier tick when its
  /// deadline elapses; without the purge a driver doing the natural
  /// "drain actions, drain transmits, repeat until idle" loop would write
  /// those stale bytes to the peer's socket between the gate's release and
  /// the `Close` — delivering membership state from an exchange the local
  /// node has already failed. The dropped bytes are safe to discard because
  /// the bridge is being torn down: any further send-half progress is
  /// forbidden by the `Failed` phase, and the bridge's remaining outbound
  /// buffer is dropped with the bridge itself.
  ///
  /// Clean (`BothClosed`) reaps have an empty pre-reap queue for the
  /// exchange — a server's response is encoded inside
  /// [`StreamBridge::drain_then_reap`]'s `SendPushPullResponse` branch and
  /// collected by [`Self::collect_bridge_transmits`] AFTER this purge runs,
  /// so the response chunk is preserved while pre-failure stragglers are
  /// dropped.
  fn purge_transmit_for(&mut self, exchange: ExchangeId) {
    self.out_transmit.retain(|(eid, _, _)| *eid != exchange);
  }

  /// Fully cancel a not-yet-opened exchange: drop its bridge, metadata, and any
  /// queued outbound bytes. Used by `leave()` for an unopened outbound exchange
  /// (its `Connect` never drained) and by the post-leave inbound mint guard for
  /// an un-minted acceptor bridge — so a cancelled exchange leaves no live
  /// bridge (no stream deadline, no record-layer buffer) and emits no bytes.
  fn cancel_exchange(&mut self, id: ExchangeId) {
    // Ignoring the removed bridge: a never-opened exchange owes the peer no
    // clean-close bytes, and dropping it clears its record-layer state.
    let _ = self.conns.remove(id);
    self.exchanges.remove(&id);
    self.purge_transmit_for(id);
    // Replace any teardown already queued for this id with an Abort, so the
    // driver tears down a transport connection it may have already opened (the
    // exchange's Connect could have been drained before leave). The driver
    // no-ops the Abort if it never dialed; without it the connection / pool
    // slot leaks and could keep post-leave reliable I/O alive at the driver.
    self
      .pending_teardowns
      .retain(|a| teardown_exchange(a) != id);
    self
      .pending_teardowns
      .push_back(StreamAction::Abort(ExchangeRef::new(id)));
  }

  /// Drop any pending [`StreamAction::Connect`] still queued for `exchange`.
  /// Symmetric to [`Self::purge_transmit_for`], but for the action queue
  /// instead of the transmit queue.
  ///
  /// Called from the dial-failure reap path
  /// ([`Self::service_handshake_completions`]'s `dial_succeeded(None)` branch)
  /// so a driver does not observe a `Connect` for an exchange the coordinator
  /// has already failed. Without this purge, a driver doing the natural
  /// "drain actions, drain transmits, repeat" loop would dequeue the queued
  /// `Connect` (Connects always surface before teardowns —
  /// see [`Self::poll_action`]'s ordering contract), open the transport socket,
  /// then drain the same exchange's `Close` and tear it down — wasted work
  /// at best, and a vector for label disclosure (the bridge's
  /// record-layer outbound buffer still holds the eager-queued local label
  /// until `bridge.fail_dial_retired()` clears it) if any path bypassed the
  /// clear.
  ///
  /// `pending_connects` only ever holds `StreamAction::Connect` (its discipline
  /// is enforced by the `debug_assert!` at each `push_back` site); the
  /// catch-all arm is defensive — variants other than `Connect` retain.
  fn purge_pending_connect_for(&mut self, exchange: ExchangeId) {
    self.pending_connects.retain(|action| match action {
      StreamAction::Connect(info) => info.id() != exchange,
      _ => true,
    });
  }

  /// Next raw inbound gossip datagram, if any. The codec-owning layer drains
  /// this, decodes each `Message`, and feeds it back through
  /// [`Self::handle_packet`].
  pub fn poll_memberlist_ingress(&mut self) -> Option<(A, Bytes)> {
    self.mem_ingress.pop_front()
  }
}

// The full SWIM bag on `I`. Methods that delegate to `Endpoint`'s full-bag
// surface (`poll_event`, `poll_transmit`, `poll_timeout`, `handle_packet`,
// `handle_alive`, `handle_suspect`, `requeue_event`, `start_probe`,
// `leave`), drive `StreamConns` / `StreamBridge` ops (whose impls require
// the full bag), or run the internal bridge-pump / mint / reap helpers.
impl<I, A, R> StreamEndpoint<I, A, R>
where
  I: crate::Id
    + crate::Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: crate::Data
    + crate::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  /// Build the coordinator from a membership [`Endpoint`], the record
  /// layer's options bundle (`R::Options`), a per-peer SNI provider, and a
  /// per-peer `SocketAddr` resolver.
  ///
  /// The `sni_provider` closure maps a peer membership address to the TLS
  /// server-name the record layer should verify against. Plain-TCP drivers
  /// supply `Box::new(|_| None)`; TLS drivers supply a closure that returns
  /// the expected server name (e.g. `Box::new(|_| Some("localhost".to_string()))`
  /// for localhost-SAN test certs).
  ///
  /// The `peer_to_socket` closure resolves a peer membership address to the
  /// transport `SocketAddr` the driver writes bytes on. Drivers where
  /// `A = SocketAddr` pass `Box::new(|addr: &SocketAddr| *addr)`; drivers
  /// with a non-`SocketAddr` membership address supply a deployment-aware
  /// resolver.
  pub fn new(
    ep: Endpoint<I, A>,
    cfg: R::Options,
    sni_provider: Box<dyn Fn(&A) -> Option<String> + Send + Sync>,
    peer_to_socket: Box<dyn Fn(&A) -> SocketAddr + Send + Sync>,
  ) -> Self {
    Self {
      ep,
      cfg,
      sni_provider,
      peer_to_socket,
      compression: crate::CompressionOptions::new(),
      encryption: crate::EncryptionOptions::new(),
      checksum: crate::ChecksumOptions::new(),
      conns: StreamConns::new(),
      exchanges: FxHashMap::default(),
      out_transmit: std::collections::VecDeque::new(),
      pending_connects: std::collections::VecDeque::new(),
      pending_teardowns: std::collections::VecDeque::new(),
      mem_ingress: std::collections::VecDeque::new(),
      dial_pending: std::collections::VecDeque::new(),
      pending_outbound_kinds: FxHashMap::default(),
      last_now: None,
      policy_reap_pending: false,
    }
  }

  /// Number of active reliable-exchange bridges (one per in-flight push/pull,
  /// reliable-ping, or user-message exchange, plus any still-handshaking
  /// dial/accept). Observation-only, for a driver/test to assert no bridge
  /// leaked after an exchange completed or its connection dropped.
  pub fn live_bridge_count(&self) -> usize {
    self.conns.len()
  }

  /// Number of entries currently staged in `pending_outbound_kinds` (the
  /// `StreamId → ExchangeKind` map populated by the `start_*` wrappers and
  /// drained either at `ExchangeMeta` allocation or on the pre-`ExchangeMeta`
  /// failure paths inside `service_dials`). Test-only, used to prove the
  /// failure paths do not leak entries.
  #[cfg(all(test, feature = "tls"))]
  pub(crate) fn pending_outbound_kinds_len(&self) -> usize {
    self.pending_outbound_kinds.len()
  }

  /// Build the coordinator with an explicit cross-transport compression
  /// configuration. [`Self::new`] is `with_compression` with compression
  /// disabled.
  #[must_use]
  pub fn with_compression(
    ep: Endpoint<I, A>,
    cfg: R::Options,
    sni_provider: Box<dyn Fn(&A) -> Option<String> + Send + Sync>,
    peer_to_socket: Box<dyn Fn(&A) -> SocketAddr + Send + Sync>,
    compression: crate::CompressionOptions,
  ) -> Self {
    let mut this = Self::new(ep, cfg, sni_provider, peer_to_socket);
    this.compression = compression;
    this
  }

  /// Build the coordinator with an explicit cross-transport encryption
  /// configuration. [`Self::new`] is `with_encryption` with encryption
  /// disabled.
  ///
  /// Routes through [`Self::set_encryption_options`] so the bridge-fan-out
  /// runs in both the builder and the in-place setter — if a caller opens an
  /// exchange under a default-disabled coordinator and then rebuilds via
  /// `coord = coord.with_encryption(opts)`, the live bridges receive the new
  /// policy too.
  #[must_use]
  pub fn with_encryption(mut self, encryption: crate::EncryptionOptions) -> Self {
    self.set_encryption_options(encryption);
    self
  }

  /// Replace the encryption options in place. The driver calls this on a key
  /// rotation: build a new `EncryptionOptions` with the rotated `Keyring`,
  /// then publish it via the setter. Single-threaded `&mut self` — no lock.
  ///
  /// Propagates the new options to every live bridge so an exchange opened
  /// under the prior policy cannot keep feeding traffic under the old
  /// encryption rules — without this fan-out, a peer holding a pre-update
  /// reliable exchange would still see plaintext accepted on that bridge
  /// because the bridge holds a clone of the prior `EncryptionOptions`.
  ///
  /// For an INSECURE transport (`R::is_secure() == false`, e.g. plain TCP)
  /// the per-bridge [`StreamBridge::set_encryption`] fails the bridge.
  /// Four coordinator-side cleanups close the resulting plaintext-leak
  /// and stale-action gaps:
  ///
  /// (1) Purge every [`Self::out_transmit`] chunk tagged with each
  ///     newly-failed bridge's exchange. A live bridge may already have
  ///     encoded bytes under the prior policy in the record layer's
  ///     outbound buffer (cleared by the failure transition) AND drained
  ///     those bytes into [`Self::out_transmit`] (NOT cleared by the
  ///     failure transition — `out_transmit` is coordinator state, not
  ///     bridge state). Emitting those chunks on the wire after publishing
  ///     the new policy would leak plaintext post-enablement.
  ///
  /// (2) Purge any pending [`StreamAction::Connect`] for each
  ///     newly-failed bridge's exchange. A `start_push_pull` queues a
  ///     `Connect` that the driver may not yet have drained; letting it
  ///     surface from a subsequent [`Self::poll_action`] would have the
  ///     driver open a transport socket for a bridge the coordinator has
  ///     already failed.
  ///
  /// (3) Synchronously enqueue a [`StreamAction::Abort`] for each
  ///     newly-failed bridge so the driver's next [`Self::poll_action`]
  ///     returns the teardown directly. A terminal bridge returns no
  ///     per-bridge timeout and an idle endpoint may have no scheduler
  ///     timeout at all, so a teardown that only fires from the natural
  ///     `pump_bridges` reap on the next [`Self::handle_timeout`] is not
  ///     reachable from the documented driver interface without an
  ///     external scheduler kick. `Abort` (not `Close`) because the bridge
  ///     FAILED: its queued bytes are stale and the driver must discard them.
  ///
  /// (4) Drop the entire [`Self::out_transmit`] queue. The per-exchange
  ///     purge in (1) only reaches chunks belonging to bridges still in
  ///     [`Self::conns`]; a bridge that completed its exchange cleanly
  ///     and was already reaped can have left bytes here that step (1)
  ///     cannot iterate. On an insecure transport those orphans are
  ///     plaintext encoded under the prior policy, so a driver doing the
  ///     natural drain loop would emit them on the wire after the new
  ///     policy publishes. The unconditional drop covers the orphan
  ///     case without needing an exchange-id-keyed metadata trail
  ///     surviving past reap.
  ///
  /// The bridge remains in [`Self::conns`] until the next
  /// `pump_bridges` reaps it via the existing `reap_bridge` flow,
  /// which drains the bridge's stream events into the inner endpoint
  /// (the `StreamErrored` lifecycle notice the SWIM FSM consumes to
  /// retry the affected exchange under a fresh bridge constructed
  /// under the new policy) and emits its OWN `Abort` (the reap is
  /// failed). The driver may therefore observe two `Abort` actions for
  /// the same exchange across the policy-change and the subsequent reap
  /// — the second is a no-op for the documented driver contract (the
  /// socket was torn down on the first `Abort` and the driver-side
  /// mapping no longer recognises the exchange).
  ///
  /// For a SECURE transport (`R::is_secure() == true`, e.g. TLS) the
  /// per-bridge setter's `is_secure()` branch force-disables the bridge's
  /// `EncryptionOptions` regardless of the new options (the reliable path
  /// is already protected by the transport) and does NOT fail the bridge —
  /// the on-wire bytes are TLS records, so there is no plaintext-leak
  /// path to close. Step (4)'s `out_transmit` drop is therefore skipped on
  /// `R::is_secure() == true` so legitimate TLS records-layer bytes for
  /// live exchanges survive the policy change unchanged.
  ///
  /// **Gossip ingress purge** — [`Self::mem_ingress`] is drained
  /// unconditionally on every effective policy change (both insecure and
  /// secure transports). [`Self::handle_gossip`] buffers raw datagrams; the
  /// codec-owning driver decrypts them at drain time via
  /// [`Self::decrypt_gossip`], which reads the coordinator's CURRENT
  /// `self.encryption`. Without this drain a datagram queued under one
  /// policy would be decrypted under the policy in effect at drain time —
  /// a plaintext datagram queued while strict-mode was ON would be
  /// accepted after the operator switched to disabled, and a ciphertext
  /// datagram queued while disabled would be rejected after enabling.
  /// Gossip is lossy and self-healing, so the dropped datagrams recover
  /// on the next gossip round. The drain runs even on TLS coordinators
  /// because gossip rides plain UDP regardless of the reliable
  /// transport's `is_secure()` rating.
  ///
  /// **No-op reapply** — a config reconciler republishing the same effective
  /// policy short-circuits at entry and skips the bridge-fan-out entirely.
  /// Without that guard, every live insecure-transport reapply would tear
  /// down every reliable exchange for no security gain: the bridge clone is
  /// already running under these exact options, so re-failing it is pure
  /// availability loss. The check relies on the `PartialEq` derive added
  /// alongside this guard to `EncryptionOptions` (and transitively to
  /// `Keyring`).
  ///
  /// **Immediate-reap wake** — a real policy change that fails any bridge
  /// sets the [`Self::policy_reap_pending`] latch so the next
  /// [`Self::poll_timeout`] returns `last_now` as an immediate wake. The
  /// driver then runs [`Self::handle_timeout`] → `pump_bridges`, which
  /// reaps every terminal bridge in the same tick and clears the latch.
  /// A terminal bridge contributes no per-bridge timeout, so without this
  /// wake an idle endpoint with no other scheduled timer would leave the
  /// failed bridges sitting in [`Self::conns`] until some unrelated event
  /// next triggered a tick.
  pub fn set_encryption_options(&mut self, encryption: crate::EncryptionOptions) {
    if self.encryption == encryption {
      // Defensive reassignment: the equality check is structural, so a
      // reapply of the same logical policy with a distinct allocation
      // ends up holding the new clone (cheaper on subsequent comparisons
      // if the operator hands the coordinator a long-lived value).
      self.encryption = encryption;
      return;
    }
    let mut newly_failed: Vec<ExchangeId> = Vec::new();
    for id in self.conns.ids() {
      let Some(bridge) = self.conns.get_mut(id) else {
        continue;
      };
      let was_failed = bridge.is_failed();
      bridge.set_encryption(encryption.clone());
      if !was_failed && bridge.is_failed() {
        newly_failed.push(id);
      }
    }
    let any_failed = !newly_failed.is_empty();
    for id in newly_failed {
      self.purge_transmit_for(id);
      self.purge_pending_connect_for(id);
      // The bridge is in a FAILED terminal phase (the policy change rejected
      // it): its queued bytes are stale plaintext encoded under the prior
      // policy. Abort so the driver discards them rather than flushing them on
      // the wire after the new policy publishes.
      let action = StreamAction::Abort(ExchangeRef::new(id));
      debug_assert!(
        matches!(
          action,
          StreamAction::Shutdown(_) | StreamAction::Close(_) | StreamAction::Abort(_)
        ),
        "pending_teardowns holds only Shutdown / Close / Abort actions",
      );
      self.pending_teardowns.push_back(action);
    }
    if any_failed {
      self.policy_reap_pending = true;
    }
    if !R::is_secure() {
      // Drop every remaining [`Self::out_transmit`] chunk after the
      // per-bridge purges above. The per-bridge purge keyed by `ExchangeId`
      // only reaches chunks belonging to bridges still in [`Self::conns`];
      // a bridge that completed its exchange cleanly and was already reaped
      // (removed from `conns`) can have left bytes here that the per-bridge
      // loop above cannot iterate. On an insecure transport
      // (`R::is_secure() == false`) those orphaned chunks are plaintext
      // encoded under the prior policy; emitting them through
      // [`Self::poll_transport_transmit`] after the new policy publishes
      // would leak plaintext post-enablement. The clear runs on every
      // effective policy change on `!R::is_secure()`, not just `any_failed`:
      // a clean-reaped-bridge orphan can sit in `out_transmit` even when no
      // currently-live bridge fails (the reaped bridge is gone from
      // `conns`, so the per-bridge loop produces an empty `newly_failed`).
      // Scoping the clear to `!R::is_secure()` leaves TLS records-layer
      // bytes intact — a secure-transport coordinator's `out_transmit`
      // carries TLS records that are confidential by construction, so the
      // post-policy-change drain on TLS has no plaintext-leak path to
      // close. The dropped bytes are safe to discard: every live bridge
      // has just been failed (its retry rebuilds the exchange under the
      // new policy), and a clean-reaped bridge has no follow-up exchange
      // tied to those bytes.
      self.out_transmit.clear();
    }
    // Drop every buffered raw gossip datagram regardless of `any_failed`.
    // `handle_gossip` enqueues `(src, raw_bytes)` into [`Self::mem_ingress`];
    // [`Self::decrypt_gossip`] reads the coordinator's CURRENT
    // `self.encryption` at drain time. Without this clear, a datagram queued
    // before the policy change is decrypted under the NEW policy — a
    // plaintext datagram queued while strict-mode was ON would be accepted
    // after the operator switches to disabled, and a ciphertext datagram
    // queued while disabled would be rejected after enabling. Gossip is
    // lossy and self-healing, so the dropped bytes recover on the next
    // gossip round. The clear runs on every effective policy change, not
    // just `any_failed`: a secure-transport (TLS) coordinator still uses
    // `self.encryption` for the plain-UDP gossip path even though its
    // reliable bridges do not fail.
    self.mem_ingress.clear();
    self.encryption = encryption;
  }

  /// Replace the compression options in place. The driver calls this
  /// when the operator updates the gossip compression policy at
  /// runtime. Single-threaded `&mut self` — no lock.
  ///
  /// Fans out to every live bridge so an in-flight exchange that
  /// started under the prior policy adopts the new policy on its next
  /// outbound encode (each [`StreamBridge`] carries a per-bridge
  /// `compression` clone captured at construction; without the
  /// fan-out, a long-lived reliable exchange would emit the old
  /// policy's bytes until it reaped).
  ///
  /// Unlike [`Self::set_encryption_options`], compression is
  /// non-security: the wire frame self-describes its algorithm via
  /// the compression-tag prefix, so a peer always decompresses under
  /// whatever policy was active at the producer's encode time. No
  /// bridge-failure cascade, no `out_transmit` / `mem_ingress` purge,
  /// no `policy_reap_pending` wake.
  pub fn set_compression_options(&mut self, compression: crate::CompressionOptions) {
    self.compression = compression;
    for id in self.conns.ids() {
      let Some(bridge) = self.conns.get_mut(id) else {
        continue;
      };
      bridge.set_compression(compression);
    }
  }

  /// Replace the gossip-plane checksum options in place. The driver calls this
  /// when the operator updates the checksum policy at runtime. Single-threaded
  /// `&mut self` — no lock.
  ///
  /// Checksum is a gossip-plane (unreliable) concern, so this updates only the
  /// coordinator's own field — read by [`Self::checksum_gossip`] on the next
  /// outbound datagram. Reliable stream bridges carry no checksum (they rely on
  /// the stream transport's own integrity), so there is no per-bridge fan-out.
  /// The new policy takes effect on the next datagram; checksum is non-security
  /// and the wire frame self-describes its algorithm via the checksum-tag
  /// prefix, so a peer always verifies under whatever policy produced the bytes.
  pub fn set_checksum_options(
    &mut self,
    checksum: crate::ChecksumOptions,
  ) -> Result<(), crate::ChecksumError> {
    // Validate the algorithm's backend is built into this binary BEFORE storing
    // it: an unusable policy is accepted by the options builder, but every
    // subsequent `checksum_gossip` would then fail and the codec-owning driver
    // would drop the datagram — silently disabling all gossip behind a false
    // success. The empty-payload probe surfaces `Disabled` / `UnknownAlgorithm`.
    checksum.apply(&[])?;
    self.checksum = checksum;
    Ok(())
  }

  /// Arm the periodic probe / gossip / push-pull schedulers. Forwards to
  /// [`Endpoint::start_scheduling`].
  #[inline]
  pub fn start_scheduling(&mut self, now: Instant) {
    self.ep.start_scheduling(now);
  }

  /// Returns `true` if the endpoint is in normal operation (not leaving
  /// or left). Forwards to [`Endpoint::is_running`]. A driver consults
  /// this before calling [`Self::leave`] to distinguish a leave that
  /// actually initiates the dead-self flush (and will emit
  /// [`Event::LeftCluster`]) from an
  /// idempotent post-leave no-op (which will not).
  #[inline]
  pub fn is_running(&self) -> bool {
    self.ep.is_running()
  }

  /// Re-broadcast the local node's metadata. Pass-through to
  /// [`Endpoint::update_meta`]; the inner endpoint bumps the local
  /// incarnation and queues an `Alive` broadcast carrying the new bytes
  /// so peers converge to the updated metadata via the normal SWIM path.
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::NotRunning`] if the local lifecycle has
  /// already transitioned to `Leaving` / `Left` / `Shutdown` — a post-leave
  /// metadata update would bump the incarnation and resurrect the local
  /// node on peers that had observed its `Leave`.
  pub fn update_meta(&mut self, meta: crate::typed::Meta) -> Result<(), crate::error::Error> {
    self.ep.update_meta(meta)
  }

  /// Queue an application user-broadcast for gossip dissemination. Forwards
  /// to the inner membership [`Endpoint`].
  #[inline]
  pub fn queue_user_broadcast(&mut self, data: Bytes) -> Result<(), crate::error::Error> {
    self.ep.queue_user_broadcast(data)
  }

  /// Initiate an application ping to `node`. Forwards to
  /// [`Endpoint::ping`]; sets `last_now`. Returns the [`crate::event::PingId`]
  /// correlation token; the terminal event (`PingCompleted` / `PingFailed`)
  /// carries it so the driver can correlate the outcome.
  #[inline]
  pub fn ping(
    &mut self,
    node: crate::Node<I, A>,
    now: Instant,
  ) -> Result<crate::event::PingId, crate::error::Error> {
    self.last_now = Some(now);
    self.ep.ping(node, now)
  }

  /// Enqueue a directed unreliable user-data packet. Forwards to
  /// [`Endpoint::send_user_packet`].
  #[inline]
  pub fn send_user_packet(&mut self, to: A, data: Bytes) -> Result<(), crate::error::Error> {
    self.ep.send_user_packet(to, data)
  }

  /// Enqueue directed unreliable user-data packets (compound if ≥2).
  /// Forwards to [`Endpoint::send_user_packets`].
  #[inline]
  pub fn send_user_packets(
    &mut self,
    to: A,
    payloads: &[Bytes],
  ) -> Result<(), crate::error::Error> {
    self.ep.send_user_packets(to, payloads)
  }

  /// The reliable-stream frame ceiling
  /// ([`max_stream_frame_size`](crate::config::EndpointOptions::max_stream_frame_size)).
  /// The driver derives its observation-channel payload byte budget from this.
  #[inline]
  pub fn max_stream_frame_size(&self) -> usize {
    self.ep.max_stream_frame_size()
  }

  /// Set the application push-pull local-state snapshot. Forwards to the
  /// inner [`Endpoint`].
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::LocalStateExceedsFrame`] if the snapshot's
  /// framed PushPull would exceed the reliable-stream frame budget — such a
  /// snapshot is deterministically untransmittable, so it is rejected rather
  /// than stored.
  #[inline]
  pub fn set_local_state_snapshot(&mut self, bytes: Bytes) -> Result<(), crate::error::Error> {
    self.ep.set_local_state_snapshot(bytes)
  }

  /// Set the application ack payload attached to probe acks. Forwards to the
  /// inner [`Endpoint`].
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::AckPayloadExceedsMtu`] if the framed Ack
  /// carrying `payload` would not fit the node's gossip packet budget — an
  /// over-budget Ack is deterministically unsendable on the gossip socket,
  /// so the payload is rejected rather than stored.
  #[inline]
  pub fn set_ack_payload(&mut self, payload: Bytes) -> Result<(), crate::error::Error> {
    self.ep.set_ack_payload(payload)
  }

  /// Install a custom peer-admission predicate. Forwards to
  /// [`Endpoint::set_alive_delegate`]; the machine consults it inline for every
  /// inbound Alive (gossip and join push/pull).
  #[inline]
  pub fn set_alive_delegate(&mut self, delegate: impl crate::delegate::AliveDelegate<I, A>) {
    self.ep.set_alive_delegate(delegate);
  }

  /// Install a custom join-merge predicate. Forwards to
  /// [`Endpoint::set_merge_delegate`]; the machine consults it on each join
  /// push/pull merge.
  #[inline]
  pub fn set_merge_delegate(&mut self, delegate: impl crate::delegate::MergeDelegate<I, A>) {
    self.ep.set_merge_delegate(delegate);
  }

  /// Initiate one SWIM probe tick on the inner membership endpoint.
  ///
  /// Pass-through to [`Endpoint::start_probe`]; sets `last_now`. The probe
  /// itself rides the unreliable UDP path; only if it fails does the reliable
  /// transport fallback kick in via the natural suspicion / failure-detection
  /// timing.
  pub fn start_probe(&mut self, now: Instant) -> bool {
    self.last_now = Some(now);
    self.ep.start_probe(now)
  }

  /// Seed an `Alive` state on the inner membership endpoint (bootstrap path).
  /// Pass-through to [`Endpoint::handle_alive`]; sets `last_now`.
  pub fn handle_alive(&mut self, from: A, alive: crate::typed::Alive<I, A>, at: Instant) {
    self.last_now = Some(at);
    self.ep.handle_alive(from, alive, at);
  }

  /// Inject a `Suspect` event on the inner membership endpoint (test-harness
  /// path). Pass-through to [`Endpoint::handle_suspect`]; sets `last_now`.
  pub fn handle_suspect(&mut self, from: A, suspect: crate::typed::Suspect<I>, at: Instant) {
    self.last_now = Some(at);
    self.ep.handle_suspect(from, suspect, at);
  }

  /// Re-queue an event for observation by a later [`Self::poll_event`].
  ///
  /// Anchors `last_now = Some(now)` unconditionally. `Event::DialRequested` is
  /// routed DIRECTLY into the private `dial_pending` deque (bypassing the inner
  /// queue) so a caller that calls [`Self::poll_timeout`] WITHOUT an
  /// intervening [`Self::poll_event`] sieve still sees the immediate-due rescue
  /// term; every other variant delegates to [`Endpoint::requeue_event`].
  pub fn requeue_event(&mut self, ev: Event<I, A>, now: Instant) {
    self.last_now = Some(now);
    match ev {
      Event::DialRequested(dial) => {
        // A leaving/left node re-admits no dial: dropping a requeued
        // DialRequested stops a held event from restarting a transport dial
        // (service_dials would otherwise build a bridge and surface a Connect).
        if !self.ep.is_running() {
          return;
        }
        let (id, peer, deadline) = dial.into_parts();
        self.dial_pending.push_back(PendingDial {
          id,
          peer,
          deadline,
          attempted: false,
        });
      }
      other => self.ep.requeue_event(other),
    }
  }

  /// Begin a graceful leave; delegates to the membership endpoint after making
  /// the coordinator a cancellation point.
  ///
  /// A leaving node starts no new reliable I/O. Every outbound exchange whose
  /// request bytes are not yet on the wire is cancelled here — unattempted dials,
  /// queued `Connect` directives, and any outbound exchange still holding request
  /// bytes in `out_transmit` whether or not its `Connect` was already drained —
  /// so a pre-leave push/pull or user message cannot hand its (Alive-advertising)
  /// request to the transport during the graceful drain. Buffered inbound gossip
  /// is dropped. Outbound exchanges past the request boundary (request already on
  /// the wire), and their teardown directives, are preserved so in-flight streams
  /// still close cleanly.
  pub fn leave(&mut self, now: Instant) -> Result<(), crate::error::Error> {
    self.last_now = Some(now);
    self.dial_pending.clear();
    self.pending_outbound_kinds.clear();
    self.pending_connects.clear();
    // Drop inbound gossip buffered before leave — handle_packet would drop it
    // anyway once not Running, so it must not linger or later decode.
    self.mem_ingress.clear();
    // Cancel every OUTBOUND exchange (kind is Some) whose request bytes are still
    // queued in out_transmit: its `Connect` may already be drained, but the
    // request is not yet on the wire, so writing it after leave would advertise
    // our pre-leave Alive. Inbound exchanges and request-sent outbound exchanges
    // are left to drain. A `Vec` keeps this no_std-clean (the set is small).
    let unsent_outbound: Vec<ExchangeId> = self
      .exchanges
      .iter()
      .filter(|(_, meta)| meta.outbound)
      .map(|(eid, _)| *eid)
      .collect();
    for eid in unsent_outbound {
      if self.exchange_has_pending_bytes(eid) {
        self.cancel_exchange(eid);
      }
    }
    self.ep.leave(now)
  }

  /// Next membership/lifecycle event for the driver, if any.
  ///
  /// `Event::DialRequested` is sieved out of the inner endpoint's queue into
  /// the private [`dial_pending`](Self::dial_pending) deque and is NEVER
  /// returned to external callers: the coordinator IS the driver and dials
  /// itself (see [`Self::service_dials`]). External callers only observe
  /// application-visible events.
  pub fn poll_event(&mut self) -> Option<Event<I, A>> {
    loop {
      match self.ep.poll_event()? {
        Event::DialRequested(dial) => {
          let (id, peer, deadline) = dial.into_parts();
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
          continue;
        }
        other => return Some(other),
      }
    }
  }

  /// Unified next-deadline = `min` over the membership endpoint, every bridge
  /// (handshake or exchange deadline), AND every pending-dial intent's own
  /// deadline. Returns an immediate-due wake (an `Instant` already `<= caller's
  /// now`) whenever `dial_pending` holds an entry `service_dials` has not yet
  /// attempted.
  ///
  /// The pending-dial deadline term is correctness, not optimisation: a
  /// fully-stalled `dial_pending` queue must still be serviced no later than
  /// its intent's `deadline` so the `dial_failed` does not slip past the
  /// user-visible exchange timeout on a quiet cluster. The immediate-due term
  /// is defence-in-depth for callers that bypass the high-level `start_*`
  /// wrappers (which dial in-band) and queue a `DialRequested` directly. There
  /// are NO connection-pool terms — connection-per-exchange means the bridge's
  /// own `poll_timeout` already covers every per-connection timer.
  ///
  /// `last_now` is `None` only before the very first `handle_*` / `start_*`
  /// call: in that window the immediate-due wake degrades to the intent's
  /// `deadline` term.
  ///
  /// A pending [`Self::set_encryption_options`] policy reap (one or more
  /// bridges failed by a runtime policy change) folds `last_now` into the
  /// returned `min` so the driver wakes immediately and reaps the failed
  /// bridges in the next [`Self::handle_timeout`] tick — a terminal bridge
  /// contributes no per-bridge timeout of its own, and an idle endpoint
  /// may have no scheduler timeout at all. The latch can only be set
  /// after a bridge exists in `conns`, which requires a `start_*` /
  /// `handle_*` call to have already anchored `last_now`, so the wake is
  /// always reachable.
  pub fn poll_timeout(&mut self) -> Option<Instant> {
    let mut best = self.ep.poll_timeout();
    for id in self.conns.ids() {
      if let Some(b) = self.conns.get_mut(id) {
        if let Some(t) = b.poll_timeout() {
          best = Some(best.map_or(t, |b| b.min(t)));
        }
      }
    }
    let mut has_unattempted = false;
    for entry in &self.dial_pending {
      let t = entry.deadline;
      best = Some(best.map_or(t, |b| b.min(t)));
      if !entry.attempted {
        has_unattempted = true;
      }
    }
    if has_unattempted {
      if let Some(anchor) = self.last_now {
        best = Some(best.map_or(anchor, |b| b.min(anchor)));
      }
    }
    if self.policy_reap_pending {
      if let Some(anchor) = self.last_now {
        best = Some(best.map_or(anchor, |b| b.min(anchor)));
      }
    }
    best
  }

  /// Next typed unreliable memberlist [`Transmit`] for the driver to encode
  /// onto the unreliable (UDP) path, if any.
  ///
  /// Each call drains ONE `Transmit` straight out of the inner
  /// `Endpoint::poll_transmit`; nothing is prebuffered coordinator-internally,
  /// so the inner pop — which decrements `Endpoint`'s leave-completion counter
  /// and emits `Event::LeftCluster` after the last dead-self notice — happens
  /// at the SAME moment the datagram crosses to the external driver. A caller
  /// that `leave(now)`s, ticks, and then reads `poll_event` cannot observe
  /// `LeftCluster` until it has drained the dead-self tail through this
  /// accessor: tearing the socket down on `LeftCluster` therefore guarantees
  /// every dead-self broadcast has been handed to the driver, so peers see
  /// `Dead`/`Left` rather than wrongly Suspecting.
  pub fn poll_memberlist_transmit(&mut self) -> Option<Transmit<I, A>> {
    self.ep.poll_transmit()
  }

  /// Feed one decoded unreliable memberlist
  /// [`Message`](crate::typed::Message) into the inner membership
  /// endpoint. Pass-through to [`Endpoint::handle_packet`]; the composed unit's
  /// unreliable ingress is `handle_gossip` → `poll_memberlist_ingress` → (codec
  /// decode) → `handle_packet`, never a direct call into the inner `Endpoint`.
  pub fn handle_packet(&mut self, from: A, msg: crate::typed::Message<I, A>, now: Instant) {
    self.ep.handle_packet(from, msg, now);
  }

  /// Mutable borrow of the inner membership endpoint, for tests that must
  /// drive a scenario the public `start_*` wrappers cannot reach — e.g.
  /// invoking `Endpoint::start_reliable_ping` WITHOUT the in-band
  /// `service_dials` + `flush_outbound` the coordinator wrapper runs, or
  /// retiring a dial intent directly with `Endpoint::dial_failed`.
  #[cfg(all(test, feature = "tcp"))]
  pub(crate) fn endpoint_mut(&mut self) -> &mut Endpoint<I, A> {
    &mut self.ep
  }

  /// Snapshot of every live exchange handle, for tests that probe which
  /// bridges the coordinator currently holds.
  #[cfg(all(test, feature = "tcp"))]
  pub(crate) fn exchange_ids(&self) -> Vec<ExchangeId> {
    self.conns.ids()
  }

  /// The reliable-unit ceiling a given exchange's bridge was built with, for a
  /// test asserting the ceiling tracks `EndpointOptions::max_stream_frame_size`
  /// rather than a hard-coded constant.
  #[cfg(all(test, feature = "tcp"))]
  pub(crate) fn bridge_reliable_max(&mut self, id: ExchangeId) -> Option<usize> {
    self.conns.get_mut(id).map(|b| b.reliable_max())
  }

  /// Whether the given exchange's live bridge currently considers encryption
  /// enabled. Used by the runtime-propagation regression test to assert that
  /// [`Self::set_encryption_options`] fanned the new options out to every
  /// in-flight bridge (rather than just `self.encryption`, which would leave
  /// a peer's pre-update reliable exchange accepting plaintext on the bridge).
  #[cfg(all(test, feature = "tcp", feature = "encryption-aes-gcm"))]
  pub(crate) fn bridge_encryption_enabled(&mut self, id: ExchangeId) -> Option<bool> {
    self
      .conns
      .get_mut(id)
      .map(|b| b.encryption_for_test().is_enabled())
  }

  /// Whether the given exchange's live bridge is currently in
  /// [`bridge_phase::BridgePhase::Failed`]. Used by the
  /// encryption-policy-change regression test to assert that an insecure-transport
  /// bridge fails on a runtime [`Self::set_encryption_options`] update so the
  /// SWIM FSM retries the affected exchange under a fresh bridge constructed
  /// under the new policy.
  ///
  /// [`bridge_phase::BridgePhase::Failed`]: crate::bridge_phase::BridgePhase::Failed
  #[cfg(all(test, feature = "tcp", feature = "encryption-aes-gcm"))]
  pub(crate) fn bridge_is_failed(&mut self, id: ExchangeId) -> Option<bool> {
    self.conns.get_mut(id).map(|b| b.is_failed())
  }

  /// Whether the given exchange's live bridge is in
  /// [`bridge_phase::BridgePhase::Active`] and has NOT yet retired its send half
  /// ([`StreamBridge::fin_owed`] is `false`). Used by the TLS failed-reap
  /// regression to assert the pre-failure invariant — an Established bridge that
  /// has sent nothing — so the subsequent deadline failure runs
  /// `retire_halves`'s `send_close_notify` (queuing a `close_notify` into
  /// rustls's write buffer) rather than finding the send half already closed.
  ///
  /// [`bridge_phase::BridgePhase::Active`]: crate::bridge_phase::BridgePhase::Active
  /// [`StreamBridge::fin_owed`]: crate::streams::bridge::StreamBridge::fin_owed
  #[cfg(all(test, feature = "tls"))]
  pub(crate) fn bridge_is_established_pre_fin(&mut self, id: ExchangeId) -> Option<bool> {
    self.conns.get_mut(id).map(|b| {
      matches!(
        b.phase_ref(),
        crate::streams::phase::StreamPhase::Established(crate::bridge_phase::BridgePhase::Active)
      ) && !b.fin_owed()
    })
  }

  /// Append one [`StreamAction::Shutdown`] / [`StreamAction::Close`] /
  /// [`StreamAction::Abort`] to the teardown queue, for tests that exercise
  /// [`Self::poll_action`]'s Connect-before-teardown ordering by injecting a
  /// teardown at the same producer site `maybe_emit_shutdown` / `reap_bridge` use.
  #[cfg(all(test, feature = "tcp"))]
  pub(crate) fn push_teardown(&mut self, action: StreamAction) {
    debug_assert!(
      matches!(
        action,
        StreamAction::Shutdown(_) | StreamAction::Close(_) | StreamAction::Abort(_)
      ),
      "pending_teardowns holds only Shutdown / Close / Abort actions",
    );
    self.pending_teardowns.push_back(action);
  }

  /// Step (2) of the per-tick order: pump every bridge's outbound half, drain
  /// each non-terminal stream's endpoint-events into the `Endpoint`, and
  /// D1-drain-then-reap any bridge that turned terminal.
  ///
  /// Extracted so [`Self::flush_outbound`] can re-use the same bridge step
  /// after `service_dials`. There is no inbound `pump_in`: inbound bytes are
  /// fed through [`Self::handle_transport_data`] →
  /// `StreamBridge::handle_transport_data` directly; this step only drives the
  /// outbound side and the endpoint-event drain.
  fn pump_bridges(&mut self, now: Instant) {
    for id in self.conns.ids() {
      // Split borrow: take the bridge out, operate, put back (or reap).
      let Some(mut br) = self.conns.remove(id) else {
        continue;
      };
      // Replay any pre-promotion buffered plaintext FIRST (inbound), before
      // the outbound pump. A peer that coalesced its first request with its
      // label prefix had that request stripped of its label and buffered as
      // inbound plaintext while the bridge was still `Handshaking`; this is
      // the post-mint pump (run AFTER `service_handshake_completions`
      // promoted the bridge), so the buffered plaintext reassembles into the
      // just-minted `Stream` THIS tick rather than waiting for the next
      // transport read. A no-op on every bridge with no buffered tail (the
      // steady state).
      // Ignoring Err: a replay failure terminalizes the bridge; the
      // `is_terminal()` reap path below handles it. There is no separate
      // action.
      let _ = br.replay_pending(now);
      // `pump_out` sets the bridge `fatal` flag on a transport / FSM error, so
      // `is_terminal()` below drives the prompt reap; the `#[must_use]` Result
      // is consumed — terminality is the signal.
      // Ignoring Err: `pump_out` failing terminalizes the bridge; the
      // `is_terminal()` reap path below handles it. There is no separate
      // action.
      let _ = br.pump_out(now);
      if br.is_terminal() {
        br.drain_then_reap(&mut self.ep, now);
        self.reap_bridge(id, &mut br, now);
        drop(br);
      } else {
        // Drain endpoint-events EVERY tick (non-terminal payload-only path).
        br.drain_payload_only(&mut self.ep, now);
        // `drain_payload_only` may flip the bridge to terminal (a
        // `StreamCommand::Close` from an admission-rejected join sets `fatal`);
        // re-check so the bridge D1-drains and reaps in this SAME tick rather
        // than holding the connection until its exchange deadline.
        if br.is_terminal() {
          br.drain_then_reap(&mut self.ep, now);
          self.reap_bridge(id, &mut br, now);
          drop(br);
        } else {
          // Graceful half-close signal: the bridge retired its send half
          // (clean exchange) and is awaiting the peer's FIN, so tell the
          // driver to half-close the transport write side after it has
          // drained our buffered bytes. Emitted at most once per exchange.
          // The bridge's outbound bytes are collected by `finalize_tick`
          // while it is still alive — only a reaped (dropped) bridge needs
          // the inline reap-time collection in `reap_bridge`.
          self.maybe_emit_shutdown(id, &br);
          self.conns.insert(id, br);
        }
      }
    }
  }

  /// Reap a terminal bridge: forget the exchange and emit its teardown action.
  ///
  /// A clean (`BothClosed`) reap drains the bridge's final reply bytes (anything
  /// `drain_then_reap` just encoded) into the outbound queue and emits
  /// [`StreamAction::Close`], so the driver delivers them before its FIN. A
  /// failed reap discards the bridge's outbound bytes — both already-queued
  /// chunks and its current record-layer buffer — and emits
  /// [`StreamAction::Abort`], so the driver RSTs without putting stale bytes
  /// (including a TLS `close_notify`) on the wire.
  fn reap_bridge(&mut self, id: ExchangeId, br: &mut StreamBridge<I, A, R>, now: Instant) {
    // Discipline the bridge's outbound bytes by terminal outcome BEFORE emitting
    // the teardown action below.
    //
    // FAILED reap — discard everything; a failed exchange RSTs and must surface
    // no bytes:
    // (a) Purge already-queued `out_transmit` chunks tagged with this exchange —
    //     pre-failure label / request bytes from earlier ticks (a dialer that was
    //     waiting for the response when its exchange deadline elapsed never
    //     drained them). Without the purge the [`Self::poll_action`] gate would
    //     withhold the `Abort` behind those stale bytes and a driver doing the
    //     natural "drain actions, drain transmits, repeat" loop would emit them
    //     after the exchange already failed — leaking membership state.
    // (b) Do NOT collect the bridge's current record-layer buffer. For plain TCP
    //     `clear_outbound` already emptied it on the failure transition, so there
    //     is nothing to collect; but for TLS `clear_outbound` is a no-op and the
    //     failure path queued a `close_notify` into rustls's write buffer.
    //     Collecting would drain that alert (and any pending rustls ciphertext)
    //     into `out_transmit`, and the gate would then withhold the `Abort`
    //     behind it until the driver put the alert on the wire. Both are safe to
    //     drop: the failed phase forbids further send progress and the bridge is
    //     about to be dropped.
    //
    // CLEAN (`BothClosed`) reap — keep and collect. `out_transmit` may already
    // hold legitimate response chunks from an EARLIER pump. The acceptor's lazy
    // outbound label prefix fires the instant its inbound label validates; if the
    // dialer's `[label]` and `[request][FIN]` arrive in two separate transport
    // reads, the next `pump_bridges` drains the label prefix into `out_transmit`
    // BEFORE the request bytes arrive, and `collect_bridge_transmits` adds the
    // final response chunk as the second tagged chunk. Purging would drop the
    // label prefix and leave the peer rejecting an unlabeled response — so a
    // clean reap never purges and always collects, delivering both before its FIN.
    if br.is_failed() {
      self.purge_transmit_for(id);
    } else {
      self.collect_bridge_transmits(id, br);
    }
    // Snapshot the terminal outcome BEFORE the exchange-meta is removed
    // — drivers correlate the outcome with their own ExchangeId-keyed
    // waiters via the public ExchangeCompleted event below. Emission
    // fires for ALL outbound kinds; the kind is carried on the payload
    // so consumers can filter (sync-join consumes only PushPull
    // completions; reliable-ping and user-message completions are
    // observable for future monitoring). Inbound exchanges (kind =
    // None) do NOT emit — synchronous-join drivers observe ONLY their
    // own outbound exchange's terminal outcome.
    let outcome = if br.is_failed() {
      ExchangeOutcome::Failed
    } else {
      ExchangeOutcome::Succeeded
    };
    let removed = self.exchanges.remove(&id);
    if let Some(meta) = &removed
      && let Some(kind) = meta.kind
    {
      self
        .ep
        .emit_event(Event::ExchangeCompleted(ExchangeCompleted::new(
          id,
          crate::CheapClone::cheap_clone(&meta.peer),
          outcome,
          kind,
        )));
    }
    if let Some(PendingMint::Outbound(sid)) = removed.and_then(|m| m.mint) {
      // The bridge reaped before its label / handshake step settled (e.g. a
      // label mismatch on a dial): the `StreamId` was never minted into a
      // `Stream`, but the inner endpoint still holds the pending intent.
      // Retire it so the exchange does not strand (a reliable-ping fallback
      // is released, etc.). A bridge whose label / handshake step settled has
      // `mint == None` (taken at promotion), so a promoted bridge's clean reap
      // does not re-enter `dial_failed`.
      self.ep.dial_failed(
        sid,
        crate::error::StreamError::DialFailed("stream label exchange aborted".into()),
        now,
      );
    }
    // Route on the SAME terminal outcome computed above: a failed reap (dial
    // failure, label/encryption rejection, or an elapsed exchange deadline)
    // emits `Abort` so the driver discards the bridge's stale buffered bytes
    // rather than flushing them; a clean (`BothClosed`) reap emits `Close` so
    // the driver delivers the legitimate response/label bytes before its FIN.
    let action = if br.is_failed() {
      StreamAction::Abort(ExchangeRef::new(id))
    } else {
      StreamAction::Close(ExchangeRef::new(id))
    };
    debug_assert!(
      matches!(
        action,
        StreamAction::Shutdown(_) | StreamAction::Close(_) | StreamAction::Abort(_)
      ),
      "pending_teardowns holds only Shutdown / Close / Abort actions",
    );
    self.pending_teardowns.push_back(action);
  }

  /// Emit the one [`StreamAction::Shutdown`] for an exchange once its bridge
  /// has retired its send half (clean half-close while awaiting the peer's
  /// FIN). A terminal bridge is reaped with `Close` instead and never reaches
  /// here.
  ///
  /// Gated by the per-exchange `fin_emitted` latch. For a record layer with an
  /// in-band close the half-close is the `close_notify` alert (already encoded
  /// into the outbound queue by the record layer) and this action is the
  /// transport-level companion; for plain TCP the half-close is the out-of-band
  /// TCP FIN, so this action tells the driver to issue `shutdown(write)` on the
  /// connection after it has drained our buffered bytes.
  fn maybe_emit_shutdown(&mut self, id: ExchangeId, br: &StreamBridge<I, A, R>) {
    if !br.fin_owed() {
      return;
    }
    if let Some(meta) = self.exchanges.get_mut(&id) {
      if !meta.fin_emitted {
        meta.fin_emitted = true;
        let action = StreamAction::Shutdown(ExchangeRef::new(id));
        debug_assert!(
          matches!(
            action,
            StreamAction::Shutdown(_) | StreamAction::Close(_) | StreamAction::Abort(_)
          ),
          "pending_teardowns holds only Shutdown / Close / Abort actions",
        );
        self.pending_teardowns.push_back(action);
      }
    }
  }

  /// Drain one bridge's outbound bytes into [`Self::out_transmit`], tagged
  /// with its exchange handle + peer socket.
  fn collect_bridge_transmits(&mut self, id: ExchangeId, br: &mut StreamBridge<I, A, R>) {
    let Some(peer_socket) = self.exchanges.get(&id).map(|m| m.peer_socket) else {
      return;
    };
    let mut buf = Vec::new();
    br.poll_transport_transmit(&mut buf);
    if !buf.is_empty() {
      self
        .out_transmit
        .push_back((id, peer_socket, Bytes::from(buf)));
    }
  }

  /// Step (4): for every exchange still awaiting its `Stream`, mint it once
  /// the label / handshake step has settled.
  ///
  /// `is_handshaking()` is `false` once the record layer reports the label /
  /// handshake step done (the mint window) AND once the bridge is `Established`
  /// (already minted) — so the `meta.mint.is_some()` guard distinguishes the
  /// mint window from an already-promoted bridge. A bridge that FAILED during
  /// the label / handshake step is terminal (and not handshaking); it is
  /// skipped here and reaped by `pump_bridges`.
  fn service_handshake_completions(&mut self, now: Instant) {
    for id in self.conns.ids() {
      let needs_mint = matches!(
        self.exchanges.get(&id),
        Some(meta) if meta.mint.is_some()
      );
      if !needs_mint {
        continue;
      }
      let ready = match self.conns.get_mut(id) {
        Some(br) => !br.is_terminal() && !br.is_handshaking(),
        None => false,
      };
      if !ready {
        continue;
      }
      // Take the mint decision out of the meta (single-shot).
      let mint = self
        .exchanges
        .get_mut(&id)
        .and_then(|m| m.mint.take())
        .expect("needs_mint implies the mint decision is present");
      match mint {
        PendingMint::Outbound(stream_id) => match self.ep.dial_succeeded(stream_id, now) {
          Some(stream) => {
            if let Some(br) = self.conns.get_mut(id) {
              br.promote(stream);
            }
          }
          None => {
            // The dial intent was retired by the inner endpoint (deadline
            // elapsed in `dial_succeeded`'s `now >= intent.deadline` check, or
            // an external `dial_failed` cleared the intent) before a `Stream`
            // could be minted. The bridge is in dial-failed state.
            //
            // (1) Fail the bridge BEFORE removing it so the failure transition
            //     runs its `records.clear_outbound()` — without this a dialer
            //     record layer that eager-queues its local label
            //     (`[12][len][local_label]` at construction) leaves it in the
            //     record-layer outbound buffer and `collect_bridge_transmits`
            //     would drain it into `out_transmit`.
            //     `fail_dial_retired` is the entry point that triggers
            //     `records.clear_outbound`: a retired-but-unopened socket
            //     transitions without retiring halves (no FIN can be
            //     delivered on a socket that was never opened) while
            //     clearing the outbound buffer.
            if let Some(br) = self.conns.get_mut(id) {
              br.fail_dial_retired();
            }
            // (2) Purge any already-drained chunks tagged with this exchange
            //     from `out_transmit` (defensive — handles bytes an earlier
            //     pump may have drained before the dial intent was retired).
            self.purge_transmit_for(id);
            // (3) Purge any still-queued `Connect` for this exchange so a
            //     driver doing the natural drain loop does not open a
            //     transport socket for an exchange the coordinator has already
            //     failed. If the driver had already drained `Connect` from a
            //     prior tick this is a no-op; the `Close` enqueued below still
            //     fires so the driver can clean up the open socket.
            self.purge_pending_connect_for(id);
            // (4) Remove the bridge WITHOUT calling
            //     `collect_bridge_transmits`. The bridge is failed and being
            //     dropped; its record-layer outbound buffer has just been
            //     cleared by step (1)'s `fail_dial_retired`. Collecting from
            //     a failed bridge is the leak vector this guard closes — bytes
            //     from a failed bridge must never reach `out_transmit`.
            if let Some(br) = self.conns.remove(id) {
              drop(br);
            }
            self.exchanges.remove(&id);
            // (5) Emit `Abort` so a driver that had already drained
            //     `Connect` from an earlier tick can tear the open socket
            //     down and DISCARD any bytes it queued before the dial was
            //     retired — the exchange FAILED, so those bytes are stale. If
            //     `Connect` was still in `pending_connects`, step (3) dropped
            //     it and `Abort` is a no-op for that exchange.
            let action = StreamAction::Abort(ExchangeRef::new(id));
            debug_assert!(
              matches!(
                action,
                StreamAction::Shutdown(_) | StreamAction::Close(_) | StreamAction::Abort(_)
              ),
              "pending_teardowns holds only Shutdown / Close / Abort actions",
            );
            self.pending_teardowns.push_back(action);
          }
        },
        PendingMint::Inbound(peer) => match self.ep.accept_stream(peer, now) {
          Some(stream) => {
            if let Some(br) = self.conns.get_mut(id) {
              br.promote(stream);
            }
          }
          None => {
            // A Leaving/Left node mints no inbound Stream and leaves no live
            // bridge: a connection accepted before leave whose handshake only
            // settles during the drain is fully cancelled, so its record layer
            // queues and emits no bytes and it holds no buffers to the deadline.
            // `accept_stream` owns the lifecycle decision; `None` is the signal.
            self.cancel_exchange(id);
          }
        },
      }
    }
  }

  /// Zero-time outbound flush invoked from the high-level `start_*` APIs AFTER
  /// `service_dials`. Runs the shared tick tail (bridge pump + label /
  /// handshake-settled mint + `collect_transmits`) WITHOUT step (3)
  /// (`Endpoint::handle_timeout`).
  ///
  /// Step (3) is deliberately skipped: membership timers advance solely
  /// through the driver's explicit [`Self::handle_timeout`], which fires AFTER
  /// the driver has drained [`Self::poll_memberlist_ingress`], decoded each
  /// frame, and fed each typed message via [`Self::handle_packet`]. Advancing
  /// time inside a `start_*` call would fire same-instant probe / suspect /
  /// gossip / push-pull schedulers BEFORE a just-arrived (still-buffered)
  /// `Ack` / `Alive` is decoded and applied.
  ///
  /// `service_dials` is run BY THE CALLER (the `start_*` wrapper) before this
  /// method; this flush then mints any label / handshake step that settled
  /// in-band and pumps the freshly-built bridge so its first label prefix
  /// (fresh dial) or its request bytes (label / handshake step already
  /// settled) emerge on the next [`Self::poll_transport_transmit`].
  pub(crate) fn flush_outbound(&mut self, now: Instant) {
    self.pump_bridges(now);
    self.service_handshake_completions(now);
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Shared tail of [`Self::run_tick`] and [`Self::flush_outbound`]: collect
  /// outbound bytes from every live bridge. (Terminal bridges already flushed
  /// their final bytes at reap time inside [`Self::reap_bridge`].)
  fn finalize_tick(&mut self, _now: Instant) {
    self.collect_transmits();
  }

  /// Move any `Event::DialRequested` currently in the inner endpoint's queue
  /// into the private [`dial_pending`](Self::dial_pending) deque, preserving
  /// FIFO order of every other event.
  pub(crate) fn sieve_dial_events(&mut self) {
    let mut others: Vec<Event<I, A>> = Vec::new();
    while let Some(ev) = self.ep.poll_event() {
      match ev {
        Event::DialRequested(dial) => {
          let (id, peer, deadline) = dial.into_parts();
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
        }
        other => others.push(other),
      }
    }
    for ev in others {
      self.ep.requeue_event(ev);
    }
  }

  /// Collect outbound bytes from every live bridge into the outbound queue,
  /// tagged with the exchange handle + peer.
  fn collect_transmits(&mut self) {
    for id in self.conns.ids() {
      if let Some(mut br) = self.conns.remove(id) {
        self.collect_bridge_transmits(id, &mut br);
        self.conns.insert(id, br);
      }
    }
  }
}

// The full SWIM bag. Methods that resolve a peer address to a transport
// `SocketAddr` via the stored `peer_to_socket` closure, derive a per-dial
// record-layer context via `R::dial_context::<A>`, or transitively reach
// either through the coordinator tick (`run_tick` → `service_dials`).
impl<I, A, R> StreamEndpoint<I, A, R>
where
  I: crate::Id
    + crate::Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: crate::Data
    + crate::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  /// Inbound gossip datagram from the UDP socket.
  ///
  /// **Buffered only** — the codec-owning driver MUST drain via
  /// [`Self::poll_memberlist_ingress`], decode each frame, feed every typed
  /// message via [`Self::handle_packet`], and then call
  /// [`Self::handle_timeout`] to advance time. Running [`Self::handle_timeout`]
  /// before the buffered gossip is decoded and fed would risk same-instant
  /// probe / suspect timers firing before a just-arrived `Ack` / `Alive` is
  /// applied — a spurious fallback ping or false `Suspect` could fire even
  /// though the resolving message is already sitting in
  /// [`Self::poll_memberlist_ingress`]'s queue locally. Every UDP datagram is
  /// carried as gossip (reliable exchanges ride separate transport connections).
  pub fn handle_gossip(&mut self, from: A, datagram: &[u8], now: Instant) {
    self.last_now = Some(now);
    // A leaving/left node buffers no inbound gossip: handle_packet would drop it
    // anyway, so do not allocate or queue it during the drain.
    if !self.ep.is_running() {
      return;
    }
    self
      .mem_ingress
      .push_back((from, Bytes::copy_from_slice(datagram)));
  }

  /// Inbound bytes for one exchange's transport connection.
  ///
  /// Routes `bytes` into the owning bridge's
  /// [`StreamBridge::handle_transport_data`], then runs a coordinator tick.
  ///
  /// `eof = true` signals the transport `read == 0` half-close anchor — the
  /// out-of-band peer-FIN a transport with no in-band close (plain TCP)
  /// delivers in place of an in-band `close_notify` alert. A record layer with
  /// an in-band close infers its close anchor from `peer_has_closed()` (latched
  /// on the in-band alert), but a record layer with no in-band close keeps
  /// `peer_has_closed()` permanently `false` — there is no in-band close
  /// signal — so the driver MUST surface the FIN via this parameter. The
  /// bridge's byte pump derives the close-anchor truth value
  /// (`eof || records.peer_has_closed()`) uniformly across every record layer;
  /// the explicit flag carries the missing transport signal.
  ///
  /// A `(bytes.len() > 0, eof = true)` delivery — bytes followed by an
  /// observed `read == 0` on the same wake — is fed in two steps: the bytes
  /// first (one full pump), then an empty-slice EOF (the recv-half retirement
  /// anchor). The single coordinator tick at the end advances time once.
  pub fn handle_transport_data(&mut self, id: ExchangeId, bytes: &[u8], eof: bool, now: Instant) {
    self.last_now = Some(now);
    if let Some(bridge) = self.conns.get_mut(id) {
      if !bytes.is_empty() {
        // Ignoring Err: an `Err` means the bridge terminalized (label /
        // decode / transport failure); `run_tick`'s `pump_bridges` reaps it
        // and emits the `Close` action. There is no separate action here.
        let _ = bridge.handle_transport_data(bytes, now);
      }
      if eof {
        // Empty-slice feed = transport `read == 0` EOF anchor. Drives the
        // recv-half retirement (`observe_recv_fin`) or the truncation-decode-
        // fail path depending on the bridge's current state. Run even when the
        // bytes step terminalized the bridge — the bridge ignores subsequent
        // feeds in a terminal phase.
        // Ignoring Err: same as the bytes feed above — terminality is the
        // reap signal, not the return value.
        let _ = bridge.handle_transport_data(&[], now);
      }
    }
    self.run_tick(now);
  }

  /// The driver's outbound dial task for exchange `id` failed to connect
  /// (connection refused, dial timeout, host unreachable) BEFORE any bytes
  /// flowed. Fails the owning bridge so the next coordinator tick reaps it
  /// with [`ExchangeOutcome::Failed`] and emits the terminal
  /// [`Event::ExchangeCompleted`].
  ///
  /// This is distinct from [`Self::handle_transport_data`] with `eof = true`:
  /// a clean transport `read == 0` (peer half-closed after the wire was
  /// established) is benign for a one-way `UserMessage` (the FSM maps it to a
  /// successful completion — see `Stream::handle_data`'s
  /// `OutboundSendingRequest(UserMessage)` EOF arm). A dial that never
  /// connected has NO established wire: feeding it as a benign EOF would
  /// FALSELY complete a `UserMessage` `send_reliable` as success. Routing the
  /// connect failure through `fail_dial_retired` instead terminalizes the
  /// exchange as a genuine failure regardless of kind, so the driver's parked
  /// reliable-send waiter resolves with an error. A no-op if the exchange's
  /// bridge was already reaped (a same-tick `Close`/`Abort` from the machine).
  pub fn handle_dial_failed(&mut self, id: ExchangeId, now: Instant) {
    self.last_now = Some(now);
    if let Some(bridge) = self.conns.get_mut(id) {
      bridge.fail_dial_retired();
    }
    self.run_tick(now);
  }

  /// The driver accepted an inbound transport connection from `from`.
  /// Allocates an [`ExchangeId`], builds a server-side `Handshaking` bridge
  /// bounded by [`ACCEPT_HANDSHAKE_DEADLINE`], and returns the handle the
  /// driver tags this connection's inbound bytes with. The `Stream` is minted
  /// later (at label / handshake step settled, via `Endpoint::accept_stream`).
  ///
  /// An `R::acceptor` construction error (a misconfigured record layer) is
  /// unrecoverable for this connection: no bridge is inserted and the returned
  /// handle has no exchange. The driver observes this as a connection that
  /// never produces bytes (and may close it on its own accept-side deadline);
  /// the membership layer is untouched because no `Stream` ever existed.
  pub fn accept_connection(&mut self, from: A, now: Instant) -> ExchangeId {
    self.last_now = Some(now);
    let id = self.conns.allocate();
    // A leaving/left node accepts no NEW inbound reliable stream: it builds no
    // bridge, so no Stream is ever minted and no push/pull response leaves the
    // node. The handle is still returned (monotonic, never reused) so the driver
    // has a stable id; the connection produces no bytes and the driver closes it
    // on its own accept deadline. Streams accepted before leave keep draining.
    if !self.ep.is_running() {
      return id;
    }
    // SocketAddr conversion needed: `peer_socket` tags entries on
    // `out_transmit` (driver-facing SocketAddr-typed queue) so the driver
    // writes bytes on the right transport connection.
    let peer_socket = (self.peer_to_socket)(&from);
    let peer = crate::CheapClone::cheap_clone(&from);
    match R::acceptor(&self.cfg) {
      Ok(records) => {
        let bridge = StreamBridge::new(
          records,
          now + self.ep.accept_handshake_deadline(),
          self.compression,
          self.encryption.clone(),
          self.ep.max_stream_frame_size(),
        );
        self.conns.insert(id, bridge);
        self.exchanges.insert(
          id,
          ExchangeMeta {
            peer_socket,
            peer,
            mint: Some(PendingMint::Inbound(from)),
            fin_emitted: false,
            // Inbound exchange — the initiator-side wrapper that picks
            // the kind never ran locally. Leave unset; the reap path
            // does not emit kind-specific events for inbound.
            kind: None,
            outbound: false,
          },
        );
      }
      Err(_) => {
        // No bridge for a config-rejected server connection. The handle is
        // still returned (monotonic; never reused) so the driver has a stable
        // key, but it maps to no exchange.
      }
    }
    id
  }

  /// Initiate an outbound push/pull state exchange with `peer` and attempt the
  /// dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_push_pull`] that ALSO drives
  /// `service_dials(now)` + `flush_outbound(now)` before returning, so the
  /// `DialRequested` the inner endpoint queues is sieved, attempted (the
  /// `Connect` action surfaced and the `Handshaking` bridge built), and the
  /// dial's first label prefix / handshake flight emerges on the very next
  /// [`Self::poll_transport_transmit`] — a driver that uses only the public
  /// poll surface sees the exchange progress without a same-instant
  /// `handle_timeout` pre-pump.
  pub fn start_push_pull(&mut self, peer: A, kind: PushPullKind, now: Instant) -> StreamId {
    self.last_now = Some(now);
    let id = self.ep.start_push_pull(peer, kind, now);
    self
      .pending_outbound_kinds
      .insert(id, crate::event::ExchangeKind::PushPull);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a reliable-stream fallback ping for probe `probe_seq` and attempt
  /// the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_reliable_ping`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time outbound-flush
  /// semantics. The `deadline` is the owning probe's single cumulative
  /// deadline (NOT an independent stream-timeout), forwarded unchanged; `now`
  /// is taken separately because `service_dials` needs the real wall-clock
  /// instant and `last_now` must remain a known-past anchor.
  pub fn start_reliable_ping(
    &mut self,
    peer_id: I,
    peer_addr: A,
    probe_seq: u32,
    deadline: Instant,
    now: Instant,
  ) -> StreamId {
    self.last_now = Some(now);
    let id = self
      .ep
      .start_reliable_ping(peer_id, peer_addr, probe_seq, deadline);
    self
      .pending_outbound_kinds
      .insert(id, crate::event::ExchangeKind::ReliablePing);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a one-way reliable user-message delivery to `peer` and attempt
  /// the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_user_message`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time
  /// outbound-flush semantics.
  pub fn start_user_message(
    &mut self,
    peer: A,
    payload: Bytes,
    now: Instant,
  ) -> Result<StreamId, crate::error::Error> {
    self.last_now = Some(now);
    let id = self.ep.start_user_message(peer, payload, now)?;
    self
      .pending_outbound_kinds
      .insert(id, crate::event::ExchangeKind::UserMessage);
    self.service_dials(now);
    self.flush_outbound(now);
    Ok(id)
  }

  /// Timer tick from the driver.
  pub fn handle_timeout(&mut self, now: Instant) {
    self.last_now = Some(now);
    self.run_tick(now);
  }

  /// The fixed per-tick step order (load-bearing — see module docs).
  ///
  /// Step (2) (pump every bridge + drain each non-terminal stream's
  /// endpoint-events into the `Endpoint`) MUST strictly precede step (3)
  /// (`ep.handle_timeout`): a reliable-fallback ping ack delivered on the same
  /// tick the probe cumulative deadline expires is carried by the stream's
  /// last `poll_endpoint_event`; draining it after the probe timeout would
  /// lose it and wrongly Suspect a live peer. Do not reorder.
  ///
  /// Step (4) (label / handshake-settled mint) mints the `Stream` for any
  /// bridge whose label / handshake step settled since the last tick and
  /// promotes it; a freshly-promoted OUTBOUND bridge carries its request bytes
  /// in the minted `Stream`'s output buffer. Step (5) (`service_dials`) inserts
  /// new `Handshaking` outbound bridges. A dialer record layer with no
  /// handshake (its inbound label is validated in-line on the established
  /// intake) is never handshaking, so step (5.5) — a second
  /// `service_handshake_completions` — promotes those freshly-inserted dial
  /// bridges in the SAME tick, before step (5.6)'s `pump_bridges` pumps their
  /// request bytes out. Without the step (5.5) extra promote, a
  /// reliable-fallback ping bridge created by step (3) would have its `Stream`
  /// minted only on the NEXT coordinator wake — under a strict-poll driver
  /// that wakes only at [`Self::poll_timeout`], that next wake is the bridge's
  /// exchange deadline itself, at which point
  /// [`crate::stream::Stream::handle_data`] would reject the buffered request
  /// as timed out. `pump_bridges` and `service_handshake_completions` are both
  /// idempotent on already-handled bridges, so the duplicated calls are no-ops
  /// on bridges already serviced upstream. There is NO connection drained-reap
  /// step (connection-per-exchange — a reaped bridge frees its own connection
  /// via the `Close` action).
  fn run_tick(&mut self, now: Instant) {
    // (1) inbound feed already done by the caller (`handle_transport_data`).
    // (2) pump bridges + drain stream endpoint-events into the Endpoint.
    self.pump_bridges(now);
    // (3) THEN membership timers (probe cumulative-deadline, suspicion).
    self.ep.handle_timeout(now);
    // (4) mint the Stream for any bridge whose label / handshake step just
    // settled.
    self.service_handshake_completions(now);
    // (5) dial requests emitted by (3).
    self.service_dials(now);
    // (5.5) promote any dial bridge whose records are not handshaking from
    // the moment of construction (the dialer's role) so step (5.6)'s pump
    // can transmit the request bytes this same tick. Idempotent on bridges
    // already promoted by step (4).
    self.service_handshake_completions(now);
    // (5.6) pump bridges promoted/inserted by (4), (5), and (5.5) this same
    // tick.
    self.pump_bridges(now);
    self.finalize_tick(now);
    // Clear the policy-change reap latch: both `pump_bridges` calls above
    // have reaped every terminal bridge in `conns` (a bridge failed by
    // `set_encryption_options` is in `BridgePhase::Failed` and therefore
    // `is_terminal()`), so any wake the latch was asking for has been
    // serviced this tick. Leaving the latch set would have `poll_timeout`
    // keep returning immediate-due wakes forever once the reap is done.
    self.policy_reap_pending = false;
  }

  /// Step (5): drain the private `dial_pending` deque, surfacing one
  /// [`StreamAction::Connect`] and building one `Handshaking` client bridge per
  /// intent. Does NOT call `dial_succeeded` — the `Stream` is minted at the
  /// label / handshake-settled step (step 4) across a later tick (or this same
  /// tick when invoked from a `start_*` flush, since a no-handshake dialer's
  /// label step settles at construction).
  pub(crate) fn service_dials(&mut self, now: Instant) {
    // A leaving/left node initiates no dial: skip sieving and draining so no
    // bridge is built or Connect surfaced after leave. Defensive — the start_*
    // gates and requeue_event already prevent new DialRequested — but this
    // guards any residual intent against opening a fresh connection.
    if !self.ep.is_running() {
      return;
    }
    // Sieve any DialRequested newly emitted by the inner endpoint into the
    // private deque, then drain that deque as the sole input. Non-DialRequested
    // events stay in the inner endpoint's queue for the public `poll_event`.
    self.sieve_dial_events();
    let pending = core::mem::take(&mut self.dial_pending);
    for entry in pending {
      let PendingDial {
        id,
        peer,
        deadline,
        attempted: _,
      } = entry;
      // Take the kind stashed by the originating `start_*` wrapper up front.
      // Every pre-`ExchangeMeta` failure path below — expired deadline, TLS
      // SNI rejection at `R::dial_context`, dialer construction error —
      // calls `dial_failed` and `continue`s without re-inserting, so the
      // map is implicitly cleaned up on every failure exit. Only the
      // success path past `ExchangeMeta` allocation re-inserts (so a later
      // `reap_bridge` can carry the exchange's kind on
      // `Event::ExchangeCompleted`). `None` here indicates a `DialRequested`
      // emitted outside this coordinator's start path — kept defensive
      // (reap then does not emit a kind-specific event for this exchange).
      let exchange_kind = self.pending_outbound_kinds.remove(&id);
      // Retire the intent without opening a connection if its own deadline
      // has already elapsed (mirrors the sibling coordinators'
      // expired-intent gate).
      if now >= deadline {
        self.ep.dial_failed(
          id,
          crate::error::StreamError::DialFailed("stream dial deadline elapsed".into()),
          now,
        );
        continue;
      }
      // SocketAddr conversion needed: `peer_socket` tags entries on
      // `out_transmit` and the `StreamAction::Connect(ConnectInfo)` payload
      // (both driver-facing SocketAddr-typed surfaces).
      let peer_socket = (self.peer_to_socket)(&peer);
      let peer_a = crate::CheapClone::cheap_clone(&peer);
      // Resolve the per-dial record-layer context (e.g. the TLS verification
      // identity). An `Err` is the soft-fail-via-dial_failed path —
      // retire the intent for this one peer and move on.
      let sni_owned = (self.sni_provider)(&peer);
      let ctx = match R::dial_context::<A>(&peer, sni_owned.as_deref()) {
        Ok(c) => c,
        Err(msg) => {
          self
            .ep
            .dial_failed(id, crate::error::StreamError::DialFailed(msg.into()), now);
          continue;
        }
      };
      // Construct the dialer record layer. A construction error retires the
      // intent (a misconfigured record layer cannot dial); for a record layer
      // with infallible constructors this branch is unreachable.
      let records = match R::dialer(&self.cfg, ctx) {
        Ok(r) => r,
        Err(e) => {
          self.ep.dial_failed(
            id,
            crate::error::StreamError::DialFailed(
              format!("record-layer construction failed: {e}").into(),
            ),
            now,
          );
          continue;
        }
      };
      let exchange = self.conns.allocate();
      let bridge = StreamBridge::new(
        records,
        deadline,
        self.compression,
        self.encryption.clone(),
        self.ep.max_stream_frame_size(),
      );
      self.conns.insert(exchange, bridge);
      self.exchanges.insert(
        exchange,
        ExchangeMeta {
          peer_socket,
          peer: peer_a,
          mint: Some(PendingMint::Outbound(id)),
          fin_emitted: false,
          kind: exchange_kind,
          outbound: true,
        },
      );
      // `id` is the machine `StreamId` of the originating `start_*` dial,
      // threaded into the Connect so a driver can correlate a surfaced Connect
      // back to the exact `start_*` it issued — `service_dials` drains the
      // SHARED `dial_pending` deque, so one command's drain can also flush an
      // unrelated same-peer dial; the peer address alone does not call-scope it.
      let action = StreamAction::Connect(ConnectInfo::new(exchange, peer_socket, id));
      debug_assert!(
        matches!(action, StreamAction::Connect(_)),
        "pending_connects holds only Connect actions",
      );
      self.pending_connects.push_back(action);
    }
  }
}
