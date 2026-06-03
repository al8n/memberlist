//! The `StreamTransport` trait + the `Intake` enum that record-layer
//! impls return from `handle_transport_data`.
//!
//! `StreamTransport` is the surface `StreamBridge` and `StreamEndpoint`
//! use to drive a record layer transport-agnostically. The bridge feeds
//! transport ciphertext via `handle_transport_data`, drains decrypted
//! plaintext via `read_plaintext`, encrypts outbound via `write_plaintext`,
//! and drains the resulting ciphertext via `poll_transport_transmit`.
//! The trait's associated types parameterize the per-record-layer
//! options (`Options`), the per-dial extra information needed at
//! construction (`DialContext`), and the construction error
//! (`ConstructError`).

use crate::Instant;
#[cfg(not(feature = "std"))]
use std::vec::Vec;

/// Outcome of a single [`StreamTransport::handle_transport_data`] call.
///
/// * `Done` — input fully consumed; the record layer made progress
///   (handshake step completed, or plaintext was surfaced via
///   [`StreamTransport::read_plaintext`]).
/// * `Pending(n)` — `n` bytes of input were consumed but the record
///   layer is back-pressured (e.g. rustls's 16 KiB received-plaintext
///   limit). The bridge interleaves a `read_plaintext` drain with
///   another `handle_transport_data` call to feed the remainder.
/// * `Failed` — the input is a terminal record-layer reject (TLS
///   handshake mismatch; TCP label-mismatch / double-label / over-long
///   / non-UTF-8 / unlabeled-but-required). The bridge terminalizes
///   with `BridgeFailure::Transport`.
#[derive(Debug, PartialEq, Eq)]
pub enum Intake {
  /// Input fully consumed; the record layer made progress (a handshake
  /// step completed, or plaintext was surfaced via
  /// [`StreamTransport::read_plaintext`]).
  Done,
  /// `n` bytes of input were consumed but the record layer is
  /// back-pressured (e.g. rustls's 16 KiB received-plaintext limit). The
  /// bridge interleaves a `read_plaintext` drain with another
  /// `handle_transport_data` call to feed the remainder.
  Pending(usize),
  /// The input is a terminal record-layer reject (a TLS handshake
  /// mismatch; a TCP label-mismatch / double-label / over-long /
  /// non-UTF-8 / unlabeled-but-required). The bridge terminalizes.
  Failed,
}

/// The bridge-facing surface that every plug-in record layer
/// (`tls::TlsRecords`, `tcp::RawRecords`, future `EncryptedRecords`)
/// must provide to be drivable by `StreamBridge` and `StreamEndpoint`.
pub trait StreamTransport: Sized {
  /// Caller-built immutable options bundle for this record layer.
  /// `tls::TlsRecords::Options = tls::TlsOptions`;
  /// `tcp::RawRecords::Options = LabelOptions<()>`.
  type Options;

  /// Per-dial extra information the record layer needs at the moment
  /// of constructing a dialer (e.g. the rustls `ServerName` for cert
  /// verification). Derived from the supplied `server_name` hint at dial
  /// time via [`Self::dial_context`].
  /// `tls::TlsRecords::DialContext = rustls::pki_types::ServerName<'static>`;
  /// `tcp::RawRecords::DialContext = ()`.
  type DialContext;

  /// Error type returned by [`Self::dialer`] / [`Self::acceptor`].
  /// `tls::TlsRecords::ConstructError = rustls::Error`;
  /// `tcp::RawRecords::ConstructError = core::convert::Infallible`.
  type ConstructError: core::fmt::Display;

  /// Derive the per-dial context from the address and an explicit
  /// `server_name` hint. Returns `Err(reason)` for the
  /// soft-fail-via-dial_failed path: the coordinator calls
  /// `Endpoint::dial_failed(stream_id,
  /// StreamError::DialFailed(reason))` and drops the dial intent for
  /// that one peer.
  ///
  /// `server_name` carries the SNI string the coordinator's per-peer
  /// `sni_provider` closure produced for this dial. Transports that
  /// require SNI for peer authentication (e.g. `tls::TlsRecords`) use it
  /// directly; transports without an SNI requirement (e.g.
  /// `tcp::RawRecords`) ignore it.
  fn dial_context<A>(
    addr: &A,
    server_name: Option<&str>,
  ) -> Result<Self::DialContext, &'static str>;

  /// Construct the dialer (client) side of the record layer from the
  /// options bundle and the per-dial context produced by
  /// [`Self::dial_context`].
  fn dialer(opts: &Self::Options, ctx: Self::DialContext) -> Result<Self, Self::ConstructError>;
  /// Construct the acceptor (server) side of the record layer from the
  /// options bundle.
  fn acceptor(opts: &Self::Options) -> Result<Self, Self::ConstructError>;

  /// Feed inbound transport bytes (one transport read) into the record
  /// layer at `now`, returning the [`Intake`] outcome.
  fn handle_transport_data(&mut self, input: &[u8], now: Instant) -> Intake;
  /// Drain any outbound transport bytes the record layer has queued into
  /// `out`, returning the number of bytes appended.
  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize;
  /// `true` while the record layer has not yet completed its handshake
  /// (always `false` for a record layer with no handshake, e.g. plain TCP
  /// once its label step has settled).
  fn is_handshaking(&self) -> bool;
  /// Drain decrypted application plaintext into `out`, returning the
  /// number of bytes appended.
  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize;
  /// Encrypt `plaintext` into the record layer's outbound queue (drained
  /// via [`Self::poll_transport_transmit`]).
  fn write_plaintext(&mut self, plaintext: &[u8]);

  /// TLS: queue the `close_notify` alert (in-band). TCP: no-op (the
  /// FIN is the out-of-band TCP `shutdown(write)` at the driver layer).
  fn send_close_notify(&mut self);

  /// TLS: returns `true` once the in-band `close_notify` has been
  /// observed via `process_new_packets`. TCP: always `false` (TCP has
  /// no in-band close signal; the bridge's `pending_eof` latch
  /// carries the out-of-band FIN instead).
  fn peer_has_closed(&self) -> bool;

  /// Drop any outbound bytes queued in this record layer. Called by
  /// `StreamBridge::fail` on every failure transition so a Failed
  /// bridge cannot leak its local label / partial response on the
  /// subsequent reap path.
  fn clear_outbound(&mut self);

  /// `true` when the underlying transport already provides confidentiality
  /// (e.g. TLS); `false` when it does not (e.g. plain TCP).
  ///
  /// Type-level capability — no `&self`. `R::is_secure()` is a compile-time
  /// constant per impl. The bridge uses this to skip wrapping the reliable
  /// unit in an `Encrypted` wrapper when the transport already encrypts:
  /// double-encryption costs CPU and bandwidth without adding security.
  ///
  /// Mirrors the legacy `memberlist-net::StreamLayer::is_secure`.
  fn is_secure() -> bool;
}
