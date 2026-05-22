//! Transport-agnostic address bridge trait, shared by the QUIC, TLS, and
//! TCP coordinators.
//!
//! Maps the memberlist address type `A` to the concrete `SocketAddr` the
//! transport endpoint dials/accepts on, and back; also supplies the
//! optional verification identity used at dial time on transports that
//! need one (TLS, QUIC). The verification identity is exposed via an
//! associated type `ServerName: ?Sized + AsRef<str>` so implementers
//! can choose the underlying representation. The most common choice is
//! `type ServerName = str;` — returning `Option<&str>` — which is
//! identical-ergonomics to the previous `Cow<str>`-based API modulo the
//! `Option` wrapping. A validated-newtype implementation can substitute
//! its own type so long as it derefs to `&str` via `AsRef`.
//!
//! The trait stays rustls-free at the type level — `AsRef<str>` is std
//! only — so TCP-only consumers do not transitively depend on
//! `rustls-pki-types`.

use std::net::SocketAddr;

/// Maps the memberlist address type `A` to the concrete `SocketAddr` the
/// coordinator dials/accepts on, and back; also supplies the optional
/// verification identity used at dial time on transports that need one
/// (TLS, QUIC).
///
/// The coordinator is generic over the implementor instead of hard-coding a
/// helper, so the only place that needs to know how `A` relates to a wire
/// `SocketAddr` — and what verification identity to assert against the peer's
/// cert — is the driver that owns the socket. The sim harness implements
/// this as the identity for `A = SocketAddr` (with `"localhost"` as the
/// `server_name`, matching the sim's localhost-SAN test cert); a production
/// driver maps its resolved/advertised address and returns the peer's real
/// SAN/CN. Carried as a zero-sized type parameter so the translation is a
/// static call with no per-coordinator state.
pub trait AddrBridge<A> {
  /// The verification-identity type. Default `str`; a custom
  /// validated-newtype hostname can substitute (e.g.
  /// `ValidatedHostname` that wraps a validated string).
  type ServerName: ?Sized + AsRef<str>;

  /// The peer `SocketAddr` to dial / the wire address of an inbound
  /// peer.
  fn to_socket(addr: &A) -> SocketAddr;

  /// The memberlist address for a peer observed at `socket`.
  fn from_socket(socket: SocketAddr) -> A;

  /// The verification identity used at dial time on transports that
  /// need one. Returns `None` for transports (plain TCP) that do not.
  ///
  /// Lifetime tied to the input `addr` — implementers must hold the
  /// identity somewhere reachable from `&A` (static literal, a slice
  /// into `addr`, or a cached value inside an addr-reachable buffer).
  ///
  /// Parsed via `ServerName::try_from(server_name)` and fed to the configured
  /// server-cert verifier on the reliable path: the QUIC coordinator threads
  /// it through `quinn_proto::Endpoint::connect`, the TLS coordinator through
  /// `rustls::ClientConnection::new`; both reach the same
  /// `ServerCertVerifier::verify_server_cert`.
  ///
  /// The value returned by the bridge MUST match the SAN/CN of the cert
  /// chain installed on the peer's `ServerConfig` — otherwise the rustls
  /// `WebPkiServerVerifier` (or any strict custom verifier) returns
  /// `CertificateError::NotValidForName` and the handshake fails. A
  /// production deployment that keys `A` on a name carrying the peer's SAN
  /// returns `Some(&name)`; the sim's identity bridge (where
  /// `A = SocketAddr`) returns `Some("localhost")` to match the
  /// localhost-SAN test certs.
  ///
  /// The seam lives on the bridge — not on each `start_*` call — so the
  /// verification identity is sourced uniformly from the same place the
  /// driver supplies the wire address.
  fn server_name(addr: &A) -> Option<&Self::ServerName>;
}
