//! Transport-agnostic address bridge trait, shared by the QUIC and TLS
//! coordinators.
//!
//! Maps the memberlist address type `A` to the concrete `SocketAddr` the
//! transport endpoint dials/accepts on, and back; also supplies the rustls
//! `server_name` used to verify the peer's certificate at dial time.

use std::net::SocketAddr;

/// Maps the memberlist address type `A` to the concrete `SocketAddr` the
/// coordinator dials/accepts on, and back; also supplies the rustls
/// `server_name` used to verify the peer's certificate at dial time.
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
  /// The peer `SocketAddr` to dial / the wire address of an inbound peer.
  fn to_socket(addr: &A) -> SocketAddr;
  /// The memberlist address for a peer observed at `socket`.
  fn from_socket(socket: SocketAddr) -> A;
  /// The rustls `server_name` used to verify the peer's certificate at
  /// dial time.
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
  /// returns `Cow::Borrowed(&name)`; the sim's identity bridge (where
  /// `A = SocketAddr`) returns `Cow::Borrowed("localhost")` to match the
  /// localhost-SAN test certs.
  ///
  /// The seam lives on the bridge — not on each `start_*` call — so the
  /// verification identity is sourced uniformly from the same place the
  /// driver supplies the wire address.
  fn server_name(addr: &A) -> std::borrow::Cow<'_, str>;
}
