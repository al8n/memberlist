//! TLS-over-TCP record layer for the generic Sans-I/O stream transport.
//!
//! rustls is itself Sans-I/O — the TLS record layer lives in the deterministic
//! core and the driver moves only raw bytes. [`TlsRecords`] wraps a rustls
//! `ClientConnection` / `ServerConnection` behind a byte-only interface and
//! implements [`StreamTransport`], so a [`crate::streams::StreamEndpoint`]
//! parameterised with `R = TlsRecords` carries reliable membership exchanges
//! over a per-exchange TLS-over-TCP connection (plain UDP carries the
//! unreliable gossip on a separate socket).

#[cfg(test)]
mod bridge;
#[cfg(test)]
mod conn;
mod options;
mod records;

use std::time::Instant;

use rustls::pki_types::ServerName;

use crate::{
  addr_bridge::AddrBridge,
  streams::transport::{Intake, StreamTransport},
};

pub use options::TlsOptions;
pub use records::TlsRecords;

/// The TLS record layer as a transport-agnostic [`StreamTransport`] plug.
///
/// Adapts the inherent [`TlsRecords`] surface — whose `handle_transport_data`
/// takes only `&[u8]` and returns `Result<records::Intake, rustls::Error>` —
/// to the unified trait surface: the `now` argument is accepted and dropped
/// (rustls is timer-free; deadlines live in the bridge), and the rustls `Err`
/// (mTLS reject, decrypt error, malformed record) is mapped to
/// [`Intake::Failed`] — the trait's terminal-reject variant — while the local
/// `records::Intake::{Done, Pending}` map straight across.
impl StreamTransport for TlsRecords {
  type Options = TlsOptions;
  type DialContext = ServerName<'static>;
  type ConstructError = rustls::Error;

  fn dial_context<A, B>(addr: &A) -> Result<ServerName<'static>, &'static str>
  where
    B: AddrBridge<A>,
  {
    let sn = B::server_name(addr).ok_or("tls bridge returned None for server_name")?;
    ServerName::try_from(sn.as_ref().to_owned()).map_err(|_| "tls server_name failed to parse")
  }

  fn dialer(opts: &Self::Options, ctx: Self::DialContext) -> Result<Self, Self::ConstructError> {
    TlsRecords::client(opts.client().clone(), ctx)
  }

  fn acceptor(opts: &Self::Options) -> Result<Self, Self::ConstructError> {
    TlsRecords::server(opts.server().clone())
  }

  fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    // The inherent method is timer-free, so `_now` is dropped. A rustls `Err`
    // is a terminal protocol failure (mTLS reject / decrypt / malformed
    // record) and maps to the unified `Intake::Failed`; `records::Intake` has
    // no `Failed` of its own — the failure rides the `Result`.
    match TlsRecords::handle_transport_data(self, input) {
      Ok(records::Intake::Done) => Intake::Done,
      Ok(records::Intake::Pending(n)) => Intake::Pending(n),
      Err(_) => Intake::Failed,
    }
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    TlsRecords::poll_transport_transmit(self, out)
  }

  fn is_handshaking(&self) -> bool {
    TlsRecords::is_handshaking(self)
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    TlsRecords::read_plaintext(self, out)
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) {
    TlsRecords::write_plaintext(self, plaintext)
  }

  fn send_close_notify(&mut self) {
    TlsRecords::send_close_notify(self)
  }

  fn peer_has_closed(&self) -> bool {
    TlsRecords::peer_has_closed(self)
  }

  fn clear_outbound(&mut self) {
    // No-op for TLS: there is no separately-queued outbound buffer in
    // `TlsRecords`. `write_plaintext` encrypts straight into rustls and
    // `poll_transport_transmit` drains rustls's own `write_tls` queue —
    // rustls exposes no API to discard that pending ciphertext. The failure
    // path's leak prevention for TLS is the FSM's `enter_failed` clearing its
    // `output_buf` (so no further plaintext is ever handed to `write_plaintext`),
    // not a record-layer buffer clear. TCP's `RawRecords`, which DOES hold a
    // plaintext-side `outbound` buffer (the label prefix + raw bytes), is the
    // record layer this trait method exists to serve.
  }

  fn is_secure() -> bool {
    true
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn tls_records_is_secure_returns_true() {
    use super::TlsRecords;
    use crate::streams::StreamTransport;
    assert!(
      TlsRecords::is_secure(),
      "TLS provides transport confidentiality"
    );
  }

  #[test]
  fn tls_endpoint_type_is_constructible_signature() {
    // Behavioural coverage is tls_conformance (needs the sim clock + a peer +
    // the virtual TCP). This guards the public constructor signature only.
    fn _sig<I, A, B>()
    where
      I: nodecraft::Id
        + memberlist_wire::Data
        + nodecraft::CheapClone
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
      A: memberlist_wire::Data
        + nodecraft::CheapClone
        + Eq
        + core::hash::Hash
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
      B: super::AddrBridge<A>,
    {
      let _: fn(
        crate::endpoint::Endpoint<I, A>,
        super::TlsOptions,
      ) -> crate::streams::StreamEndpoint<I, A, B, crate::tls::records::TlsRecords> =
        crate::streams::StreamEndpoint::<I, A, B, crate::tls::records::TlsRecords>::new;
    }
  }

  /// The TLS coordinator's GOSSIP path still encrypts when configured — gossip
  /// is plain UDP regardless of the reliable transport, so only the reliable
  /// path skips its inner Encrypted wrapper (TLS already wraps it). The gossip
  /// datagram is exchanged on a separate socket and needs its own
  /// confidentiality envelope.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn tls_endpoint_gossip_encryption_roundtrip() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use memberlist_wire::{EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      addr_bridge::AddrBridge,
      config::EndpointConfig,
      endpoint::Endpoint,
      streams::StreamEndpoint,
      tls::options::tests::{test_client, test_server},
    };

    /// Identity `AddrBridge` for `A = SocketAddr`, parallel to the TCP test
    /// harness. The `server_name` accessor reports `"localhost"` so the
    /// coordinator construction matches the gossip-only smoke shape (no
    /// per-exchange reliable dial is performed here).
    struct IdentityBridge;
    impl AddrBridge<SocketAddr> for IdentityBridge {
      type ServerName = str;
      fn to_socket(addr: &SocketAddr) -> SocketAddr {
        *addr
      }
      fn from_socket(socket: SocketAddr) -> SocketAddr {
        socket
      }
      fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
        Some("localhost")
      }
    }

    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7300);
    let ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("n-7300"), local));
    let cfg = TlsOptions::new(test_server(), test_client());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0xAB; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg).with_encryption(opts);
    let datagram = b"tls gossip body".to_vec();
    let on_wire = coord.encrypt_gossip(&datagram).expect("encrypt");
    assert_ne!(
      on_wire, datagram,
      "encrypted gossip differs from the plaintext datagram"
    );
    assert_eq!(
      on_wire[0],
      memberlist_wire::ENCRYPTED_TAG,
      "TLS gossip path still encrypts (gossip is plain UDP)"
    );
    let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
    assert_eq!(back, datagram);
  }
}
