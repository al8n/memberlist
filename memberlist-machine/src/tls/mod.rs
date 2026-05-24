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
    TlsRecords::client(opts.client_arc(), ctx)
  }

  fn acceptor(opts: &Self::Options) -> Result<Self, Self::ConstructError> {
    TlsRecords::server(opts.server_arc())
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
    use std::net::SocketAddr;

    use memberlist_wire::{EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        test_support::{endpoint, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7300);
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

  /// With NO keyring configured, `encrypt_gossip` is an identity transform
  /// — the on-wire datagram is byte-identical to the input plaintext, and
  /// `decrypt_gossip` returns the same bytes back. Mirrors the TCP analogue
  /// in `tcp/mod.rs::stream_endpoint_gossip_encryption_disabled_is_byte_identical`.
  #[test]
  fn tls_endpoint_gossip_encryption_disabled_is_byte_identical() {
    use std::net::SocketAddr;

    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        test_support::{endpoint, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7301);
    let cfg = TlsOptions::new(test_server(), test_client());
    let coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg);
    let datagram = b"a gossip body".to_vec();
    let on_wire = coord.encrypt_gossip(&datagram).expect("identity");
    assert_eq!(on_wire, datagram, "no keyring -> identity transform");
    let back = coord.decrypt_gossip(&on_wire).expect("identity");
    assert_eq!(back, datagram);
  }

  /// A keyring whose primary requires a backend the binary was not built
  /// with (here: ChaCha20-Poly1305 under an aes-gcm-only build) must
  /// surface as `Err`, NOT silently emit plaintext. Mirrors the TCP analogue
  /// in `tcp/mod.rs::stream_endpoint_encrypt_gossip_returns_err_on_unsupported_backend`.
  #[cfg(all(
    feature = "encryption-aes-gcm",
    not(feature = "encryption-chacha20-poly1305")
  ))]
  #[test]
  fn tls_endpoint_encrypt_gossip_returns_err_on_unsupported_backend() {
    use std::net::SocketAddr;

    use memberlist_wire::{EncryptionError, EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        test_support::{endpoint, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7302);
    let cfg = TlsOptions::new(test_server(), test_client());
    let kr = Keyring::new(SecretKey::ChaCha20Poly1305([0x42; 32]));
    let opts = EncryptionOptions::new().with_keyring(kr);
    let coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg).with_encryption(opts);
    let datagram = b"this gossip must not go out plaintext".to_vec();
    let err = coord
      .encrypt_gossip(&datagram)
      .expect_err("missing backend must surface as Err, not silent plaintext");
    assert!(
      matches!(err, EncryptionError::UnsupportedAlgorithm(_)),
      "got {err:?}"
    );
  }

  /// Strict-mode rejection MUST fire at the coordinator's public ingress.
  /// A configured keyring + a leading tag that is not `Encrypted` is an
  /// unauthenticated plaintext Ping/Ack/Alive frame; passing it through to
  /// `handle_gossip` would bypass strict-mode entirely. `decrypt_gossip`
  /// routes through `unwrap_transforms_with_encryption`, which applies the
  /// strict-mode entry check before any wrapper decoding.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn tls_endpoint_decrypt_gossip_rejects_plaintext_when_encryption_enabled() {
    use std::net::SocketAddr;

    use memberlist_wire::{
      encode_plain_frame, EncryptionOptions, FrameError, Keyring, MessageTag, SecretKey,
    };
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        test_support::{endpoint, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7303);
    let cfg = TlsOptions::new(test_server(), test_client());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg).with_encryption(opts);
    let plain_ping = encode_plain_frame(MessageTag::Ping, b"opaque-body").expect("encode");
    let result = coord.decrypt_gossip(&plain_ping);
    assert!(
      matches!(result, Err(FrameError::Encryption(_))),
      "decrypt_gossip MUST reject a plaintext datagram while encryption is \
       enabled — got {result:?}",
    );
  }

  /// Wire-byte guarantee for encrypted gossip under a configured `gossip_mtu`:
  /// a near-budget plaintext frame's on-wire ciphertext stays within
  /// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`. Operators on tight path-MTU
  /// networks size `gossip_mtu` accordingly; this test pins that the AEAD
  /// wrapper inflates the datagram by EXACTLY `ENCRYPTED_WRAPPER_OVERHEAD`.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn tls_endpoint_encrypted_gossip_wire_bytes_within_configured_mtu_plus_overhead() {
    use std::net::SocketAddr;

    use memberlist_wire::{EncryptionOptions, Keyring, SecretKey, ENCRYPTED_WRAPPER_OVERHEAD};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      config::EndpointConfig,
      endpoint::Endpoint,
      streams::{
        test_support::{addr, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let cfg_ep = EndpointConfig::new(SmolStr::new("local"), addr(7304)).with_gossip_mtu(1200);
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_ep);
    let cfg = TlsOptions::new(test_server(), test_client());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg).with_encryption(opts);
    let plaintext = vec![0xab; 1200];
    let on_wire = coord
      .encrypt_gossip(&plaintext)
      .expect("aes-gcm primary -> encrypt succeeds");
    assert!(
      on_wire.len() <= 1200 + ENCRYPTED_WRAPPER_OVERHEAD,
      "encrypted on-wire datagram MUST stay within \
       gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD; \
       got on_wire={} > 1200 + {} = {}",
      on_wire.len(),
      ENCRYPTED_WRAPPER_OVERHEAD,
      1200 + ENCRYPTED_WRAPPER_OVERHEAD,
    );
    assert_eq!(
      on_wire.len(),
      1200 + ENCRYPTED_WRAPPER_OVERHEAD,
      "AES-GCM is a streaming AEAD: a 1200-byte plaintext inflates by \
       EXACTLY ENCRYPTED_WRAPPER_OVERHEAD (30 bytes) — header + 12-byte \
       nonce + 16-byte auth tag",
    );
    let back = coord.decrypt_gossip(&on_wire).expect("roundtrip decrypt");
    assert_eq!(back, plaintext, "roundtrip preserves the plaintext bytes");
  }

  /// `EndpointConfig::with_gossip_mtu(n)` MUST be readable on the
  /// constructed coordinator via `gossip_mtu()`. No encryption is exercised
  /// — pure config plumbing.
  #[test]
  fn tls_endpoint_gossip_mtu_is_propagated_from_config() {
    use std::net::SocketAddr;

    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      config::EndpointConfig,
      endpoint::Endpoint,
      streams::{
        test_support::{addr, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let cfg_ep = EndpointConfig::new(SmolStr::new("local"), addr(7305)).with_gossip_mtu(1200);
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_ep);
    let cfg = TlsOptions::new(test_server(), test_client());
    let coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg);
    assert_eq!(
      coord.gossip_mtu(),
      1200,
      "StreamEndpoint::gossip_mtu must read the configured value (not the \
       legacy 1400 constant) so the FSM's plaintext-budget bound tracks \
       EndpointConfig",
    );
  }

  /// A runtime policy change MUST purge the `mem_ingress` queue. A datagram
  /// queued under the old policy would otherwise survive across the flip and
  /// be surfaced by the next `poll_memberlist_ingress` under the new strict-
  /// mode regime it was never validated against.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn tls_endpoint_set_encryption_options_purges_buffered_gossip_on_policy_change() {
    use std::{net::SocketAddr, time::Instant};

    use memberlist_wire::{EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        test_support::{addr, endpoint, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let now = Instant::now();
    let ep = endpoint(7306);
    let cfg = TlsOptions::new(test_server(), test_client());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg);

    // Queue a plaintext gossip datagram under disabled encryption.
    let peer = addr(7307);
    let datagram = [1u8, 2, 3, 4, 5];
    coord.handle_gossip(peer, &datagram, now);
    assert!(
      coord.poll_memberlist_ingress().is_some(),
      "pre-condition: a queued gossip datagram is observable through \
       poll_memberlist_ingress before the policy change",
    );
    // Re-queue (the assertion above drained the queue).
    coord.handle_gossip(peer, &datagram, now);

    // Publish an enabling encryption policy. The `mem_ingress` queue must be
    // cleared — the queued datagram was accepted on a disabled-encryption
    // policy and would otherwise surface under the new strict-mode regime.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    assert!(
      coord.poll_memberlist_ingress().is_none(),
      "set_encryption_options MUST purge the `mem_ingress` queue on a real \
       policy change",
    );
  }

  /// Reapplying the IDENTICAL `EncryptionOptions` MUST be a no-op: the
  /// queued `mem_ingress` datagram survives, and the coordinator's reported
  /// options stay equal. The no-op guard is `PartialEq` on
  /// `EncryptionOptions` + `Keyring`.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn tls_endpoint_set_encryption_options_is_noop_when_reapplying_same_policy() {
    use std::{net::SocketAddr, time::Instant};

    use memberlist_wire::{EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        test_support::{addr, endpoint, IdentityBridge},
        StreamEndpoint,
      },
      tls::options::tests::{test_client, test_server},
    };

    let now = Instant::now();
    let ep = endpoint(7308);
    let cfg = TlsOptions::new(test_server(), test_client());
    let key = SecretKey::Aes256([0x42; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, TlsRecords> =
      StreamEndpoint::new(ep, cfg).with_encryption(opts.clone());

    // Queue a gossip datagram under the enabled policy. (The strict-mode
    // entry check fires on `decrypt_gossip`, not `handle_gossip` — gossip
    // bytes are buffered raw and surfaced through `poll_memberlist_ingress`.)
    let peer = addr(7309);
    let datagram = [9u8, 8, 7, 6, 5];
    coord.handle_gossip(peer, &datagram, now);

    // Reapply the IDENTICAL options. The no-op guard MUST skip the purge:
    // queued bytes stay queued, and the coordinator's stored options stay
    // equal.
    coord.set_encryption_options(opts.clone());

    assert!(
      coord.poll_memberlist_ingress().is_some(),
      "a no-op reapply MUST NOT purge the `mem_ingress` queue — the queued \
       datagram was validated against the SAME options and is still in-bounds",
    );
    assert_eq!(
      coord.encryption_options(),
      &opts,
      "the no-op reapply still publishes the (identical) options as the \
       endpoint's current configuration",
    );
  }
}
