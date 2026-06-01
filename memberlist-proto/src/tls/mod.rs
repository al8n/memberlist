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

use crate::Instant;

use rustls::pki_types::ServerName;

use crate::streams::transport::{Intake, StreamTransport};

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

  fn dial_context<A>(
    _addr: &A,
    server_name: Option<&str>,
  ) -> Result<ServerName<'static>, &'static str> {
    let sn = server_name.ok_or("tls dial_context called without server_name")?;
    ServerName::try_from(sn.to_owned()).map_err(|_| "tls server_name failed to parse")
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
    fn _sig<I, A>()
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
    {
      let _: fn(
        crate::endpoint::Endpoint<I, A>,
        super::TlsOptions,
        Box<dyn Fn(&A) -> Option<String> + Send + Sync>,
        Box<dyn Fn(&A) -> core::net::SocketAddr + Send + Sync>,
      ) -> crate::streams::StreamEndpoint<I, A, crate::tls::records::TlsRecords> =
        crate::streams::StreamEndpoint::<I, A, crate::tls::records::TlsRecords>::new;
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
    use core::net::SocketAddr;

    use crate::{EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        StreamEndpoint,
        test_support::{endpoint, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7300);
    let cfg = TlsOptions::new(test_server(), test_client());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0xAB; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
    let datagram = b"tls gossip body".to_vec();
    let on_wire = coord.encrypt_gossip(&datagram).expect("encrypt");
    assert_ne!(
      on_wire, datagram,
      "encrypted gossip differs from the plaintext datagram"
    );
    assert_eq!(
      on_wire[0],
      crate::ENCRYPTED_TAG,
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
    use core::net::SocketAddr;

    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        StreamEndpoint,
        test_support::{endpoint, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7301);
    let cfg = TlsOptions::new(test_server(), test_client());
    let coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());
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
    use core::net::SocketAddr;

    use crate::{EncryptionError, EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        StreamEndpoint,
        test_support::{endpoint, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7302);
    let cfg = TlsOptions::new(test_server(), test_client());
    let kr = Keyring::new(SecretKey::ChaCha20Poly1305([0x42; 32]));
    let opts = EncryptionOptions::new().with_keyring(kr);
    let coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
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
    use core::net::SocketAddr;

    use crate::{
      EncryptionOptions, FrameError, Keyring, MessageTag, SecretKey, encode_plain_frame,
    };
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        StreamEndpoint,
        test_support::{endpoint, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let ep = endpoint(7303);
    let cfg = TlsOptions::new(test_server(), test_client());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
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
    use core::net::SocketAddr;

    use crate::{ENCRYPTED_WRAPPER_OVERHEAD, EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      config::EndpointConfig,
      endpoint::Endpoint,
      streams::{
        StreamEndpoint,
        test_support::{addr, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let cfg_ep = EndpointConfig::new(SmolStr::new("local"), addr(7304)).with_gossip_mtu(1200);
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_ep);
    let cfg = TlsOptions::new(test_server(), test_client());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
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
    use core::net::SocketAddr;

    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      config::EndpointConfig,
      endpoint::Endpoint,
      streams::{
        StreamEndpoint,
        test_support::{addr, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let cfg_ep = EndpointConfig::new(SmolStr::new("local"), addr(7305)).with_gossip_mtu(1200);
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_ep);
    let cfg = TlsOptions::new(test_server(), test_client());
    let coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());
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
    use crate::Instant;
    use core::net::SocketAddr;

    use crate::{EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        StreamEndpoint,
        test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let now = Instant::now();
    let ep = endpoint(7306);
    let cfg = TlsOptions::new(test_server(), test_client());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

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
    use crate::Instant;
    use core::net::SocketAddr;

    use crate::{EncryptionOptions, Keyring, SecretKey};
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        StreamEndpoint,
        test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    let now = Instant::now();
    let ep = endpoint(7308);
    let cfg = TlsOptions::new(test_server(), test_client());
    let key = SecretKey::Aes256([0x42; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts.clone());

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

  /// Regression: an outbound dial whose `R::dial_context` rejects the per-peer
  /// SNI must drop its `pending_outbound_kinds` entry on the failure path.
  /// `TlsRecords::dial_context` requires a non-`None` `server_name` and
  /// returns `Err` otherwise; sustained TLS SNI failures (e.g. a
  /// misconfigured per-peer SAN deployment) would otherwise accumulate
  /// entries in `pending_outbound_kinds` unbounded.
  #[test]
  fn dial_context_failure_does_not_leak_pending_outbound_kinds() {
    use crate::Instant;
    use core::net::SocketAddr;

    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      event::PushPullKind,
      streams::{
        StreamEndpoint,
        test_support::{addr, endpoint, test_peer_to_socket},
      },
      tls::options::tests::{test_client, test_server},
    };

    let now = Instant::now();
    let ep = endpoint(7310);
    let cfg = TlsOptions::new(test_server(), test_client());
    // SNI provider returns `None`. `TlsRecords::dial_context` rejects with
    // "tls dial_context called without server_name", which routes through the
    // pre-`ExchangeMeta` `dial_failed` path inside `service_dials`.
    let sni: Box<dyn Fn(&SocketAddr) -> Option<String> + Send + Sync> = Box::new(|_| None);
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(ep, cfg, sni, test_peer_to_socket());

    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    assert_eq!(
      coord.pending_outbound_kinds_len(),
      0,
      "pending_outbound_kinds must drain on dial_context failure",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "no bridge is allocated when dial_context rejects the per-peer SNI",
    );
  }

  /// An ESTABLISHED TLS exchange whose deadline elapses while it still owes no
  /// FIN (`fin_owed == false`) must surface NO outbound bytes — in particular
  /// not the `close_notify` the failure path queues — before its
  /// `StreamAction::Abort`. A failed exchange RSTs; it never puts a graceful
  /// alert on the wire.
  ///
  /// The leak this guards is TLS-specific. On the failure transition
  /// `StreamBridge::fail_with_retire` runs `retire_halves`, whose
  /// `R::send_close_notify()` queues a `close_notify` alert into rustls's write
  /// buffer (the send half was never closed, so the latch lets it through), then
  /// `fail` calls `R::clear_outbound()`. For plain TCP `clear_outbound` empties
  /// the record layer's outbound buffer, so a failed reap has nothing to
  /// collect; but for TLS `clear_outbound` is a documented no-op (rustls exposes
  /// no API to discard pending ciphertext), so the `close_notify` survives in
  /// rustls's `write_tls` queue. If `StreamEndpoint::reap_bridge` drained that
  /// buffer into `out_transmit` for a failed reap, the per-exchange teardown
  /// gate in [`StreamEndpoint::poll_action`] would withhold the `Abort` behind
  /// the queued alert, and a driver doing the natural "drain actions, drain
  /// transmits, repeat" loop would write the `close_notify` on the wire BEFORE
  /// the RST. The fix collects the bridge's final bytes ONLY for a clean
  /// (`BothClosed`) reap; a failed reap purges the exchange's queue and collects
  /// nothing.
  ///
  /// Setup: a bare client `TlsRecords` drives the acceptor coordinator's TLS
  /// handshake to completion with NO application data, so the acceptor's bridge
  /// reaches `Established(Active)` having sent nothing (`fin_owed == false`).
  /// One `handle_timeout` past the bridge's exchange deadline reaps it as
  /// `Failed(Timeout)` via `pump_out`'s `Done`-gate / flush-deadline path, which
  /// runs `retire_halves` and queues the `close_notify`.
  ///
  /// Mutation gate: making `reap_bridge` call `collect_bridge_transmits`
  /// unconditionally (instead of only on the clean-reap `else`) fails BOTH
  /// assertions below — the 24-byte `close_notify` ciphertext surfaces for the
  /// exchange and the `Abort` is withheld behind it.
  #[test]
  fn failed_established_tls_exchange_no_close_notify_before_abort() {
    use crate::Instant;
    use core::{net::SocketAddr, time::Duration};
    use std::sync::Arc;

    use bytes::Bytes;
    use rustls::pki_types::ServerName;
    use smol_str::SmolStr;

    use super::{TlsOptions, TlsRecords};
    use crate::{
      streams::{
        ExchangeId, StreamAction, StreamEndpoint,
        test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
      },
      tls::options::tests::{test_client, test_server},
    };

    fn action_kind(a: &StreamAction) -> &'static str {
      match a {
        StreamAction::Connect(_) => "Connect",
        StreamAction::Shutdown(_) => "Shutdown",
        StreamAction::Close(_) => "Close",
        StreamAction::Abort(_) => "Abort",
      }
    }

    let now = Instant::now();
    let acceptor_ep = endpoint(8001);
    let cfg = TlsOptions::new(test_server(), test_client());
    let mut acceptor: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> =
      StreamEndpoint::new(acceptor_ep, cfg, test_sni_provider(), test_peer_to_socket());
    let dialer_addr = addr(8000);

    // (1) The driver accepts an inbound TLS connection. The acceptor builds a
    // server-side `Handshaking` bridge bounded by `ACCEPT_HANDSHAKE_DEADLINE`.
    let exchange = acceptor.accept_connection(dialer_addr, now);

    // (2) A bare client record layer (the dialer's TLS half) handshakes with the
    // acceptor coordinator carrying NO application request — the realistic case
    // of a peer that connects and completes TLS but then stalls. Shuttle the
    // ciphertext both ways until the handshake settles; deliver NOTHING else.
    let mut client = TlsRecords::client(
      Arc::new(test_client()),
      ServerName::try_from("localhost").unwrap(),
    )
    .expect("client TlsRecords");
    for _ in 0..64 {
      let mut c_out = Vec::new();
      client.poll_transport_transmit(&mut c_out);
      if !c_out.is_empty() {
        acceptor.handle_transport_data(exchange, &c_out, false, now);
      }
      let mut s_out = Vec::new();
      while let Some((id, _peer, bytes)) = acceptor.poll_transport_transmit() {
        assert_eq!(id, exchange, "the only live exchange is the acceptor's");
        s_out.extend_from_slice(&bytes);
      }
      if !s_out.is_empty() {
        client
          .handle_transport_data(&s_out)
          .expect("the client consumes the server flight");
      }
      // Drain the acceptor's pre-failure teardown queue (there is none yet) so
      // a stray action cannot mask the post-failure assertion below.
      while acceptor.poll_action().is_some() {}
      if c_out.is_empty() && s_out.is_empty() {
        break;
      }
    }

    // Pre-failure invariant: the acceptor's bridge is Established and has sent
    // nothing, so the deadline failure's `retire_halves` WILL queue a
    // `close_notify` (the send half is not yet retired). If this regressed to a
    // half-closed or already-failed bridge the test would no longer exercise the
    // `close_notify`-leak path.
    assert_eq!(
      acceptor.bridge_is_established_pre_fin(exchange),
      Some(true),
      "the acceptor reached Established(Active) with no FIN owed — the state \
       whose deadline failure queues a close_notify",
    );
    assert!(
      !acceptor.exchange_has_pending_bytes(exchange),
      "the established acceptor has drained its handshake ciphertext; no bytes \
       are queued for the exchange before it fails",
    );

    // (3) The exchange deadline elapses. The default `stream_timeout` is 10s and
    // the bridge's exchange deadline (snapshotted at promotion) is `now + 10s`,
    // so `now + 11s` is strictly past it. `pump_bridges` fails the bridge to
    // `Failed(Timeout)` (running `retire_halves` -> `send_close_notify`, which
    // queues the `close_notify` into rustls's write buffer) and `reap_bridge`
    // emits its teardown.
    let deadline_elapsed = now + Duration::from_secs(11);
    acceptor.handle_timeout(deadline_elapsed);

    // (4) Load-bearing assertion A: BEFORE draining any transmits, the `Abort`
    // is available immediately — it is NOT withheld behind a queued
    // `close_notify`. Were the alert collected into `out_transmit`, the gate
    // would see it and `poll_action` would return `None` here (the bytes must be
    // drained first).
    assert!(
      !acceptor.exchange_has_pending_bytes(exchange),
      "a failed reap must surface NO bytes for the exchange — the close_notify \
       the failure path queued must NOT be collected into out_transmit",
    );
    let first = acceptor.poll_action();
    let aborted = first
      .as_ref()
      .and_then(StreamAction::as_abort)
      .is_some_and(|r| r.id() == exchange);
    assert!(
      aborted,
      "the failed exchange's Abort must surface immediately (not withheld \
       behind a collected close_notify); first post-failure action was {:?}",
      first.as_ref().map(action_kind),
    );

    // (5) Load-bearing assertion B: a full natural drain loop surfaces NO bytes
    // tagged with the failed exchange (the close_notify never reaches the wire).
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = acceptor.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = acceptor.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(id, b)| (*id == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "NO outbound bytes (notably the TLS close_notify) may surface for the \
       failed exchange {exchange:?}; got {stale:?}",
    );
    // No teardown OTHER than the already-consumed Abort exists for the exchange:
    // a clean `Close` must never be emitted for a failed reap.
    assert!(
      actions_observed
        .iter()
        .all(|a| a.as_close().is_none_or(|r| r.id() != exchange)),
      "a failed reap emits Abort, never Close, for the exchange; got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }
}
