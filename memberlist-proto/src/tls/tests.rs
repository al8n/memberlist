use crate::{
  streams::{Labeled, StreamEndpoint},
  tls::records::TlsRecords,
};
use core::{fmt, net::SocketAddr};
use rustls::pki_types::ServerName;
use std::sync::Arc;
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
    I: crate::Id,
    A: crate::Data
      + crate::CheapClone
      + Eq
      + core::hash::Hash
      + fmt::Debug
      + fmt::Display
      + Send
      + Sync
      + 'static,
  {
    // The reliable transport is the label decorator over the TLS record
    // layer; its options bundle is `LabelOptions<TlsOptions>` (the label
    // rides `LabelOptions`, never `TlsOptions`).
    let _: fn(
      crate::endpoint::Endpoint<I, A>,
      crate::streams::LabelOptions<super::TlsOptions>,
      Box<dyn Fn(&A) -> Option<String> + Send + Sync>,
      Box<dyn Fn(&A) -> SocketAddr + Send + Sync>,
    ) -> StreamEndpoint<I, A, Labeled<TlsRecords>> =
      StreamEndpoint::<I, A, Labeled<TlsRecords>>::new;
  }
}

/// The TLS coordinator's GOSSIP path still encrypts when configured — gossip
/// is plain UDP regardless of the reliable transport, so only the reliable
/// path skips its inner Encrypted wrapper (TLS already wraps it). The gossip
/// datagram is exchanged on a separate socket and needs its own
/// confidentiality envelope.
#[cfg(feature = "aes-gcm")]
#[test]
fn tls_endpoint_gossip_encryption_roundtrip() {
  use SocketAddr;

  use crate::{EncryptionOptions, Keyring, SecretKey};
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let ep = endpoint(7300);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0xAB; 32])));
  let coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
    StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket()).with_encryption(opts);
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
  use SocketAddr;

  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let ep = endpoint(7301);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
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
#[cfg(all(feature = "aes-gcm", not(feature = "chacha20-poly1305")))]
#[test]
fn tls_endpoint_encrypt_gossip_returns_err_on_unsupported_backend() {
  use SocketAddr;

  use crate::{EncryptionError, EncryptionOptions, Keyring, SecretKey};
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let ep = endpoint(7302);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let kr = Keyring::new(SecretKey::ChaCha20Poly1305([0x42; 32]));
  let opts = EncryptionOptions::new().with_keyring(kr);
  let coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
    StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket()).with_encryption(opts);
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
#[cfg(feature = "aes-gcm")]
#[test]
fn tls_endpoint_decrypt_gossip_rejects_plaintext_when_encryption_enabled() {
  use SocketAddr;

  use crate::{EncryptionOptions, FrameError, Keyring, MessageTag, SecretKey, encode_plain_frame};
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let ep = endpoint(7303);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
  let coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
    StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket()).with_encryption(opts);
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
#[cfg(feature = "aes-gcm")]
#[test]
fn tls_endpoint_encrypted_gossip_wire_bytes_within_configured_mtu_plus_overhead() {
  use SocketAddr;

  use crate::{ENCRYPTED_WRAPPER_OVERHEAD, EncryptionOptions, Keyring, SecretKey};
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    config::EndpointOptions,
    endpoint::Endpoint,
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{addr, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let cfg_ep = EndpointOptions::new(SmolStr::new("local"), addr(7304)).with_gossip_mtu(1200);
  let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg_ep);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
  let coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
    StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket()).with_encryption(opts);
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

/// `EndpointOptions::with_gossip_mtu(n)` MUST be readable on the
/// constructed coordinator via `gossip_mtu()`. No encryption is exercised
/// — pure config plumbing.
#[test]
fn tls_endpoint_gossip_mtu_is_propagated_from_config() {
  use SocketAddr;

  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    config::EndpointOptions,
    endpoint::Endpoint,
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{addr, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let cfg_ep = EndpointOptions::new(SmolStr::new("local"), addr(7305)).with_gossip_mtu(1200);
  let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg_ep);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
    StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());
  assert_eq!(
    coord.gossip_mtu(),
    1200,
    "StreamEndpoint::gossip_mtu must read the configured value (not the \
       legacy 1400 constant) so the FSM's plaintext-budget bound tracks \
       EndpointOptions",
  );
}

/// A runtime policy change MUST purge the `mem_ingress` queue. A datagram
/// queued under the old policy would otherwise survive across the flip and
/// be surfaced by the next `poll_memberlist_ingress` under the new strict-
/// mode regime it was never validated against.
#[cfg(feature = "aes-gcm")]
#[test]
fn tls_endpoint_set_encryption_options_purges_buffered_gossip_on_policy_change() {
  use crate::Instant;
  use SocketAddr;

  use crate::{EncryptionOptions, Keyring, SecretKey};
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let now = Instant::now();
  let ep = endpoint(7306);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let mut coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
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
#[cfg(feature = "aes-gcm")]
#[test]
fn tls_endpoint_set_encryption_options_is_noop_when_reapplying_same_policy() {
  use crate::Instant;
  use SocketAddr;

  use crate::{EncryptionOptions, Keyring, SecretKey};
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let now = Instant::now();
  let ep = endpoint(7308);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let key = SecretKey::Aes256([0x42; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let mut coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
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

/// A runtime `set_encryption_options` change with a LIVE TLS bridge reaches
/// that bridge's `set_encryption`, which — because `TlsRecords::is_secure()`
/// is `true` — resets the bridge's reliable-plane encryption to disabled
/// rather than failing it: TLS already provides confidentiality, so the
/// reliable path never double-wraps. The bridge therefore survives the policy
/// change (no `EncryptionPolicyChanged` cascade, unlike plain TCP).
#[cfg(feature = "aes-gcm")]
#[test]
fn tls_endpoint_set_encryption_options_keeps_live_bridge_via_is_secure() {
  use crate::Instant;
  use SocketAddr;

  use crate::{EncryptionOptions, Keyring, SecretKey};
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let now = Instant::now();
  let ep = endpoint(7310);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let mut coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
    StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

  // A live inbound TLS bridge (still Handshaking — the TLS handshake has not
  // run, but the bridge exists in `conns`).
  let _exchange = coord
    .accept_connection(addr(7311), now)
    .expect("test: connection admitted");
  assert_eq!(coord.live_bridge_count(), 1);

  // Publish an enabling encryption policy. For the TLS bridge, `set_encryption`
  // hits the `is_secure()` branch: it disables the reliable-plane encryption
  // and does NOT fail the bridge.
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x55; 32])));
  coord.set_encryption_options(opts.clone());

  assert_eq!(
    coord.live_bridge_count(),
    1,
    "a TLS bridge survives the encryption-policy change (is_secure resets, \
       never fails)",
  );
  assert_eq!(
    coord.encryption_options(),
    &opts,
    "the coordinator still publishes the new gossip-path policy",
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
  use SocketAddr;

  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    event::PushPullKind,
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket},
    },
    tls::options::tests::{test_client, test_server},
  };

  let now = Instant::now();
  let ep = endpoint(7310);
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  // SNI provider returns `None`. `TlsRecords::dial_context` rejects with
  // "tls dial_context called without server_name", which routes through the
  // pre-`ExchangeMeta` `dial_failed` path inside `service_dials`.
  let sni: Box<dyn Fn(&SocketAddr) -> Option<String> + Send + Sync> = Box::new(|_| None);
  let mut coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
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
  use Arc;
  use core::{net::SocketAddr, time::Duration};

  use ServerName;
  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      ExchangeId, LabelOptions, Labeled, StreamAction, StreamEndpoint,
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
  let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
  let mut acceptor: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
    StreamEndpoint::new(acceptor_ep, cfg, test_sni_provider(), test_peer_to_socket());
  let dialer_addr = addr(8000);

  // (1) The driver accepts an inbound TLS connection. The acceptor builds a
  // server-side `Handshaking` bridge bounded by `ACCEPT_HANDSHAKE_DEADLINE`.
  let exchange = acceptor
    .accept_connection(dialer_addr, now)
    .expect("test: connection admitted");

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

// ── cluster label on the TLS reliable plane (Labeled<TlsRecords>) ──────────

/// Two coordinators carrying the SAME cluster label complete a reliable
/// push/pull over `Labeled<TlsRecords>`: the in-TLS label validates on both
/// sides, the dialer's `Stream` mints, the acceptor's `Stream` mints AFTER
/// its inbound label validates, and the membership state merges in BOTH
/// directions (the dialer learns the acceptor and the acceptor learns the
/// dialer purely through the labeled TLS exchange — no gossip).
///
/// This is the TLS analogue of the plain-TCP two-node label join: the label
/// rides `LabelOptions` ahead of the first application record inside the TLS
/// session, and the generic [`crate::streams::Labeled`] decorator strips +
/// validates it before any membership data reaches the FSM.
#[test]
fn tls_two_same_label_nodes_complete_a_reliable_exchange() {
  use crate::Instant;
  use core::{net::SocketAddr, time::Duration};

  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    event::PushPullKind,
    streams::{
      ExchangeId, LabelOptions, Labeled, StreamAction, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let mut now = Instant::now();
  let dialer_addr = addr(8100);
  let acceptor_addr = addr(8101);

  // Both nodes share the cluster label `cluster-x`. The label lives on
  // `LabelOptions`; `TlsOptions` is untouched.
  let label = || Some(b"cluster-x".to_vec());
  let mut dialer: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> = StreamEndpoint::new(
    endpoint(dialer_addr.port()),
    LabelOptions::new_in(label(), TlsOptions::new(test_server(), test_client())),
    test_sni_provider(),
    test_peer_to_socket(),
  );
  let mut acceptor: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> = StreamEndpoint::new(
    endpoint(acceptor_addr.port()),
    LabelOptions::new_in(label(), TlsOptions::new(test_server(), test_client())),
    test_sni_provider(),
    test_peer_to_socket(),
  );

  // The dialer initiates a Join push/pull against the acceptor's address.
  let _sid = dialer.start_push_pull(acceptor_addr, PushPullKind::Join, now);

  // Drive both coordinators until the membership converges. Each iteration:
  // tick both, translate a dialer `Connect` into the acceptor's
  // `accept_connection`, shuttle per-exchange ciphertext both ways, and
  // translate a `Shutdown`/`Close` teardown into the peer's transport EOF.
  let mut dialer_ex: Option<ExchangeId> = None;
  let mut acceptor_ex: Option<ExchangeId> = None;
  for _ in 0..256 {
    dialer.handle_timeout(now);
    acceptor.handle_timeout(now);

    // Dialer actions: the first `Connect` opens the acceptor's inbound
    // exchange. Teardowns become an EOF delivered to the acceptor.
    while let Some(action) = dialer.poll_action() {
      match action {
        StreamAction::Connect(info) => {
          dialer_ex = Some(info.id());
          if acceptor_ex.is_none() {
            acceptor_ex = Some(
              acceptor
                .accept_connection(dialer_addr, now)
                .expect("test: connection admitted"),
            );
          }
        }
        StreamAction::Shutdown(r) | StreamAction::Close(r) => {
          if Some(r.id()) == dialer_ex {
            if let Some(ax) = acceptor_ex {
              acceptor.handle_transport_data(ax, &[], true, now);
            }
          }
        }
        StreamAction::Abort(_) => panic!("the labeled dialer must not abort a same-label peer"),
      }
    }
    // Acceptor actions: it never dials; its teardown becomes an EOF to the
    // dialer.
    while let Some(action) = acceptor.poll_action() {
      match action {
        StreamAction::Connect(_) => panic!("the acceptor never dials"),
        StreamAction::Shutdown(r) | StreamAction::Close(r) => {
          if Some(r.id()) == acceptor_ex {
            if let Some(dx) = dialer_ex {
              dialer.handle_transport_data(dx, &[], true, now);
            }
          }
        }
        StreamAction::Abort(_) => panic!("the labeled acceptor must not abort a same-label peer"),
      }
    }

    // Shuttle the dialer's per-exchange ciphertext to the acceptor.
    if let Some(ax) = acceptor_ex {
      let mut to_acceptor = Vec::new();
      while let Some((id, _peer, bytes)) = dialer.poll_transport_transmit() {
        if Some(id) == dialer_ex {
          to_acceptor.extend_from_slice(&bytes);
        }
      }
      if !to_acceptor.is_empty() {
        acceptor.handle_transport_data(ax, &to_acceptor, false, now);
      }
    }
    // Shuttle the acceptor's per-exchange ciphertext to the dialer.
    if let Some(dx) = dialer_ex {
      let mut to_dialer = Vec::new();
      while let Some((id, _peer, bytes)) = acceptor.poll_transport_transmit() {
        if Some(id) == acceptor_ex {
          to_dialer.extend_from_slice(&bytes);
        }
      }
      if !to_dialer.is_empty() {
        dialer.handle_transport_data(dx, &to_dialer, false, now);
      }
    }

    // Drain any application events the coordinators surface (membership
    // notices) so their queues do not back up.
    while dialer.poll_event().is_some() {}
    while acceptor.poll_event().is_some() {}

    if dialer.endpoint_ref().num_members() >= 2 && acceptor.endpoint_ref().num_members() >= 2 {
      break;
    }
    now += Duration::from_millis(5);
  }

  // Both sides learned the other purely through the labeled TLS push/pull.
  assert!(
    dialer.endpoint_ref().num_members() >= 2,
    "the dialer merged the acceptor's state over the labeled TLS exchange \
       (num_members={})",
    dialer.endpoint_ref().num_members(),
  );
  assert!(
    acceptor.endpoint_ref().num_members() >= 2,
    "the acceptor merged the dialer's push over the labeled TLS exchange \
       (num_members={})",
    acceptor.endpoint_ref().num_members(),
  );
}

/// A labeled acceptor rejects a peer that completes the TLS handshake and
/// then presents a MISMATCHED in-TLS cluster label: the bridge fails BEFORE
/// any `Stream` is minted, so no membership is merged and the coordinator
/// surfaces an `Abort` (RST). The label is decrypted plaintext consumed by
/// the [`crate::streams::Labeled`] gate before the mint — the wrong-cluster
/// payload that follows the label never reaches the FSM.
#[test]
fn tls_acceptor_rejects_mismatched_inbound_label_before_mint() {
  use crate::Instant;
  use Arc;
  use SocketAddr;

  use ServerName;
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamAction, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let now = Instant::now();
  let dialer_addr = addr(8110);
  let mut acceptor: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> = StreamEndpoint::new(
    endpoint(8111),
    LabelOptions::new_in(
      Some(b"cluster-x".to_vec()),
      TlsOptions::new(test_server(), test_client()),
    ),
    test_sni_provider(),
    test_peer_to_socket(),
  );

  let exchange = acceptor
    .accept_connection(dialer_addr, now)
    .expect("test: connection admitted");

  // A bare client TLS half drives the handshake to completion, then sends a
  // WRONG-cluster label `[12][7][other-x]` followed by a payload — all as TLS
  // application data inside the established session.
  let mut client = TlsRecords::client(
    Arc::new(test_client()),
    ServerName::try_from("localhost").unwrap(),
  )
  .expect("client TlsRecords");

  // First, complete the TLS handshake (no application data yet).
  let mut handshook = false;
  for _ in 0..64 {
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      acceptor.handle_transport_data(exchange, &c_out, false, now);
    }
    let mut s_out = Vec::new();
    while let Some((id, _peer, bytes)) = acceptor.poll_transport_transmit() {
      if id == exchange {
        s_out.extend_from_slice(&bytes);
      }
    }
    if !s_out.is_empty() {
      client
        .handle_transport_data(&s_out)
        .expect("client consumes the server flight");
    }
    while acceptor.poll_action().is_some() {}
    if !client.is_handshaking() && c_out.is_empty() && s_out.is_empty() {
      handshook = true;
      break;
    }
  }
  assert!(handshook, "the client TLS handshake completed");

  // Then the client encrypts a MISMATCHED label header + payload and
  // delivers it to the acceptor as one TLS record flight.
  let mut wrong = vec![12u8, 7];
  wrong.extend_from_slice(b"other-x");
  wrong.extend_from_slice(b"push-pull-request-bytes");
  client.write_plaintext(&wrong);
  let mut c_app = Vec::new();
  client.poll_transport_transmit(&mut c_app);
  assert!(!c_app.is_empty(), "the client produced encrypted app data");
  acceptor.handle_transport_data(exchange, &c_app, false, now);

  // Drive the reap and observe: an `Abort` surfaces, NO `Stream` was minted
  // (the bridge never reached `Established(Active)`), the bridge is gone, and
  // no membership was merged (only the local node).
  acceptor.handle_timeout(now);
  let mut saw_abort = false;
  let mut saw_close_or_shutdown = false;
  while let Some(action) = acceptor.poll_action() {
    match action {
      StreamAction::Abort(r) if r.id() == exchange => saw_abort = true,
      StreamAction::Close(r) | StreamAction::Shutdown(r) if r.id() == exchange => {
        saw_close_or_shutdown = true;
      }
      _ => {}
    }
  }
  assert!(
    saw_abort,
    "a mismatched in-TLS label must fail the exchange with an Abort (RST)",
  );
  assert!(
    !saw_close_or_shutdown,
    "a rejected exchange must not emit a graceful Close/Shutdown",
  );
  assert_eq!(
    acceptor.live_bridge_count(),
    0,
    "the rejected bridge was reaped; no Stream was minted",
  );
  assert_eq!(
    acceptor.endpoint_ref().num_members(),
    1,
    "no membership merged from a wrong-cluster peer (only the local node)",
  );
}

/// TLS leak-hygiene: a labeled acceptor sends NO in-TLS application data —
/// in particular NOT its own `[12][len][label]` prefix — to a peer that has
/// completed the TLS handshake but has NOT yet presented a valid inbound
/// label. The acceptor's outbound label is queued LAZILY by the
/// [`crate::streams::Labeled`] gate at inbound-label validation, so a
/// wrong-cluster / slow-loris peer that only finishes the TLS handshake
/// never learns the local cluster identity. The TLS analogue of the
/// plain-TCP `failed_acceptor_label_mismatch_no_bytes_before_close` /
/// `accept_connection_then_handle_timeout_no_bytes_before_inbound_validation`
/// leak invariants.
///
/// Distinguishing the in-TLS label from the TLS handshake flights is the
/// crux: after the handshake settles the client drains the acceptor's
/// outbound and decrypts it; the acceptor must surface ZERO decrypted
/// application plaintext (the label) until the client sends its own valid
/// label.
#[test]
fn tls_acceptor_discloses_no_label_before_inbound_validates() {
  use crate::Instant;
  use Arc;
  use SocketAddr;

  use ServerName;
  use smol_str::SmolStr;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
    tls::options::tests::{test_client, test_server},
  };

  let now = Instant::now();
  let dialer_addr = addr(8120);
  let mut acceptor: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> = StreamEndpoint::new(
    endpoint(8121),
    LabelOptions::new_in(
      Some(b"cluster-x".to_vec()),
      TlsOptions::new(test_server(), test_client()),
    ),
    test_sni_provider(),
    test_peer_to_socket(),
  );

  let exchange = acceptor
    .accept_connection(dialer_addr, now)
    .expect("test: connection admitted");

  let mut client = TlsRecords::client(
    Arc::new(test_client()),
    ServerName::try_from("localhost").unwrap(),
  )
  .expect("client TlsRecords");

  // Complete the TLS handshake, draining every decrypted plaintext byte the
  // client surfaces along the way. The acceptor must NEVER surface decrypted
  // application plaintext (its label) before the client presents one.
  let mut leaked_plaintext: Vec<u8> = Vec::new();
  for _ in 0..64 {
    acceptor.handle_timeout(now);
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      acceptor.handle_transport_data(exchange, &c_out, false, now);
    }
    let mut s_out = Vec::new();
    while let Some((id, _peer, bytes)) = acceptor.poll_transport_transmit() {
      if id == exchange {
        s_out.extend_from_slice(&bytes);
      }
    }
    if !s_out.is_empty() {
      client
        .handle_transport_data(&s_out)
        .expect("client consumes the server flight");
      // Any decrypted application plaintext here is the acceptor's label
      // leaking pre-validation — the property under test forbids it.
      client.read_plaintext(&mut leaked_plaintext);
    }
    while acceptor.poll_action().is_some() {}
    if !client.is_handshaking() && c_out.is_empty() && s_out.is_empty() {
      break;
    }
  }
  assert!(
    !client.is_handshaking(),
    "the client TLS handshake completed"
  );

  // One more pump round after the handshake to force any buffered acceptor
  // output to the client; still NO application plaintext may appear.
  for _ in 0..4 {
    acceptor.handle_timeout(now);
    let mut s_out = Vec::new();
    while let Some((id, _peer, bytes)) = acceptor.poll_transport_transmit() {
      if id == exchange {
        s_out.extend_from_slice(&bytes);
      }
    }
    if !s_out.is_empty() {
      client
        .handle_transport_data(&s_out)
        .expect("client consumes");
      client.read_plaintext(&mut leaked_plaintext);
    }
    while acceptor.poll_action().is_some() {}
  }

  assert!(
    leaked_plaintext.is_empty(),
    "a labeled acceptor must disclose NO in-TLS application bytes (its \
       cluster label) before the peer's inbound label validates; leaked {:?}",
    leaked_plaintext,
  );
  assert_eq!(
    acceptor.live_bridge_count(),
    1,
    "the acceptor is still waiting for the inbound label (bridge alive, no \
       Stream minted yet)",
  );
}

/// Invariant-4 on TLS: an UNLABELED node passes a 12-byte first reliable unit
/// (whose `[unit_len]` prefix byte is `0x0C` == `LABELED_TAG`) straight
/// through as decrypted plaintext rather than misclassifying the leading byte
/// as an in-TLS cluster-label tag. With no local label the
/// [`crate::streams::Labeled`] gate is a pure passthrough from byte 0, so the
/// 12-byte unit is surfaced verbatim and the exchange is NOT rejected at the
/// label gate.
///
/// Driven directly on the `Labeled<TlsRecords>` record layer (the acceptor
/// side of a real rustls handshake against a bare client peer): once the TLS
/// handshake settles, the peer encrypts a `[0x0C][12 payload bytes]` frame
/// and the unlabeled decorator must surface it byte-for-byte — never an
/// `Intake::Failed` from treating `0x0C` as a `LABELED_TAG`. This is the TLS
/// analogue of the plain-TCP `unlabeled_acceptor_passes_through_a_12_byte_first_unit`
/// latent-bug regression.
#[test]
fn tls_unlabeled_node_passes_through_a_12_byte_first_unit() {
  use crate::Instant;
  use Arc;

  use ServerName;

  use super::{TlsOptions, TlsRecords};
  use crate::{
    streams::{Intake, LabelOptions, Labeled, StreamTransport},
    tls::options::tests::{test_client, test_server},
  };

  // Unlabeled acceptor record layer: the label gate is a pure passthrough
  // from byte 0 (no mint gate, no `[12]` classifier on the stream).
  let mut records = <Labeled<TlsRecords>>::acceptor(&LabelOptions::new_in(
    None,
    TlsOptions::new(test_server(), test_client()),
  ))
  .expect("unlabeled acceptor records");
  assert!(
    <Labeled<TlsRecords>>::is_secure(),
    "the TLS reliable transport is secure"
  );

  // Bare client peer to drive a real TLS handshake against `records`.
  let mut peer = TlsRecords::client(
    Arc::new(test_client()),
    ServerName::try_from("localhost").unwrap(),
  )
  .expect("peer client");
  for _ in 0..64 {
    let mut p_out = Vec::new();
    peer.poll_transport_transmit(&mut p_out);
    if !p_out.is_empty() {
      assert!(
        !matches!(
          records.handle_transport_data(&p_out, Instant::now()),
          Intake::Failed
        ),
        "no label reject may occur while the TLS handshake is in flight",
      );
    }
    let mut r_out = Vec::new();
    records.poll_transport_transmit(&mut r_out);
    if !r_out.is_empty() {
      peer
        .handle_transport_data(&r_out)
        .expect("peer consumes server flight");
    }
    if p_out.is_empty() && r_out.is_empty() {
      break;
    }
  }
  assert!(
    !records.is_handshaking(),
    "the unlabeled TLS record layer settled (handshake done, no label gate)"
  );

  // The peer encrypts a 12-byte reliable unit `[0x0C][12 payload bytes]`. The
  // leading `0x0C` is the unit-length prefix, NOT a label tag.
  let mut first_unit = vec![12u8];
  first_unit.extend_from_slice(b"ABCDEFGHIJKL");
  peer.write_plaintext(&first_unit);
  let mut enc = Vec::new();
  peer.poll_transport_transmit(&mut enc);
  assert!(!enc.is_empty(), "the peer produced encrypted app data");

  assert!(
    !matches!(
      records.handle_transport_data(&enc, Instant::now()),
      Intake::Failed
    ),
    "an unlabeled node must NOT classify the 0x0C-led first unit as a label \
       tag — no Intake::Failed at the gate",
  );
  let mut got = Vec::new();
  records.read_plaintext(&mut got);
  assert_eq!(
    got, first_unit,
    "the 0x0C-led 12-byte unit surfaced verbatim as decrypted plaintext (no \
       label classification)",
  );
}
