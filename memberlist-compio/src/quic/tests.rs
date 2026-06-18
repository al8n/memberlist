use std::{net::SocketAddr, sync::Arc, time::Duration};

use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use super::*;
use crate::{FirstAddrResolver, MaybeResolved, OsResolver, SocketAddrResolver};
use rustls::version::TLS13;
use std::io::ErrorKind;

const ALPN: &[u8] = b"memberlist-quic";

/// Build a minimal valid `QuicOptions` for the construction test. Mirrors
/// the `tests/support/quic.rs` fixture; the test never performs a QUIC
/// handshake, the config just has to type-check and pass `QuicOptions::new`.
fn default_test_quic_config() -> QuicOptions {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen self-sign");
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());

  let provider = Arc::new(rustls::crypto::ring::default_provider());

  let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert.clone()], key)
    .expect("server single cert");
  rustls_server.alpn_protocols = vec![ALPN.to_vec()];

  let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
    .expect("QuicServerConfig");
  let server_cfg = ServerConfig::with_crypto(Arc::new(qsc));

  let mut roots = RootCertStore::empty();
  roots.add(cert).expect("add root cert");
  let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&TLS13])
    .expect("TLS 1.3")
    .with_root_certificates(roots)
    .with_no_client_auth();
  rustls_client.alpn_protocols = vec![ALPN.to_vec()];

  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
    .expect("QuicClientConfig");
  let client_cfg = ClientConfig::new(Arc::new(qcc));

  let reset_key = [0x5au8; 32];
  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &reset_key);
  let endpoint_cfg = EndpointConfig::new(Arc::new(hmac));

  let mut transport = TransportConfig::default();
  transport
    .max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(5)).expect("idle timeout"),
    ))
    .keep_alive_interval(Some(Duration::from_secs(1)));

  QuicOptions::new(
    endpoint_cfg,
    server_cfg,
    client_cfg,
    transport,
    "localhost",
    memberlist_proto::UnreliableTransport::Datagram,
  )
}

fn test_quic_opts() -> QuicTransportOptions {
  let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
  QuicTransportOptions::new()
    .with_local_id(SmolStr::new("test-node"))
    .with_advertise_addr(MaybeResolved::Resolved(bind))
    .with_quic_config(default_test_quic_config())
}

#[compio::test]
async fn new_with_resolved_advertise_skips_resolver() {
  let opts = test_quic_opts();
  let t: QuicTransport = QuicTransport::new(opts, &OsResolver, &FirstAddrResolver)
    .await
    .expect("construct QuicTransport");
  assert_eq!(t.local_id().as_str(), "test-node");
  assert!(t.local_address().is_resolved());
  let _: &SocketAddr = t.advertise_address();
}

/// `new` rejects a missing `local_id` with `InvalidInput` BEFORE any
/// resolution or socket bind — the field is required.
#[compio::test]
async fn new_without_local_id_errors() {
  let opts = QuicTransportOptions::<SmolStr, SocketAddr>::new()
    .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()))
    .with_quic_config(default_test_quic_config());
  let res =
    QuicTransport::<SmolStr, SocketAddr>::new(opts, &SocketAddrResolver, &FirstAddrResolver).await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("local_id"));
    }
    Err(other) => panic!("expected InvalidInput(local_id), got {other:?}"),
    Ok(_) => panic!("a missing local_id must be rejected, but construction succeeded"),
  }
}

/// `new` rejects a missing `advertise_addr` with `InvalidInput`.
#[compio::test]
async fn new_without_advertise_addr_errors() {
  let opts = QuicTransportOptions::<SmolStr, SocketAddr>::new()
    .with_local_id(SmolStr::new("no-adv"))
    .with_quic_config(default_test_quic_config());
  let res =
    QuicTransport::<SmolStr, SocketAddr>::new(opts, &SocketAddrResolver, &FirstAddrResolver).await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("advertise_addr"));
    }
    Err(other) => panic!("expected InvalidInput(advertise_addr), got {other:?}"),
    Ok(_) => panic!("a missing advertise_addr must be rejected, but construction succeeded"),
  }
}

/// `new` rejects a missing `quic_config` with `InvalidInput`.
#[compio::test]
async fn new_without_quic_config_errors() {
  let opts = QuicTransportOptions::<SmolStr, SocketAddr>::new()
    .with_local_id(SmolStr::new("no-cfg"))
    .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()));
  let res =
    QuicTransport::<SmolStr, SocketAddr>::new(opts, &SocketAddrResolver, &FirstAddrResolver).await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("quic_config"));
    }
    Err(other) => panic!("expected InvalidInput(quic_config), got {other:?}"),
    Ok(_) => panic!("a missing quic_config must be rejected, but construction succeeded"),
  }
}
