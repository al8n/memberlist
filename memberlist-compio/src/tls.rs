//! TLS-backed memberlist driver.
//!
//! [`TlsTransport`] owns the bound `UdpSocket` (gossip) and `TcpListener`
//! (reliable; TLS handshake-on-accept); the machine-layer
//! `StreamEndpoint<I, SocketAddr, TlsRecords>` is built inside
//! [`TlsTransport::run`] from the stored `TlsTransportOptions` (cert/key
//! bundle + SNI provider). Construct via
//! [`Memberlist::new`](crate::Memberlist::new); [`TlsMemberlist`] is the
//! pinned-alias convenience shape.
//!
//! ## Server name
//!
//! TLS requires a server name to verify the peer's certificate against. The
//! `sni_provider` closure on [`TlsTransportOptions`] is called per dial with
//! the peer's membership address; it must return `Some(name)` matching the
//! peer cert's SAN/CN. Returning `None` causes the dial to fail with
//! `"tls bridge returned None for server_name"`. The default closure returns
//! `Some("localhost".to_string())` for every peer — matches the bundled smoke
//! tests' self-signed localhost-SAN certs. Production operators supply a
//! closure mapping each peer to its actual DNS name or SAN.

#![cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]

use std::net::SocketAddr;

use compio::net::{TcpListener, UdpSocket};
use hostaddr::HostAddr;
use memberlist_proto::{
  TlsOptions, TlsRecords, config::EndpointConfig, endpoint::Endpoint, streams::StreamEndpoint,
};
use smol_str::SmolStr;

use crate::{
  AdvertiseAddrResolver, Delegate, Memberlist, MemberlistError, Resolver, Result,
  StreamTransportOptions, Transport, TransportRuntime,
  delegate::{BoxedAlive, BoxedMerge, VoidDelegate},
  maybe_resolved::MaybeResolved,
};

/// TLS-backed [`Memberlist`] alias. Defaults: `I = SmolStr`,
/// `A = HostAddr<SmolStr>`, `D = VoidDelegate<I, SocketAddr>`.
pub type TlsMemberlist<I = SmolStr, A = HostAddr<SmolStr>, D = VoidDelegate<I, SocketAddr>> =
  Memberlist<TlsTransport<I, A>, D>;

/// Boxed SNI provider closure: maps a peer's `SocketAddr` to the expected TLS
/// server name string used for certificate verification. Returns `None` to
/// abort the dial before the handshake.
pub type SniProvider = Box<dyn Fn(&SocketAddr) -> Option<String> + Send + Sync>;

/// Per-backend TLS-specific transport options.
///
/// Embedded into `Options<TlsTransport<I, A>>::transport()`. Bundles the
/// local identifier, the (possibly-unresolved) advertise address, the
/// stream-transport tuning knobs, the per-peer SNI provider closure (config
/// source migrated from the machine-layer `StreamEndpoint::new` callers),
/// and the machine-layer `TlsOptions` bundle (cert/key/verifier).
pub struct TlsTransportOptions<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: Option<I>,
  advertise_addr: Option<MaybeResolved<A, SocketAddr>>,
  stream: StreamTransportOptions,
  sni_provider: SniProvider,
  tls_options: Option<TlsOptions>,
}

impl<I, A> TlsTransportOptions<I, A> {
  /// Construct with defaults. Caller MUST chain `with_local_id`,
  /// `with_advertise_addr`, and `with_tls_options` before passing to
  /// [`TlsTransport::new`]. The default `sni_provider` returns
  /// `Some("localhost".to_string())` for every peer — matches the bundled
  /// smoke tests' self-signed localhost-SAN certs.
  #[inline]
  pub fn new() -> Self {
    Self {
      local_id: None,
      advertise_addr: None,
      stream: StreamTransportOptions::new(),
      sni_provider: Box::new(|_addr: &SocketAddr| Some("localhost".to_string())),
      tls_options: None,
    }
  }

  /// Builder: local node identifier.
  #[must_use]
  #[inline]
  pub fn with_local_id(mut self, id: I) -> Self {
    self.local_id = Some(id);
    self
  }

  /// Builder: advertise address (resolved or unresolved).
  #[must_use]
  #[inline]
  pub fn with_advertise_addr(mut self, addr: MaybeResolved<A, SocketAddr>) -> Self {
    self.advertise_addr = Some(addr);
    self
  }

  /// Builder: stream-transport tuning knobs.
  #[must_use]
  #[inline]
  pub fn with_stream(mut self, opts: StreamTransportOptions) -> Self {
    self.stream = opts;
    self
  }

  /// Builder: SNI provider closure. Default returns
  /// `Some("localhost".to_string())` for every peer; for deployments with
  /// per-peer SAN certs, supply a closure mapping each dialed `SocketAddr`
  /// to its expected SNI string. Returning `None` for a peer causes the
  /// outbound TLS dial to fail before the handshake.
  #[must_use]
  #[inline]
  pub fn with_sni_provider(mut self, f: SniProvider) -> Self {
    self.sni_provider = f;
    self
  }

  /// Builder: TLS machine options (cert/key/verifier). Must be set before
  /// [`TlsTransport::new`].
  #[must_use]
  #[inline]
  pub fn with_tls_options(mut self, opts: TlsOptions) -> Self {
    self.tls_options = Some(opts);
    self
  }

  /// Local node identifier — must be set before [`TlsTransport::new`].
  #[inline]
  pub const fn local_id(&self) -> Option<&I> {
    self.local_id.as_ref()
  }

  /// Advertise address — must be set before [`TlsTransport::new`].
  #[inline]
  pub const fn advertise_addr(&self) -> Option<&MaybeResolved<A, SocketAddr>> {
    self.advertise_addr.as_ref()
  }

  /// Stream-transport tuning knobs.
  #[inline]
  pub const fn stream(&self) -> &StreamTransportOptions {
    &self.stream
  }

  /// SNI provider closure.
  #[inline]
  pub fn sni_provider(&self) -> &(dyn Fn(&SocketAddr) -> Option<String> + Send + Sync) {
    self.sni_provider.as_ref()
  }

  /// TLS machine options bundle.
  #[inline]
  pub const fn tls_options(&self) -> Option<&TlsOptions> {
    self.tls_options.as_ref()
  }
}

impl<I, A> Default for TlsTransportOptions<I, A> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// TLS-backed memberlist transport.
///
/// Owns the bound `UdpSocket` (gossip) and `TcpListener` (reliable; TLS
/// handshake-on-accept). The machine-layer
/// `StreamEndpoint<I, SocketAddr, TlsRecords>` is built inside
/// [`TlsTransport::run`] from the stored config (`tls_options` +
/// `sni_provider`).
pub struct TlsTransport<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: I,
  local_address: MaybeResolved<A, SocketAddr>,
  advertise_socket: SocketAddr,
  gossip_socket: UdpSocket,
  tcp_listener: TcpListener,
  stream_options: StreamTransportOptions,
  sni_provider: SniProvider,
  tls_options: TlsOptions,
}

impl<I, A> Transport for TlsTransport<I, A>
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: Clone + Send + 'static,
{
  type Error = MemberlistError;
  type Id = I;
  type Address = A;
  type Options = TlsTransportOptions<I, A>;

  async fn new<RES, AR>(
    options: Self::Options,
    resolver: &RES,
    advertise_resolver: &AR,
  ) -> Result<Self, Self::Error>
  where
    RES: Resolver<Address = Self::Address>,
    AR: AdvertiseAddrResolver,
  {
    // Fail fast on a stream knob that would deterministically break the
    // backend (a zero `bridge_recv_buf_len` makes every bridge read return a
    // false EOF) BEFORE binding any socket. `bridge_recv_buf_len` lives in
    // `T::Options`, so it is not reachable at the generic `Memberlist::new`
    // gossip-MTU/advertise checks — validate it here at the earliest
    // stream-backend construction boundary.
    options.stream.validate()?;
    let local_id = options.local_id.ok_or_else(|| {
      MemberlistError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "local_id required",
      ))
    })?;
    let advertise_input = options.advertise_addr.ok_or_else(|| {
      MemberlistError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "advertise_addr required",
      ))
    })?;
    let tls_options = options.tls_options.ok_or_else(|| {
      MemberlistError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "tls_options required",
      ))
    })?;

    let advertise_socket = match &advertise_input {
      MaybeResolved::Resolved(s) => *s,
      MaybeResolved::Unresolved(a) => {
        let candidates = resolver
          .resolve(a)
          .await
          .map_err(|e| MemberlistError::Resolve(std::io::Error::other(e.to_string())))?;
        advertise_resolver.pick(candidates).map_err(|e| {
          MemberlistError::Resolve(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            e.to_string(),
          ))
        })?
      }
    };

    let gossip_socket = UdpSocket::bind(advertise_socket)
      .await
      .map_err(MemberlistError::Io)?;
    // Read back the actually-bound address: an ephemeral `:0` advertise
    // request resolves to a concrete OS-assigned port here, and the TCP
    // reliable listener MUST bind the same port the node gossips. Bind
    // the listener to the gossip socket's resolved address so UDP gossip
    // and TLS-over-TCP reliable share one port even when `:0` was requested.
    let advertise_socket = gossip_socket.local_addr().map_err(MemberlistError::Io)?;
    let tcp_listener = TcpListener::bind(advertise_socket)
      .await
      .map_err(MemberlistError::Io)?;

    Ok(Self {
      local_id,
      local_address: advertise_input,
      advertise_socket,
      gossip_socket,
      tcp_listener,
      stream_options: options.stream,
      sni_provider: options.sni_provider,
      tls_options,
    })
  }

  #[inline]
  fn local_id(&self) -> &Self::Id {
    &self.local_id
  }

  #[inline]
  fn local_address(&self) -> &MaybeResolved<Self::Address, SocketAddr> {
    &self.local_address
  }

  #[inline]
  fn advertise_address(&self) -> &SocketAddr {
    &self.advertise_socket
  }

  async fn run<D>(self, runtime: TransportRuntime<Self, D>)
  where
    D: Delegate<Id = Self::Id, Address = SocketAddr>,
  {
    // `Memberlist::new` is generic over `T` and cannot build the
    // record-layer-specific endpoint; build it here from `self`'s
    // stored config. TLS supplies the per-peer SNI provider; the
    // membership address IS the transport socket (`|addr| *addr`).
    let cfg = crate::options::apply_memberlist_options(
      EndpointConfig::new(self.local_id, self.advertise_socket),
      &runtime.memberlist_options,
    );
    // Install the optional machine admission predicates before the driver
    // loop starts; each `None` leaves the `Endpoint` at its admit-all
    // default. `BoxedAlive`/`BoxedMerge` forward the boxed dyn so it
    // satisfies the setters' `impl AliveDelegate`/`impl MergeDelegate` bound.
    let mut ep = Endpoint::new(cfg);
    if let Some(ad) = runtime.alive_delegate {
      ep.set_alive_delegate(BoxedAlive(ad));
    }
    if let Some(md) = runtime.merge_delegate {
      ep.set_merge_delegate(BoxedMerge(md));
    }
    let endpoint = StreamEndpoint::new(
      ep,
      self.tls_options,
      self.sni_provider,
      Box::new(|addr| *addr),
    );

    let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded();
    crate::driver::stream_driver_loop::<Self::Id, SocketAddr, TlsRecords, D>(
      endpoint,
      self.gossip_socket,
      self.tcp_listener,
      runtime.commands_rx,
      runtime.events_tx,
      runtime.events_dropped,
      runtime.observation_dropped,
      runtime.snapshot,
      bridge_ready_rx,
      bridge_ready_tx,
      runtime.shutdown_flag,
      runtime.driver_options,
      self.stream_options,
      runtime.delegate,
    )
    .await;
  }
}

#[cfg(test)]
mod transport_tests {
  use std::{net::SocketAddr, sync::Arc};

  use memberlist_proto::TlsOptions;

  use super::*;
  use crate::{FirstAddrResolver, MaybeResolved, OsResolver};

  /// Accept-any server-cert verifier for the construction test.
  ///
  /// The transport-construction test does NOT exercise the TLS handshake;
  /// the verifier just has to type-check inside a `ClientConfig`.
  #[derive(Debug)]
  struct AcceptAnyServer(Arc<rustls::crypto::CryptoProvider>);

  impl rustls::client::danger::ServerCertVerifier for AcceptAnyServer {
    fn verify_server_cert(
      &self,
      _e: &rustls::pki_types::CertificateDer<'_>,
      _i: &[rustls::pki_types::CertificateDer<'_>],
      _n: &rustls::pki_types::ServerName<'_>,
      _o: &[u8],
      _t: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
      &self,
      _m: &[u8],
      _c: &rustls::pki_types::CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
      &self,
      _m: &[u8],
      _c: &rustls::pki_types::CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }

  fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    rustls::crypto::CryptoProvider::get_default()
      .cloned()
      .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()))
  }

  /// Build a self-signed localhost-SAN `ServerConfig` + accept-any
  /// `ClientConfig`. Mirrors the existing `tls_smoke` fixture; the
  /// construction test does not perform a handshake.
  fn test_tls_options() -> TlsOptions {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()])
      .expect("rcgen generate_simple_self_signed");
    let chain = vec![rustls::pki_types::CertificateDer::from(
      ck.cert.der().to_vec(),
    )];
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());

    let provider = crypto_provider();

    let server_cfg = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .expect("TLS 1.3 supported")
      .with_no_client_auth()
      .with_single_cert(chain, key)
      .expect("valid self-signed cert");

    let client_cfg = rustls::ClientConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .expect("TLS 1.3 supported")
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(AcceptAnyServer(provider)))
      .with_no_client_auth();

    TlsOptions::new(server_cfg, client_cfg)
  }

  fn test_tls_opts() -> TlsTransportOptions {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    TlsTransportOptions::new()
      .with_local_id(smol_str::SmolStr::new("test-node"))
      .with_advertise_addr(MaybeResolved::Resolved(bind))
      .with_tls_options(test_tls_options())
  }

  #[compio::test]
  async fn new_with_resolved_advertise_skips_resolver() {
    let opts = test_tls_opts();
    let t: TlsTransport = TlsTransport::new(opts, &OsResolver, &FirstAddrResolver)
      .await
      .expect("construct TlsTransport");
    assert_eq!(t.local_id().as_str(), "test-node");
    assert!(t.local_address().is_resolved());
    let _: &SocketAddr = t.advertise_address();
  }
}
