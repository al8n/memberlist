//! QUIC-backed memberlist driver.
//!
//! [`QuicTransport`] owns the bound `UdpSocket`; QUIC carries no separate
//! TCP listener — quinn-proto multiplexes streams over the single UDP
//! socket. The machine-layer `QuicEndpoint<I>` is built inside
//! [`QuicTransport::run`] from the stored [`QuicConfig`]. Construct via
//! [`Memberlist::new`](crate::Memberlist::new); [`QuicMemberlist`] is the
//! pinned-alias convenience shape.
//!
//! ## TLS server name
//!
//! QUIC's TLS 1.3 handshake requires a server name to verify the peer's
//! certificate against. [`QuicConfig::new`] installs a cluster-uniform
//! string used for every peer (see the `quic-rustls` test setups in
//! `memberlist-simulation` for the `"localhost"` SAN convention);
//! deployments whose certs name each peer's hostname/IP supply a per-peer
//! SNI closure via [`QuicConfig::new_with_sni_provider`] that maps each
//! dialed `SocketAddr` to its expected verification identity.

#![cfg(feature = "quic")]

use std::net::SocketAddr;

use compio::net::UdpSocket;
use hostaddr::HostAddr;
use memberlist_proto::{QuicEndpoint, config::EndpointConfig, endpoint::Endpoint};
use smol_str::SmolStr;

use crate::{
  AdvertiseAddrResolver, Delegate, Memberlist, MemberlistError, Resolver, Result, Transport,
  TransportRuntime,
  delegate::{BoxedAlive, BoxedMerge, VoidDelegate},
  maybe_resolved::MaybeResolved,
};

/// Phantom type tag identifying the QUIC backend.
///
/// `Quic` does not implement
/// [`memberlist_proto::streams::StreamTransport`] — QUIC carries no
/// stream-transport record layer; its security is intrinsic to the QUIC
/// handshake. Retained as a public backend marker; [`QuicTransport`] is
/// the type that actually drives the QUIC endpoint.
pub struct Quic;

/// QUIC config bundle handed to [`QuicTransport`]. Re-exported from
/// `memberlist-proto` so callers don't need a direct dep.
pub use memberlist_proto::QuicConfig;

/// QUIC-backed [`Memberlist`] alias. Defaults: `I = SmolStr`,
/// `A = HostAddr<SmolStr>`, `D = VoidDelegate<I, SocketAddr>`.
pub type QuicMemberlist<I = SmolStr, A = HostAddr<SmolStr>, D = VoidDelegate<I, SocketAddr>> =
  Memberlist<QuicTransport<I, A>, D>;

/// Per-backend QUIC-specific transport options.
///
/// Embedded into `Options<QuicTransport<I, A>>::transport()`. Bundles the
/// local identifier, the (possibly-unresolved) advertise address, and the
/// caller-built [`QuicConfig`] (quinn-proto `EndpointConfig` /
/// `ServerConfig` / `ClientConfig` / `TransportConfig` bundle plus SNI
/// provider).
pub struct QuicTransportOptions<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: Option<I>,
  advertise_addr: Option<MaybeResolved<A, SocketAddr>>,
  quic_config: Option<QuicConfig>,
}

impl<I, A> QuicTransportOptions<I, A> {
  /// Construct. Caller MUST chain `with_local_id`, `with_advertise_addr`,
  /// and `with_quic_config` before passing to [`QuicTransport::new`].
  #[inline]
  pub fn new() -> Self {
    Self {
      local_id: None,
      advertise_addr: None,
      quic_config: None,
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

  /// Builder: QUIC config bundle (caller-built quinn-proto configs + SNI).
  #[must_use]
  #[inline]
  pub fn with_quic_config(mut self, cfg: QuicConfig) -> Self {
    self.quic_config = Some(cfg);
    self
  }

  /// Local node identifier — must be set before [`QuicTransport::new`].
  #[inline]
  pub const fn local_id(&self) -> Option<&I> {
    self.local_id.as_ref()
  }

  /// Advertise address — must be set before [`QuicTransport::new`].
  #[inline]
  pub const fn advertise_addr(&self) -> Option<&MaybeResolved<A, SocketAddr>> {
    self.advertise_addr.as_ref()
  }

  /// QUIC config bundle.
  #[inline]
  pub const fn quic_config(&self) -> Option<&QuicConfig> {
    self.quic_config.as_ref()
  }
}

impl<I, A> Default for QuicTransportOptions<I, A> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// QUIC-backed memberlist transport.
///
/// Owns the bound `UdpSocket` only — QUIC's quinn-proto multiplexes
/// streams over the single UDP socket, no separate listener. The
/// machine-layer `QuicEndpoint<I>` is built inside [`QuicTransport::run`]
/// from the stored `quic_config`.
pub struct QuicTransport<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: I,
  local_address: MaybeResolved<A, SocketAddr>,
  advertise_socket: SocketAddr,
  gossip_socket: UdpSocket,
  quic_config: QuicConfig,
}

impl<I, A> Transport for QuicTransport<I, A>
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
  type Options = QuicTransportOptions<I, A>;

  async fn new<RES, AR>(
    options: Self::Options,
    resolver: &RES,
    advertise_resolver: &AR,
  ) -> Result<Self, Self::Error>
  where
    RES: Resolver<Address = Self::Address>,
    AR: AdvertiseAddrResolver,
  {
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
    let quic_config = options.quic_config.ok_or_else(|| {
      MemberlistError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "quic_config required",
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
    // request resolves to a concrete OS-assigned port here. The node
    // gossips this resolved address to its peers.
    let advertise_socket = gossip_socket.local_addr().map_err(MemberlistError::Io)?;

    Ok(Self {
      local_id,
      local_address: advertise_input,
      advertise_socket,
      gossip_socket,
      quic_config,
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
    // QUIC endpoint (it needs the quinn-proto config bundle); build it
    // here from `self`'s stored config. `QuicConfig` carries the
    // per-peer SNI plumbing internally.
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
    let endpoint = QuicEndpoint::new(ep, self.quic_config);

    crate::quic_driver::quic_driver_loop::<Self::Id, D>(
      endpoint,
      self.gossip_socket,
      runtime.commands_rx,
      runtime.events_tx,
      runtime.events_dropped,
      runtime.observation_dropped,
      runtime.snapshot,
      runtime.shutdown_flag,
      runtime.driver_options,
      runtime.delegate,
    )
    .await;
  }
}

#[cfg(test)]
mod transport_tests {
  use std::{net::SocketAddr, sync::Arc, time::Duration};

  use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
  use rustls::RootCertStore;
  use rustls_pki_types::{CertificateDer, PrivateKeyDer};

  use super::*;
  use crate::{FirstAddrResolver, MaybeResolved, OsResolver};

  const ALPN: &[u8] = b"memberlist-quic";

  /// Build a minimal valid `QuicConfig` for the construction test. Mirrors
  /// the `tests/support/quic.rs` fixture; the test never performs a QUIC
  /// handshake, the config just has to type-check and pass `QuicConfig::new`.
  fn default_test_quic_config() -> QuicConfig {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen self-sign");
    let cert = CertificateDer::from(ck.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());

    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
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
      .with_protocol_versions(&[&rustls::version::TLS13])
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

    QuicConfig::new(endpoint_cfg, server_cfg, client_cfg, transport, "localhost")
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
}
