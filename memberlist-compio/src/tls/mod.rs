//! TLS-backed memberlist driver.
//!
//! [`TlsTransport`] owns the bound `UdpSocket` (gossip) and `TcpListener`
//! (reliable; TLS handshake-on-accept); the machine-layer
//! `StreamEndpoint<I, SocketAddr, Labeled<TlsRecords>>` is built inside
//! [`TlsTransport::run`] from the stored `TlsTransportOptions` (cert/key
//! bundle + SNI provider). Construct via
//! [`Memberlist::new`](crate::Memberlist::new).
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

use crate::StreamEndpoint;
use bytes::Bytes;
use compio::net::{TcpListener, UdpSocket};
use hostaddr::HostAddr;
use memberlist_proto::{
  LabelOptions, Labeled, TlsOptions, TlsRecords, config::EndpointOptions, endpoint::Endpoint,
};
use smol_str::SmolStr;

use crate::{
  AdvertiseAddrResolver, Delegate, MaybeResolved, MemberlistError, Resolver, Result,
  StreamTransportOptions, Transport, TransportRuntime,
  delegate::{BoxedAlive, BoxedMerge},
};
use core::fmt;
use std::io::ErrorKind;

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
/// `StreamEndpoint<I, SocketAddr, Labeled<TlsRecords>>` is built inside
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
    + fmt::Debug
    + fmt::Display
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
        ErrorKind::InvalidInput,
        "local_id required",
      ))
    })?;
    let advertise_input = options.advertise_addr.ok_or_else(|| {
      MemberlistError::Io(std::io::Error::new(
        ErrorKind::InvalidInput,
        "advertise_addr required",
      ))
    })?;
    let tls_options = options.tls_options.ok_or_else(|| {
      MemberlistError::Io(std::io::Error::new(
        ErrorKind::InvalidInput,
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
            ErrorKind::AddrNotAvailable,
            e.to_string(),
          ))
        })?
      }
    };

    // memberlist reaches a node at ONE advertised address, so the UDP gossip
    // socket and the TLS-over-TCP reliable listener MUST share a port. Bind the
    // listener first to claim an OS-assigned free port, then bind the gossip
    // socket to that same port. Binding UDP first and reusing its port for TCP
    // races: an ephemeral UDP port can land on a TCP port still in TIME_WAIT
    // (the spaces are independent), failing the TCP bind with AddrInUse. For an
    // ephemeral (`:0`) advertise we retry the pair on a fresh port when the UDP
    // bind fails transiently: AddrInUse from the port-space race, or
    // PermissionDenied (Windows WSAEACCES) when the listener's OS-assigned port
    // falls in a UDP excluded range — CI hypervisors reserve disjoint TCP/UDP
    // ranges, so a TCP-bindable port can still be UDP-forbidden. A fixed
    // (nonzero) port is a single attempt, so a genuine conflict surfaces to the
    // caller instead of looping.
    const EPHEMERAL_BIND_RETRIES: usize = 16;
    let ephemeral = advertise_socket.port() == 0;
    let (tcp_listener, advertise_socket, gossip_socket) = {
      let mut attempt = 0usize;
      loop {
        let tcp_listener = TcpListener::bind(advertise_socket)
          .await
          .map_err(MemberlistError::Io)?;
        let bound = tcp_listener.local_addr().map_err(MemberlistError::Io)?;
        match UdpSocket::bind(bound).await {
          Ok(gossip_socket) => break (tcp_listener, bound, gossip_socket),
          Err(e)
            if ephemeral
              && matches!(e.kind(), ErrorKind::AddrInUse | ErrorKind::PermissionDenied)
              && attempt < EPHEMERAL_BIND_RETRIES =>
          {
            // Release the claimed TCP port and retry on a fresh ephemeral pair.
            // On Windows `drop` closes asynchronously (IOCP), so the port would
            // linger and the retries — especially many in parallel during a test
            // run — would exhaust the ephemeral pool before finding a
            // UDP-bindable port; `close().await` releases it synchronously first.
            // Ignoring Err: this attempt's listener is being discarded regardless
            // and a fresh pair is rebound, so a close error is not actionable.
            let _ = tcp_listener.close().await;
            attempt += 1;
          }
          Err(e) => return Err(MemberlistError::Io(e)),
        }
      }
    };

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

  async fn run<D, G>(self, runtime: TransportRuntime<Self, D>, gossip_rng: G)
  where
    D: Delegate<Id = Self::Id, Address = SocketAddr>,
    G: rand::Rng + Send + Unpin + 'static,
  {
    // `Memberlist::new` is generic over `T` and cannot build the
    // record-layer-specific endpoint; build it here from `self`'s
    // stored config. TLS supplies the per-peer SNI provider; the
    // membership address IS the transport socket (`|addr| *addr`).
    let cfg = crate::options::apply_memberlist_options(
      EndpointOptions::new(self.local_id, self.advertise_socket),
      &runtime.memberlist_options,
    );
    // Install the optional machine admission predicates before the driver
    // loop starts; each `None` leaves the `Endpoint` at its admit-all
    // default. `BoxedAlive`/`BoxedMerge` forward the boxed dyn so it
    // satisfies the setters' `impl AliveDelegate`/`impl MergeDelegate` bound.
    let mut ep = Endpoint::new(cfg, gossip_rng);
    if let Some(ad) = runtime.alive_delegate {
      ep.set_alive_delegate(BoxedAlive(ad));
    }
    if let Some(md) = runtime.merge_delegate {
      ep.set_merge_delegate(BoxedMerge(md));
    }
    // The reliable transport is the cluster-label decorator over the TLS
    // record layer: the configured cluster label rides as the first plaintext
    // inside the TLS session, so two clusters that share a TLS trust anchor and
    // SNI are still isolated on the reliable plane. The same label feeds the
    // gossip codec below, so the two planes cannot diverge.
    let label_bytes = runtime.memberlist_options.label().map(|b| b.to_vec());
    let mut tls_label_opts = LabelOptions::new_in(label_bytes, self.tls_options);
    if runtime.memberlist_options.skip_inbound_label_check() {
      tls_label_opts = tls_label_opts.skip_inbound_label_check();
    }
    // Build the coordinator with all transforms disabled, then layer in each
    // configured transform whose backend is built in. With none built in the
    // base coordinator carries no transform state and the planes stay plaintext.
    #[allow(unused_mut)]
    let mut endpoint = StreamEndpoint::new(
      ep,
      tls_label_opts,
      self.sni_provider,
      Box::new(|addr| *addr),
    );
    #[cfg(compression)]
    endpoint.set_compression_options(*runtime.memberlist_options.compression());
    #[cfg(encryption)]
    endpoint.set_encryption_options(runtime.memberlist_options.encryption().clone());
    // Checksum is gossip-plane (unreliable) only — reliable bridges carry no
    // checksum — so it is threaded via the gossip-plane setter rather than a
    // reliable-bridge-fanning builder.
    // Ignoring Err: the checksum policy was validated in `Memberlist::new`
    // (`validate_checksum_options`) before this driver task spawned, so its
    // backend is built in and this store cannot fail.
    #[cfg(checksum)]
    let _ = endpoint.set_checksum_options(*runtime.memberlist_options.checksum());

    // Retain the cluster label for the gossip codec. TLS isolation comes from
    // the TLS record layer; the gossip label is still applied via the codec
    // EncodeOptions, mirroring the reactor driver's behaviour.
    let label = runtime
      .memberlist_options
      .label()
      .map(Bytes::copy_from_slice);

    let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded();
    crate::driver::stream::stream_driver_loop::<Self::Id, SocketAddr, Labeled<TlsRecords>, D, G>(
      endpoint,
      self.gossip_socket,
      self.tcp_listener,
      runtime.commands_rx,
      runtime.events_tx,
      runtime.events_dropped,
      runtime.observation_dropped,
      runtime.snapshot,
      runtime.metrics,
      bridge_ready_rx,
      bridge_ready_tx,
      runtime.shutdown_flag,
      runtime.driver_options,
      self.stream_options,
      runtime.delegate,
      label,
      runtime.cidr_policy,
    )
    .await;
  }
}

#[cfg(test)]
mod tests;
