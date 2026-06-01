//! TCP-backed memberlist driver — the first end-to-end usable surface.
//!
//! [`TcpTransport`] owns the bound UDP gossip socket and TCP reliable
//! listener; [`Memberlist::new`](crate::Memberlist::new) constructs the
//! transport, builds the initial snapshot, and spawns the driver task on
//! the compio runtime. [`TcpMemberlist`] is the pinned-alias convenience
//! shape (`I = SmolStr`, `A = SocketAddr`, default [`VoidDelegate`]); the
//! machine endpoint is built inside [`TcpTransport::run`] from the stored
//! [`TcpOptions`]. The returned handle is cheaply clonable and shares the
//! same driver task with every clone.

#![cfg(feature = "tcp")]

use std::net::SocketAddr;

use compio::net::{TcpListener, UdpSocket};
use hostaddr::HostAddr;
use memberlist_proto::{
  RawRecords, TcpOptions, config::EndpointConfig, endpoint::Endpoint, streams::StreamEndpoint,
};
use smol_str::SmolStr;

use crate::{
  AdvertiseAddrResolver, Delegate, Memberlist, MemberlistError, Resolver, Result,
  StreamTransportOptions, Transport, TransportRuntime,
  delegate::{BoxedAlive, BoxedMerge, VoidDelegate},
  maybe_resolved::MaybeResolved,
};

/// TCP-backed [`Memberlist`] alias. Defaults: `I = SmolStr`,
/// `A = HostAddr<SmolStr>`, `D = VoidDelegate<I, SocketAddr>`. Custom-typed
/// callers parametrise `Memberlist<TcpTransport<MyId, MyAddr>, MyDelegate>`
/// directly.
pub type TcpMemberlist<I = SmolStr, A = HostAddr<SmolStr>, D = VoidDelegate<I, SocketAddr>> =
  Memberlist<TcpTransport<I, A>, D>;

/// Per-backend TCP-specific transport options.
///
/// Embedded into `Options<TcpTransport<I, A>>::transport()`. Bundles the
/// local identifier, the (possibly-unresolved) advertise address, the
/// stream-transport tuning knobs, and the plain-TCP record-layer
/// `TcpOptions` (cluster label + inbound-label-check policy).
pub struct TcpTransportOptions<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: Option<I>,
  advertise_addr: Option<MaybeResolved<A, SocketAddr>>,
  stream: StreamTransportOptions,
  tcp_options: TcpOptions,
}

impl<I, A> TcpTransportOptions<I, A> {
  /// Construct with defaults. Caller MUST chain `with_local_id` and
  /// `with_advertise_addr` before passing to `TcpTransport::new`. The
  /// default `tcp_options` is the unlabeled plain-TCP record layer
  /// (`TcpOptions::new(None)`); supply [`Self::with_tcp_options`] to set
  /// a cluster label.
  #[inline]
  pub fn new() -> Self {
    Self {
      local_id: None,
      advertise_addr: None,
      stream: StreamTransportOptions::new(),
      tcp_options: TcpOptions::new(None),
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

  /// Builder: plain-TCP record-layer options (cluster label +
  /// inbound-label-check policy). Defaults to the unlabeled layer.
  #[must_use]
  #[inline]
  pub fn with_tcp_options(mut self, opts: TcpOptions) -> Self {
    self.tcp_options = opts;
    self
  }

  /// Local node identifier — must be set before `TcpTransport::new`.
  #[inline]
  pub const fn local_id(&self) -> Option<&I> {
    self.local_id.as_ref()
  }

  /// Advertise address — must be set before `TcpTransport::new`.
  #[inline]
  pub const fn advertise_addr(&self) -> Option<&MaybeResolved<A, SocketAddr>> {
    self.advertise_addr.as_ref()
  }

  /// Stream-transport tuning knobs.
  #[inline]
  pub const fn stream(&self) -> &StreamTransportOptions {
    &self.stream
  }

  /// Plain-TCP record-layer options.
  #[inline]
  pub const fn tcp_options(&self) -> &TcpOptions {
    &self.tcp_options
  }
}

impl<I, A> Default for TcpTransportOptions<I, A> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// TCP-backed memberlist transport.
///
/// Owns the bound `UdpSocket` (gossip) and `TcpListener` (reliable). The
/// machine-layer `StreamEndpoint<I, SocketAddr, RawRecords>` is built inside
/// [`TcpTransport::run`] from the stored config (`tcp_options`).
pub struct TcpTransport<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: I,
  local_address: MaybeResolved<A, SocketAddr>,
  advertise_socket: SocketAddr,
  gossip_socket: UdpSocket,
  tcp_listener: TcpListener,
  stream_options: StreamTransportOptions,
  tcp_options: TcpOptions,
}

impl<I, A> Transport for TcpTransport<I, A>
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
  type Options = TcpTransportOptions<I, A>;

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
    // and TCP reliable share one port even when `:0` was requested.
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
      tcp_options: options.tcp_options,
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
    // stored config. Plain TCP has no SNI (`|_| None`) and a membership
    // address that IS the transport socket (`|addr| *addr`).
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
      self.tcp_options,
      Box::new(|_| None),
      Box::new(|addr| *addr),
    );

    let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded();
    crate::driver::stream_driver_loop::<Self::Id, SocketAddr, RawRecords, D>(
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
  use super::*;
  use crate::{FirstAddrResolver, MaybeResolved, OsResolver};
  use std::net::SocketAddr;

  fn test_tcp_opts() -> TcpTransportOptions {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    TcpTransportOptions::new()
      .with_local_id(smol_str::SmolStr::new("test-node"))
      .with_advertise_addr(MaybeResolved::Resolved(bind))
  }

  #[compio::test]
  async fn new_with_resolved_advertise_skips_resolver() {
    let opts = test_tcp_opts();
    let t: TcpTransport = TcpTransport::new(opts, &OsResolver, &FirstAddrResolver)
      .await
      .expect("construct TcpTransport");
    assert_eq!(t.local_id().as_str(), "test-node");
    assert!(t.local_address().is_resolved());
    let _: &SocketAddr = t.advertise_address();
  }
}
