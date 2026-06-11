//! TCP-backed memberlist driver — the first end-to-end usable surface.
//!
//! [`TcpTransport`] owns the bound UDP gossip socket and TCP reliable
//! listener; [`Memberlist::new`](crate::Memberlist::new) constructs the
//! transport, builds the initial snapshot, and spawns the driver task on
//! the compio runtime. [`TcpMemberlist`] is the pinned-alias convenience
//! shape (`I = SmolStr`, `A = SocketAddr`, default [`VoidDelegate`]); the
//! machine endpoint is built inside [`TcpTransport::run`] from the stored
//! [`LabelOptions<()>`](memberlist_proto::streams::LabelOptions). The returned handle is cheaply clonable and shares the
//! same driver task with every clone.

#![cfg(feature = "tcp")]

use std::net::SocketAddr;

use bytes::Bytes;
use compio::net::{TcpListener, UdpSocket};
use hostaddr::HostAddr;
use memberlist_proto::{
  LabelOptions, RawRecords, config::EndpointOptions, endpoint::Endpoint, streams::StreamEndpoint,
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
/// local identifier, the (possibly-unresolved) advertise address, and the
/// stream-transport tuning knobs. The cluster label and inbound-label-check
/// policy are supplied via
/// [`MemberlistOptions::with_label`](crate::MemberlistOptions::with_label) and
/// [`MemberlistOptions::with_skip_inbound_label_check`](crate::MemberlistOptions::with_skip_inbound_label_check),
/// feeding both planes from a single validated source.
pub struct TcpTransportOptions<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: Option<I>,
  advertise_addr: Option<MaybeResolved<A, SocketAddr>>,
  stream: StreamTransportOptions,
}

impl<I, A> TcpTransportOptions<I, A> {
  /// Construct with defaults. Caller MUST chain `with_local_id` and
  /// `with_advertise_addr` before passing to `TcpTransport::new`.
  #[inline]
  pub fn new() -> Self {
    Self {
      local_id: None,
      advertise_addr: None,
      stream: StreamTransportOptions::new(),
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
/// [`TcpTransport::run`] from the cluster label and inbound-check policy
/// sourced from [`MemberlistOptions`](crate::MemberlistOptions).
pub struct TcpTransport<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: I,
  local_address: MaybeResolved<A, SocketAddr>,
  advertise_socket: SocketAddr,
  gossip_socket: UdpSocket,
  tcp_listener: TcpListener,
  stream_options: StreamTransportOptions,
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

    // memberlist reaches a node at ONE advertised address, so the UDP gossip
    // socket and the TCP reliable listener MUST share a port. Bind the listener
    // first to claim an OS-assigned free port, then bind the gossip socket to
    // that same port. Binding UDP first and reusing its port for TCP races: an
    // ephemeral UDP port can land on a TCP port still in TIME_WAIT (the spaces
    // are independent), failing the TCP bind with AddrInUse. For an ephemeral
    // (`:0`) advertise we retry the pair on a fresh port when the UDP bind fails
    // transiently: AddrInUse from the port-space race, or PermissionDenied
    // (Windows WSAEACCES) when the listener's OS-assigned port falls in a UDP
    // excluded range — CI hypervisors reserve disjoint TCP/UDP ranges, so a
    // TCP-bindable port can still be UDP-forbidden. A fixed (nonzero) port is a
    // single attempt, so a genuine conflict surfaces to the caller instead of
    // looping.
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
              && matches!(
                e.kind(),
                std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied
              )
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
      EndpointOptions::new(self.local_id, self.advertise_socket),
      &runtime.memberlist_options,
    );
    // The single configured label from MemberlistOptions feeds both the
    // reliable-plane label exchange and the gossip codec. Validated at the
    // MemberlistOptions setter; `new_in` is infallible here.
    let label_bytes = runtime.memberlist_options.label().map(|b| b.to_vec());
    let mut label_opts = LabelOptions::new_in(label_bytes, ());
    if runtime.memberlist_options.skip_inbound_label_check() {
      label_opts = label_opts.skip_inbound_label_check();
    }

    // Retain the validated label for the gossip codec (same source as the
    // reliable-plane opts above, so gossip and reliable cannot diverge).
    let label = runtime
      .memberlist_options
      .label()
      .map(Bytes::copy_from_slice);

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
    let mut endpoint = StreamEndpoint::with_compression(
      ep,
      label_opts,
      Box::new(|_| None),
      Box::new(|addr| *addr),
      *runtime.memberlist_options.compression(),
    )
    .with_encryption(runtime.memberlist_options.encryption().clone());
    // Checksum is gossip-plane (unreliable) only — reliable bridges carry no
    // checksum — so it is threaded via the gossip-plane setter rather than a
    // reliable-bridge-fanning builder.
    //
    // Ignoring Err: the checksum policy was validated in `Memberlist::new`
    // (`validate_checksum_options`) before this driver task spawned, so its
    // backend is built in and this store cannot fail.
    let _ = endpoint.set_checksum_options(*runtime.memberlist_options.checksum());

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
mod transport_tests {
  use super::*;
  use crate::{FirstAddrResolver, MaybeResolved, OsResolver, SocketAddrResolver};
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

  /// `new` rejects a missing `local_id` with `InvalidInput` BEFORE any
  /// resolution or socket bind — the field is required.
  #[compio::test]
  async fn new_without_local_id_errors() {
    let opts = TcpTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()));
    let res = TcpTransport::<smol_str::SmolStr, SocketAddr>::new(
      opts,
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await;
    match res {
      Err(MemberlistError::Io(e)) => {
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
        assert!(e.to_string().contains("local_id"));
      }
      Err(other) => panic!("expected InvalidInput(local_id), got {other:?}"),
      Ok(_) => panic!("a missing local_id must be rejected, but construction succeeded"),
    }
  }

  /// `new` rejects a missing `advertise_addr` with `InvalidInput`.
  #[compio::test]
  async fn new_without_advertise_addr_errors() {
    let opts = TcpTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
      .with_local_id(smol_str::SmolStr::new("no-adv"));
    let res = TcpTransport::<smol_str::SmolStr, SocketAddr>::new(
      opts,
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await;
    match res {
      Err(MemberlistError::Io(e)) => {
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
        assert!(e.to_string().contains("advertise_addr"));
      }
      Err(other) => panic!("expected InvalidInput(advertise_addr), got {other:?}"),
      Ok(_) => panic!("a missing advertise_addr must be rejected, but construction succeeded"),
    }
  }

  /// Labels larger than 253 bytes or non-UTF-8 must be rejected at the
  /// MemberlistOptions setter — not at construction and not via a panic.
  #[test]
  fn with_label_rejects_invalid_at_setter() {
    use crate::{MemberlistError, MemberlistOptions};

    let too_long = vec![b'x'; 254];
    let result = MemberlistOptions::new().with_label(Some(too_long));
    assert!(
      matches!(result, Err(MemberlistError::InvalidLabel(_))),
      "a label exceeding 253 bytes must be rejected at the setter"
    );

    let non_utf8 = vec![0xff, 0xfe];
    let result = MemberlistOptions::new().with_label(Some(non_utf8));
    assert!(
      matches!(result, Err(MemberlistError::InvalidLabel(_))),
      "a non-UTF-8 label must be rejected at the setter"
    );
  }
}
