//! QUIC-backed memberlist driver.
//!
//! [`QuicTransport`] owns the bound `UdpSocket`; QUIC carries no separate
//! TCP listener — quinn-proto multiplexes streams over the single UDP
//! socket. The machine-layer `QuicEndpoint<I>` is built inside
//! [`QuicTransport::run`] from the stored [`QuicOptions`]. Construct via
//! [`Memberlist::new`](crate::Memberlist::new).
//!
//! ## TLS server name
//!
//! QUIC's TLS 1.3 handshake requires a server name to verify the peer's
//! certificate against. [`QuicOptions::new`] installs a cluster-uniform
//! string used for every peer (see the `quic-rustls` test setups in
//! `memberlist-simulation` for the `"localhost"` SAN convention);
//! deployments whose certs name each peer's hostname/IP supply a per-peer
//! SNI closure via [`QuicOptions::new_with_sni_provider`] that maps each
//! dialed `SocketAddr` to its expected verification identity.

#![cfg(feature = "quic")]

use std::net::SocketAddr;

use crate::QuicEndpoint;
use compio::net::UdpSocket;
use hostaddr::HostAddr;
use memberlist_proto::{config::EndpointOptions, endpoint::Endpoint};
use smol_str::SmolStr;

use crate::{
  AdvertiseAddrResolver, Delegate, MemberlistError, Resolver, Result, Transport, TransportRuntime,
  delegate::{BoxedAlive, BoxedMerge},
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

use core::fmt;
/// QUIC config bundle handed to [`QuicTransport`]. Re-exported from
/// `memberlist-proto` so callers don't need a direct dep.
pub use memberlist_proto::QuicOptions;
use std::io::ErrorKind;

/// Per-backend QUIC-specific transport options.
///
/// Embedded into `Options<QuicTransport<I, A>>::transport()`. Bundles the
/// local identifier, the (possibly-unresolved) advertise address, and the
/// caller-built [`QuicOptions`] (quinn-proto `EndpointConfig` /
/// `ServerConfig` / `ClientConfig` / `TransportConfig` bundle plus SNI
/// provider).
pub struct QuicTransportOptions<I = SmolStr, A = HostAddr<SmolStr>> {
  local_id: Option<I>,
  advertise_addr: Option<MaybeResolved<A, SocketAddr>>,
  quic_config: Option<QuicOptions>,
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
  pub fn with_quic_config(mut self, cfg: QuicOptions) -> Self {
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
  pub const fn quic_config(&self) -> Option<&QuicOptions> {
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
  quic_config: QuicOptions,
}

impl<I, A> Transport for QuicTransport<I, A>
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
    let quic_config = options.quic_config.ok_or_else(|| {
      MemberlistError::Io(std::io::Error::new(
        ErrorKind::InvalidInput,
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
            ErrorKind::AddrNotAvailable,
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

  async fn run<D, G>(self, runtime: TransportRuntime<Self, D>, gossip_rng: G)
  where
    D: Delegate<Id = Self::Id, Address = SocketAddr>,
    G: rand::Rng + Send + Unpin + 'static,
  {
    // `Memberlist::new` is generic over `T` and cannot build the
    // QUIC endpoint (it needs the quinn-proto config bundle); build it
    // here from `self`'s stored config. `QuicOptions` carries the
    // per-peer SNI plumbing internally.
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
    // Retain the cluster label. The same label feeds both the gossip codec
    // (outbound stamping + inbound verification) and the QUIC reliable bridge
    // (stream label framing), ensuring the two planes share one source and
    // cannot diverge.
    let label = runtime
      .memberlist_options
      .label()
      .map(bytes::Bytes::copy_from_slice);
    // Build the coordinator with all transforms disabled, then layer in each
    // configured transform whose backend is built in. With none built in the
    // base coordinator carries no transform state and the planes stay plaintext.
    // The cluster label is not a transform — it is always applied.
    #[allow(unused_mut)]
    let mut endpoint = QuicEndpoint::new(ep, self.quic_config)
      .with_label(
        label.clone(),
        runtime.memberlist_options.skip_inbound_label_check(),
      )
      .expect("cluster label validated at the MemberlistOptions setter");
    #[cfg(compression)]
    endpoint.set_compression_options(*runtime.memberlist_options.compression());
    #[cfg(encryption)]
    endpoint.set_encryption_options(runtime.memberlist_options.encryption().clone());
    // Checksum is gossip-plane (unreliable) only — the reliable QUIC bridge
    // carries no checksum (quinn provides its own integrity) — so it is
    // threaded via the gossip-plane setter rather than a builder.
    // Ignoring Err: the checksum policy was validated in `Memberlist::new`
    // (`validate_checksum_options`) before this driver task spawned, so its
    // backend is built in and this store cannot fail.
    #[cfg(checksum)]
    let _ = endpoint.set_checksum_options(*runtime.memberlist_options.checksum());
    crate::quic_driver::quic_driver_loop::<Self::Id, D, G>(
      endpoint,
      self.gossip_socket,
      runtime.commands_rx,
      runtime.events_tx,
      runtime.events_dropped,
      runtime.observation_dropped,
      runtime.snapshot,
      runtime.metrics,
      runtime.shutdown_flag,
      runtime.driver_options,
      runtime.delegate,
      label,
      runtime.cidr_policy,
    )
    .await;
  }
}

#[cfg(test)]
mod tests;
