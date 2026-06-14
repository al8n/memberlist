//! memberlist on the smol runtime (via the runtime-agnostic reactor driver).
//!
//! Thin wrappers over [`memberlist_reactor::Memberlist`] that pin the runtime to
//! smol, so callers never name `R`. The full reactor surface is re-exported.
pub use memberlist_reactor::*;

use core::net::SocketAddr;

/// The runtime these constructors bind.
pub type Runtime = agnostic::smol::SmolRuntime;

/// Build a QUIC-backed node on smol. See [`memberlist_reactor::Memberlist::quic`].
#[cfg(feature = "quic")]
pub async fn quic<I, Res, D>(
  resolver: &Res,
  local_id: I,
  advertise: MaybeResolved<Res::Address>,
  options: Options<I>,
  delegate: D,
  quic_config: QuicOptions,
) -> Result<Memberlist<I, Res::Address>, Error>
where
  I: NodeId,
  Res: AddressResolver,
  D: Delegate<Id = I, Address = SocketAddr>,
{
  Memberlist::<I, Res::Address>::quic::<Runtime, Res, D>(
    resolver,
    local_id,
    advertise,
    options,
    delegate,
    quic_config,
  )
  .await
}

/// Build a TCP-backed node on smol. See [`memberlist_reactor::Memberlist::tcp`].
#[cfg(feature = "tcp")]
pub async fn tcp<I, Res, D>(
  resolver: &Res,
  local_id: I,
  advertise: MaybeResolved<Res::Address>,
  options: Options<I>,
  delegate: D,
) -> Result<Memberlist<I, Res::Address>, Error>
where
  I: NodeId,
  Res: AddressResolver,
  D: Delegate<Id = I, Address = SocketAddr>,
{
  Memberlist::<I, Res::Address>::tcp::<Runtime, Res, D>(
    resolver, local_id, advertise, options, delegate,
  )
  .await
}

/// Build a TLS-backed node on smol. See [`memberlist_reactor::Memberlist::tls`].
#[cfg(feature = "tls")]
pub async fn tls<I, Res, D, F>(
  resolver: &Res,
  local_id: I,
  advertise: MaybeResolved<Res::Address>,
  options: Options<I>,
  delegate: D,
  tls_options: TlsOptions,
  sni_provider: F,
) -> Result<Memberlist<I, Res::Address>, Error>
where
  I: NodeId,
  Res: AddressResolver,
  D: Delegate<Id = I, Address = SocketAddr>,
  F: Fn(&SocketAddr) -> Option<String> + Send + Sync + 'static,
{
  Memberlist::<I, Res::Address>::tls::<Runtime, Res, D, F>(
    resolver,
    local_id,
    advertise,
    options,
    delegate,
    tls_options,
    sni_provider,
  )
  .await
}
