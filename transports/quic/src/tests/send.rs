use memberlist_core::transport::{tests::send as send_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

#[cfg(feature = "compression")]
pub async fn send<S1, S2, R>(
  s1: S1::Options,
  s2: S2::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S1: StreamLayer,
  S2: StreamLayer,
  R: RuntimeLite,
{
  use nodecraft::CheapClone;

  let name = format!("{kind}_send");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::<_, _, S1>::new("node 1".into(), s1)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next(0));
  let trans1 = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;

  let mut opts = QuicTransportOptions::<_, _, S2>::new("node 2".into(), s2)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next(0));

  let trans2 = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;

  send_in(trans1, trans2).await?;
  Ok(())
}
