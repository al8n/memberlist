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
  S1: StreamLayer<Runtime = R>,
  S2: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  use nodecraft::CheapClone;

  let name = format!("{kind}_send");
  let label = Label::try_from(&name)?;

  let mut opts1 = QuicTransportOptions::<_, _, S1>::with_stream_layer_options("node 1".into(), s1)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts1.add_bind_address(kind.next(0));

  let mut opts2 = QuicTransportOptions::<_, _, S2>::with_stream_layer_options("node 2".into(), s2)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts2.add_bind_address(kind.next(0));

  send_in::<
    _,
    QuicTransport<SmolStr, SocketAddrResolver<R>, _, Lpe<_, _>, _>,
    QuicTransport<SmolStr, SocketAddrResolver<R>, _, Lpe<_, _>, _>,
    _,
  >(opts1, opts2)
  .await?;
  Ok(())
}
