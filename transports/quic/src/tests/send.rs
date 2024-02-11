use memberlist_core::transport::{tests::send as send_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

#[cfg(feature = "compression")]
pub async fn send<S1, S2, R>(s1: S1, s2: S2, kind: AddressKind) -> Result<(), AnyError>
where
  S1: StreamLayer,
  S2: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  use nodecraft::CheapClone;

  let name = format!("{kind}_send");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::new("node 1".into())
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next());
  let trans1 =
    QuicTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s1, opts).await?;

  let mut opts = QuicTransportOptions::new("node 2".into())
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next());

  let trans2 =
    QuicTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s2, opts).await?;

  send_in(trans1, trans2).await?;
  Ok(())
}
