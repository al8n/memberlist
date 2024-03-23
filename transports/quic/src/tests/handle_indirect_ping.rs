use memberlist_core::transport::{tests::handle_indirect_ping, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

pub async fn indirect_ping<S, R>(s: S, c: S, kind: AddressKind) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name = format!("{kind}_indirect_ping");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::new(name.into())
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next(0));
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  let remote_addr = trans.advertise_address();
  let tc = QuicTransportTestClient::<_, R>::new(kind.next(0), *remote_addr, c)
    .await?
    .with_label(label)
    .with_send_label(true)
    .with_send_compressed(Some(Compressor::default()))
    .with_receive_compressed(true)
    .with_receive_verify_label(true);
  handle_indirect_ping(trans, tc).await?;
  Ok(())
}
