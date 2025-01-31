use memberlist_core::transport::{tests::handle_ping_wrong_node, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

pub async fn ping_wrong_node<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_ping_wrong_node");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts).await?;
  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<_, R>::new(kind.next(0), *remote_addr, c)
    .await?
    .with_label(label)
    .with_send_label(true)
    .with_send_compressed(Some(Compressor::default()))
    .with_receive_compressed(true)
    .with_receive_verify_label(true);
  handle_ping_wrong_node(trans, tc).await?;
  Ok(())
}
