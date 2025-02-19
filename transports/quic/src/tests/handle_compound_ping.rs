use memberlist_core::transport::{tests::handle_compound_ping, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

pub async fn compound_ping<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_compound_ping");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone())
    .with_offload_size(10);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, _>::new(opts).await?;
  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<S, R>::with_num_responses(kind.next(0), *remote_addr, c, 3)
    .await?
    .with_label(label)
    .with_send_label(true)
    .with_send_compressed(Some(Compressor::default()))
    .with_receive_compressed(true)
    .with_receive_verify_label(true);

  tracing::info!("start handle compound ping");
  handle_compound_ping(trans, tc, super::compound_encoder).await?;
  tracing::info!("finish handle compound ping");
  Ok(())
}
