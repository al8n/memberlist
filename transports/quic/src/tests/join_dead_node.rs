use memberlist_core::transport::{tests::join_dead_node as join_dead_node_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

pub async fn join_dead_node<S, R>(
  s1: S::Options,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  use nodecraft::CheapClone;

  let name = format!("{kind}_join_dead_node");
  let label = Label::try_from(&name)?;

  let mut opts =
    QuicTransportOptions::<SmolStr, SocketAddrResolver<R>, S>::with_stream_layer_options(
      "node 1".into(),
      s1,
    );
  opts.add_bind_address(kind.next(0));

  join_dead_node_in::<_, QuicTransport<SmolStr, SocketAddrResolver<R>, _, Lpe<_, _>, _>, _, _>(
    opts,
    client,
    "fake".into(),
  )
  .await;
  Ok(())
}
