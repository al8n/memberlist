use memberlist_core::transport::{tests::join_dead_node as join_dead_node_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{NetTransport, NetTransportOptions};

use super::*;

pub async fn join_dead_node<S, R>(
  s1: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options("node 1".into(), s1);
  opts.add_bind_address(kind.next(0));

  join_dead_node_in::<_, NetTransport<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>, _, _>(
    opts,
    client,
    "fake".into(),
  )
  .await;
  Ok(())
}
