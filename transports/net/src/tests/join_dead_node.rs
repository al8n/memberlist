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
  S: StreamLayer,
  R: Runtime,
{
  let mut opts = NetTransportOptions::<_, _, S>::new("node 1".into(), s1);
  opts.add_bind_address(kind.next(0));
  let trans1 = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;

  join_dead_node_in(trans1, client, "fake".into()).await;
  Ok(())
}
