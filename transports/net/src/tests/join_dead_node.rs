use memberlist_core::transport::{tests::join_dead_node as join_dead_node_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{NetTransport, NetTransportOptions};

use super::*;

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn join_dead_node<S, R>(
  s1: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let mut opts = NetTransportOptions::new("node 1".into());
  opts.add_bind_address(kind.next(0));
  let trans1 =
    NetTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s1, opts).await?;

  join_dead_node_in(trans1, client, "fake".into()).await;
  Ok(())
}
