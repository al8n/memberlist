use memberlist_core::transport::{tests::join_dead_node as join_dead_node_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

pub async fn join_dead_node<S, R>(
  s1: S,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  use nodecraft::CheapClone;

  let name = format!("{kind}_join_dead_node");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::new("node 1".into());
  opts.add_bind_address(kind.next(0));
  let trans1 =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s1, opts).await?;

  join_dead_node_in(trans1, client, "fake".into()).await;
  Ok(())
}
