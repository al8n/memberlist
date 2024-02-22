use std::future::Future;

use agnostic::Runtime;
use memberlist::{
  futures::Stream,
  transport::{MaybeResolvedAddress, Node, Transport},
  Memberlist, Options,
};
use memberlist_utils::net::CIDRsPolicy;

use super::*;

async fn join_different_networks_multi_masks<F, T, R>(
  mut get_transport: impl FnMut(usize, CIDRsPolicy) -> F,
) where
  F: Future<Output = T>,
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let cidrs = CIDRsPolicy::try_from(["127.0.0.0/24", "127.0.1.0/24"].as_slice()).unwrap();
  let m1 = Memberlist::new(get_transport(0, cidrs.clone()).await, Options::lan())
    .await
    .unwrap();

  // Create a second node
  let m2 = Memberlist::new(get_transport(1, cidrs.clone()).await, Options::lan())
    .await
    .unwrap();
  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );

  m2.join(target.clone()).await.unwrap();

  // Check the hosts
  assert_eq!(m2.online_members().await.len(), 2);

  // Create a rogue node that allows all networks
  // It should see others, but will not be seen by others
  let cidrs1 = CIDRsPolicy::try_from(["127.0.0.0/8"].as_slice()).unwrap();
  let m3 = Memberlist::new(get_transport(2, cidrs1).await, Options::lan())
    .await
    .unwrap();
  // The rogue can see others, but others cannot see it
  m3.join(target.clone()).await.unwrap();

  // m1 and m2 should not see newcomer however
  assert_eq!(m1.num_online_members().await, 2);

  assert_eq!(m2.num_online_members().await, 2);

  // Another rogue, this time with a config that denies itself
  // Create a rogue node that allows all networks
  // It should see others, but will not be seen by others
  let m4 = Memberlist::new(get_transport(2, cidrs.clone()).await, Options::lan())
    .await
    .unwrap();

  let target2 = Node::new(
    m2.local_id().clone(),
    MaybeResolvedAddress::resolved(m2.advertise_address().clone()),
  );
  m4.join_many([target, target2].into_iter()).await.unwrap();

  // m1 and m2 should not see newcomer however
  assert_eq!(m1.online_members().await.len(), 2);
  assert_eq!(m2.online_members().await.len(), 2);
}

macro_rules! join_different_networks_multi_masks {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _net_join_different_networks_multi_masks >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_multi_masks(|idx, cidrs| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _>::new(format!("join_different_networks_multi_masks_node_{idx}").into())
              .with_cidrs_policy(cidrs);
            t1_opts.add_bind_address(next_socket_addr_v4(idx as u8));

            NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap()
          }).await;
        });
      }
    }
  };
}

#[cfg(feature = "tokio")]
mod tokio {
  use agnostic::tokio::TokioRuntime;
  use memberlist_net::stream_layer::tcp::Tcp;

  use super::*;
  use crate::tokio_run;

  join_different_networks_multi_masks!(tokio("tcp", Tcp::<TokioRuntime>::new()));

  #[cfg(feature = "tls")]
  join_different_networks_multi_masks!(tokio(
    "tls",
    memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  join_different_networks_multi_masks!(tokio(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
  ));
}

#[cfg(feature = "async-std")]
mod async_std {
  use agnostic::async_std::AsyncStdRuntime;
  use memberlist_net::stream_layer::tcp::Tcp;

  use super::*;
  use crate::async_std_run;

  join_different_networks_multi_masks!(async_std("tcp", Tcp::<AsyncStdRuntime>::new()));

  #[cfg(feature = "tls")]
  join_different_networks_multi_masks!(async_std(
    "tls",
    memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  join_different_networks_multi_masks!(async_std(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
  ));
}

#[cfg(feature = "smol")]
mod smol {
  use agnostic::smol::SmolRuntime;
  use memberlist_net::stream_layer::tcp::Tcp;

  use super::*;
  use crate::smol_run;

  join_different_networks_multi_masks!(smol("tcp", Tcp::<SmolRuntime>::new()));

  #[cfg(feature = "tls")]
  join_different_networks_multi_masks!(smol(
    "tls",
    memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  join_different_networks_multi_masks!(smol(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
  ));
}
