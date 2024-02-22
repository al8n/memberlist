use std::future::Future;

use agnostic::Runtime;
use memberlist::{
  futures::Stream,
  transport::{MaybeResolvedAddress, Node, Transport},
  Memberlist, Options,
};
use memberlist_utils::net::CIDRsPolicy;

use super::*;

/// Unit test for join a `Memberlist` with unique mask.
async fn join_different_networks_unique_mask<F, T, R>(
  mut get_transport: impl FnMut(usize, CIDRsPolicy) -> F,
) where
  F: Future<Output = T>,
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let cidr = CIDRsPolicy::try_from(["127.0.0.0/8"].as_slice()).unwrap();
  let m1 = Memberlist::new(get_transport(0, cidr.clone()).await, Options::lan())
    .await
    .unwrap();

  // Create a second node
  let m2 = Memberlist::new(get_transport(1, cidr).await, Options::lan())
    .await
    .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );

  m2.join(target).await.unwrap();

  // Check the hosts
  assert_eq!(m2.online_members().await.len(), 2);
}

macro_rules! join_different_networks_unique_mask {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _net_join_different_networks_unique_mask >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_unique_mask(|idx, cidrs| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _>::new(format!("join_different_networks_unique_mask_node_{idx}").into())
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

  join_different_networks_unique_mask!(tokio("tcp", Tcp::<TokioRuntime>::new()));

  #[cfg(feature = "tls")]
  join_different_networks_unique_mask!(tokio(
    "tls",
    memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  join_different_networks_unique_mask!(tokio(
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

  join_different_networks_unique_mask!(async_std("tcp", Tcp::<AsyncStdRuntime>::new()));

  #[cfg(feature = "tls")]
  join_different_networks_unique_mask!(async_std(
    "tls",
    memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  join_different_networks_unique_mask!(async_std(
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

  join_different_networks_unique_mask!(smol("tcp", Tcp::<SmolRuntime>::new()));

  #[cfg(feature = "tls")]
  join_different_networks_unique_mask!(smol(
    "tls",
    memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  join_different_networks_unique_mask!(smol(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
  ));
}
