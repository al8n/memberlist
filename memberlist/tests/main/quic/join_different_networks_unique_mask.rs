use std::future::Future;

use agnostic::Runtime;
use memberlist::{
  futures::Stream,
  transport::{MaybeResolvedAddress, Node, Transport},
  types::CIDRsPolicy,
  Memberlist, Options,
};

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
      fn [< test_ $rt:snake _ $kind:snake _join_different_networks_unique_mask >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_unique_mask(|idx, cidrs| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new(format!("join_different_networks_unique_mask_node_{idx}").into())
              .with_cidrs_policy(cidrs);
            t1_opts.add_bind_address(next_socket_addr_v4(idx as u8));

            QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap()
          }).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_different_networks_unique_mask_with_compression >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_unique_mask(|idx, cidrs| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new(format!("join_different_networks_unique_mask_node_{idx}").into())
              .with_cidrs_policy(cidrs)
              .with_compressor(Some(Default::default()));
            t1_opts.add_bind_address(next_socket_addr_v4(idx as u8));

            QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap()
          }).await;
        });
      }
    }
  };
}

test_mods!(join_different_networks_unique_mask);
