use std::future::Future;

use agnostic::Runtime;
use memberlist::{transport::MaybeResolvedAddress, types::CIDRsPolicy, Memberlist};

use super::*;

/// Unit test for join a `Memberlist` with unique mask.
async fn join_different_networks_unique_mask<F, T, R>(
  mut get_transport: impl FnMut(usize, CIDRsPolicy) -> F,
) where
  F: Future<Output = T::Options>,
  T: Transport<Runtime = R>,
  R: Runtime,
{
  let cidr = CIDRsPolicy::try_from(["127.0.0.0/8"].as_slice()).unwrap();
  let m1 = Memberlist::<T, _>::new(get_transport(0, cidr.clone()).await, Options::lan())
    .await
    .unwrap();

  // Create a second node
  let m2 = Memberlist::<T, _>::new(get_transport(1, cidr).await, Options::lan())
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
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_different_networks_unique_mask >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_unique_mask::<_, QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|idx, cidrs| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(format!("join_different_networks_unique_mask_node_{idx}").into(), $expr)
              .with_cidrs_policy(cidrs);
            t1_opts.add_bind_address(next_socket_addr_v4(idx as u8));

            t1_opts
          }).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_different_networks_unique_mask_with_compression >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_unique_mask::<_, QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|idx, cidrs| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(format!("join_different_networks_unique_mask_node_{idx}").into(), $expr)
              .with_cidrs_policy(cidrs)
              .with_compressor(Some(Default::default()));
            t1_opts.add_bind_address(next_socket_addr_v4(idx as u8));

            t1_opts
          }).await;
        });
      }
    }
  };
}

test_mods!(join_different_networks_unique_mask);
