use std::future::Future;

use agnostic::Runtime;
use memberlist_core::{
  Memberlist,
  proto::{CIDRsPolicy, MaybeResolvedAddress},
};

use super::*;

async fn join_different_networks_multi_masks<F, T, R>(
  mut get_transport: impl FnMut(usize, CIDRsPolicy) -> F,
  opts: Options,
) where
  F: Future<Output = T::Options>,
  T: Transport<Runtime = R>,
  R: Runtime,
{
  let cidrs = CIDRsPolicy::try_from(["127.0.0.0/24", "127.0.1.0/24"].as_slice()).unwrap();
  let m1 = Memberlist::<T, _>::new(get_transport(0, cidrs.clone()).await, opts.clone())
    .await
    .unwrap();

  // Create a second node
  let m2 = Memberlist::<T, _>::new(get_transport(1, cidrs.clone()).await, opts.clone())
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
  let m3 = Memberlist::<T, _>::new(get_transport(2, cidrs1).await, opts.clone())
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
  let m4 = Memberlist::<T, _>::new(get_transport(2, cidrs.clone()).await, opts)
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
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_different_networks_multi_masks >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_multi_masks::<_, QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|idx, cidrs| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(format!("join_different_networks_multi_masks_node_{idx}").into(), $expr)
              .with_cidrs_policy(cidrs);
            t1_opts.add_bind_address(next_socket_addr_v4(idx as u8));
            t1_opts
          }, Options::lan()).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_different_networks_multi_masks_with_compression >]() {
        [< $rt:snake _run >](async move {
          join_different_networks_multi_masks::<_, QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|idx, cidrs| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(format!("join_different_networks_multi_masks_node_{idx}").into(), $expr)
              .with_cidrs_policy(cidrs);
            t1_opts.add_bind_address(next_socket_addr_v4(idx as u8));
            t1_opts
          }, Options::lan().with_compress_algo(Default::default())).await;
        });
      }
    }
  };
}

test_mods!(join_different_networks_multi_masks);
