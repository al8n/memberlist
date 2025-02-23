use std::future::Future;

use agnostic::Runtime;
use memberlist::{transport::MaybeResolvedAddress, Memberlist};
use memberlist_net::{compressor::Compressor, Label};

use super::*;

/// Unit tests for join a `Memberlist` with labels.
pub async fn memberlist_join_with_labels_and_compression_and_encryption<F, T, R>(
  mut get_transport: impl FnMut(usize, Label, Compressor, SecretKey) -> F,
) where
  F: Future<Output = T::Options>,
  T: Transport<Runtime = R>,
  R: Runtime,
{
  let label1 = Label::try_from("blah").unwrap();
  let m1 = Memberlist::<T, _>::new(
    get_transport(1, label1.clone(), Compressor::default(), TEST_KEYS[0]).await,
    Options::lan(),
  )
  .await
  .unwrap();
  let m2 = Memberlist::<T, _>::new(
    get_transport(2, label1.clone(), Compressor::default(), TEST_KEYS[0]).await,
    Options::lan(),
  )
  .await
  .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  m2.join(target.clone()).await.unwrap();

  let m1m = m1.num_online_members().await;
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);

  let m2m = m2.num_online_members().await;
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);

  // Create a third node that uses no label
  let m3 = Memberlist::<T, _>::new(
    get_transport(3, Label::empty(), Compressor::default(), TEST_KEYS[0]).await,
    Options::lan(),
  )
  .await
  .unwrap();
  m3.join(target.clone()).await.unwrap_err();

  let m1m = m1.num_online_members().await;
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);

  let m2m = m2.num_online_members().await;
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);

  let m3m = m3.num_online_members().await;
  assert_eq!(m3m, 1, "expected 1 member, got {}", m3m);

  // Create a fourth node that uses a mismatched label
  let label = Label::try_from("not-blah").unwrap();
  let m4 = Memberlist::<T, _>::new(
    get_transport(4, label, Compressor::default(), TEST_KEYS[0]).await,
    Options::lan(),
  )
  .await
  .unwrap();
  m4.join(target).await.unwrap_err();

  let m1m = m1.num_online_members().await;
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);

  let m2m = m2.num_online_members().await;
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);

  let m3m = m3.num_online_members().await;
  assert_eq!(m3m, 1, "expected 1 member, got {}", m3m);

  let m4m = m4.num_online_members().await;
  assert_eq!(m4m, 1, "expected 1 member, got {}", m4m);

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
  m4.shutdown().await.unwrap();
}

macro_rules! join_with_labels_and_compression_and_encryption {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_with_labels >]() {
        [< $rt:snake _run >](async move {
          memberlist_join_with_labels_and_compression_and_encryption::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|idx, label, compressor, pk| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(format!("join_with_labels_and_compression_and_encryption_node_{idx}").into(), $expr)
              .with_label(label)
              .with_compressor(Some(compressor))
              .with_primary_key(Some(pk));
            t1_opts.add_bind_address(next_socket_addr_v4(0));
            t1_opts
          }).await;
        });
      }
    }
  };
}

test_mods!(join_with_labels_and_compression_and_encryption);
