#![allow(clippy::await_holding_lock)]

use std::{
  collections::HashMap,
  future::Future,
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::Duration,
};

use agnostic::Runtime;
use memberlist_core::{
  Memberlist,
  delegate::{CompositeDelegate, mock::MockDelegate},
  proto::MaybeResolvedAddress,
  tests::AddressKind,
};
use memberlist_net::stream_layer::StreamLayer;

use super::*;

type NetTransport<S, R> = memberlist_net::NetTransport<SmolStr, SocketAddrResolver<R>, S, R>;
type VoidDelegate = memberlist_core::delegate::VoidDelegate<SmolStr, SocketAddr>;
type Delegate = CompositeDelegate<
  SmolStr,
  SocketAddr,
  VoidDelegate,
  VoidDelegate,
  VoidDelegate,
  VoidDelegate,
  MockDelegate<SmolStr, SocketAddr>,
>;

/// This test should follow the recommended upgrade guide:
/// https://www.consul.io/docs/agent/encryption.html#configuring-gossip-encryption-on-an-existing-cluster
///
/// We will use two nodes for this: m0 and m1
///
/// 0. Start with nodes without encryption.
/// 1. Set an encryption key and set `gossip_verify_incoming=false` and `gossip_verify_outgoing=false` to all nodes.
/// 2. Change `gossip_verify_outgoing=true` to all nodes.
/// 3. Change `gossip_verify_incoming=true` to all nodes.
async fn encrypted_gossip_transition<F, S, R>(mut create_stream_layer: impl FnMut() -> F + Copy)
where
  F: Future<Output = S::Options>,
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let pretty = Arc::new(Mutex::new(HashMap::new()));
  let mut new_config = |short_name: SmolStr, addr: Option<SocketAddr>| {
    let s = create_stream_layer();
    let pretty = pretty.clone();
    async move {
      let opts = match addr {
        Some(addr) => {
          let mut opts =
            NetTransportOptions::with_stream_layer_options(short_name.clone(), s.await);
          opts.add_bind_address(addr);
          opts
        }
        None => {
          let addr = AddressKind::V4.next(0);
          let mut opts =
            NetTransportOptions::with_stream_layer_options(addr.to_string().into(), s.await);
          opts.add_bind_address(addr);
          opts
        }
      };

      // Set the gossip interval fast enough to get a reasonable test,
      // but slow enough to avoid "sendto: operation not permitted"

      let mopts = Options::lan().with_gossip_interval(Duration::from_millis(100));

      pretty.lock().unwrap().insert(opts.id().clone(), short_name);
      (mopts, opts)
    }
  };

  let create_ok = |opts: Options, topts: NetTransportOptions<SmolStr, SocketAddrResolver<R>, S>| async move {
    Memberlist::<NetTransport<S, R>, _>::with_delegate(
      CompositeDelegate::new().with_node_delegate(MockDelegate::new()),
      topts,
      opts,
    )
    .await
    .unwrap()
  };

  let join_ok = |src: Memberlist<NetTransport<S, R>, Delegate>,
                 dst: Memberlist<NetTransport<S, R>, Delegate>,
                 num_nodes: usize| {
    let pretty = pretty.clone();
    async move {
      let pretty = pretty.lock().unwrap();
      let src_name = pretty.get(src.local_id()).unwrap();
      let dst_name = pretty.get(dst.local_id()).unwrap();
      tracing::info!(
        "node {}[{}] is joining node {}[{}]",
        src_name,
        src.advertise_address(),
        dst_name,
        dst.advertise_address()
      );

      let t1 = Node::new(
        dst_name.clone(),
        MaybeResolvedAddress::resolved(*dst.advertise_address()),
      );

      src.join(t1).await.unwrap();

      wait_until_size(&src, num_nodes).await;
      wait_until_size(&dst, num_nodes).await;

      // Check the hosts
      assert_eq!(src.online_members().await.len(), num_nodes);
      assert_eq!(dst.online_members().await.len(), num_nodes);
    }
  };

  let leave_ok = |src: Memberlist<NetTransport<S, R>, Delegate>, why: String| {
    let name = pretty.lock().unwrap().get(src.local_id()).cloned().unwrap();
    async move {
      tracing::info!("node {}[{}] is leaving {}", name, src.local_id(), why);
      src.leave(Duration::from_secs(1)).await.unwrap();
    }
  };

  let shutdown_ok = |src: Memberlist<NetTransport<S, R>, Delegate>, why: String| {
    let pretty = pretty.lock().unwrap();
    async move {
      let name = pretty.get(src.local_id()).cloned().unwrap();
      tracing::info!(
        "node {}[{}] is shutting down {}",
        name,
        src.advertise_address(),
        why
      );
      src.shutdown().await.unwrap();
    }
  };

  let leave_and_shutdown = |leaver: Memberlist<NetTransport<S, R>, Delegate>,
                            bystander: Memberlist<NetTransport<S, R>, Delegate>,
                            why: String| async move {
    leave_ok(leaver.clone(), why.clone()).await;
    wait_until_size(&bystander, 1).await;
    shutdown_ok(leaver, why).await;
    wait_until_size(&bystander, 1).await;
  };

  // ==== STEP 0 ====

  // Create a first cluster of 2 nodes with no gossip encryption settings.
  let (conf0, topts) = new_config(SmolStr::from("m0"), None).await;
  let m0 = create_ok(conf0, topts).await;

  let (conf1, topts) = new_config(SmolStr::from("m1"), None).await;
  let m1 = create_ok(conf1, topts).await;

  join_ok(m0.clone(), m1.clone(), 2).await;

  tracing::info!("==== STEP 0 complete: two node unencrypted cluster ====");

  // ==== STEP 1 ====

  // Take down m0, upgrade to first stage of gossip transition settings.
  leave_and_shutdown(
    m0,
    m1.clone(),
    "to upgrade gossip encryption settings".to_string(),
  )
  .await;

  // Resurrect the first node with the first stage of gossip transition settings.
  let (conf0, topts) = new_config(SmolStr::from("m0"), None).await;
  let m0 = create_ok(
    conf0
      .with_primary_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
      .with_gossip_verify_incoming(false)
      .with_gossip_verify_outgoing(false),
    topts,
  )
  .await;

  // Join the second node. m1 has no encryption while m0 has encryption configured and
  // can receive encrypted gossip, but will not encrypt outgoing gossip.
  join_ok(m0.clone(), m1.clone(), 2).await;

  leave_and_shutdown(
    m1,
    m0.clone(),
    "to upgrade gossip to first stage".to_string(),
  )
  .await;

  // Resurrect the second node with the first stage of gossip transition settings.
  let (conf1, topts) = new_config(SmolStr::from("m1"), None).await;

  let m1 = create_ok(
    conf1
      .with_primary_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
      .with_gossip_verify_incoming(false)
      .with_gossip_verify_outgoing(false),
    topts,
  )
  .await;

  // Join the first node. Both have encryption configured and can receive
  // encrypted gossip, but will not encrypt outgoing gossip.
  join_ok(m0.clone(), m1.clone(), 2).await;

  tracing::info!("==== STEP 1 complete: two node encryption-aware cluster ====");

  // ==== STEP 2 ====

  // Take down m0, upgrade to second stage of gossip transition settings.
  leave_and_shutdown(
    m0,
    m1.clone(),
    "to upgrade gossip to second stage".to_string(),
  )
  .await;

  // Resurrect the first node with the second stage of gossip transition settings.
  let (conf0, topts) = new_config(SmolStr::from("m0"), None).await;

  let m0 = create_ok(
    conf0
      .with_primary_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
      .with_gossip_verify_incoming(false),
    topts,
  )
  .await;

  // Join the second node. At this step, both nodes have encryption
  // configured but only m0 is sending encrypted gossip.
  join_ok(m0.clone(), m1.clone(), 2).await;

  leave_and_shutdown(
    m1,
    m0.clone(),
    "to upgrade gossip to second stage".to_string(),
  )
  .await;

  // Resurrect the second node with the second stage of gossip transition settings.
  let (conf1, topts) = new_config(SmolStr::from("m1"), None).await;
  let m1 = create_ok(
    conf1
      .with_primary_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
      .with_gossip_verify_incoming(false),
    topts,
  )
  .await;

  // Join the first node. Both have encryption configured and can receive
  // encrypted gossip, and encrypt outgoing gossip, but aren't forcing
  // incoming gossip is encrypted.
  join_ok(m1.clone(), m0.clone(), 2).await;

  tracing::info!("==== STEP 2 complete: two node encryption-aware cluster being encrypted ====");

  // ==== STEP 3 ====

  // Take down m0, upgrade to final stage of gossip transition settings.
  leave_and_shutdown(
    m0,
    m1.clone(),
    "to upgrade gossip to final stage".to_string(),
  )
  .await;

  // Resurrect the first node with the final stage of gossip transition settings.
  let (conf0, topts) = new_config(SmolStr::from("m0"), None).await;
  let m0 = create_ok(
    conf0.with_primary_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg=="))),
    topts,
  )
  .await;

  // Join the second node. At this step, both nodes have encryption
  // configured and are sending it, bu tonly m0 is verifying inbound gossip
  // is encrypted.
  join_ok(m0.clone(), m1.clone(), 2).await;

  leave_and_shutdown(
    m1,
    m0.clone(),
    "to upgrade gossip to final stage".to_string(),
  )
  .await;

  // Resurrect the second node with the final stage of gossip transition settings.
  let (conf1, topts) = new_config(SmolStr::from("m1"), None).await;
  let m1 = create_ok(
    conf1.with_primary_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg=="))),
    topts,
  )
  .await;

  // Join the first node. Both have encryption configured and fully in
  // enforcement.
  join_ok(m1, m0, 2).await;

  tracing::info!("==== STEP 3 complete: two node encrypted cluster locked down ====");
}

macro_rules! encrypted_gossip_transition {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _encrypted_gossip_transition >]() {
        [< $rt:snake _run >](async move {
          encrypted_gossip_transition::<_, $layer<[< $rt:camel Runtime >]>, [< $rt:camel Runtime >]>(|| async move { $expr }).await;
        });
      }
    }
  };
}

test_mods!(encrypted_gossip_transition);
