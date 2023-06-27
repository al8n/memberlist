use agnostic::{async_std::AsyncStdRuntime, smol::SmolRuntime, tokio::TokioRuntime, Runtime};
use futures_util::{Future, Stream};

use crate::{
  delegate::VoidDelegate,
  error::Error,
  security::{SecretKey, SecurityError},
  showbiz::tests::{get_bind_addr, get_showbiz, test_config},
  transport::TransportError,
  Name,
};

async fn test_handle_compound_ping() {
  todo!()
}

async fn test_handle_ping() {
  todo!()
}

async fn test_handle_ping_wrong_node() {
  todo!()
}

async fn test_indirect_ping() {
  todo!()
}

async fn test_tcp_ping() {
  todo!()
}

async fn test_tcp_push_pull() {
  todo!()
}

async fn test_send_msg_piggy_back() {
  todo!()
}

async fn test_encrypt_decrypt_state() {
  todo!()
}

async fn test_raw_send_udp_crc() {
  todo!()
}

async fn test_ingest_packet_crc() {
  todo!()
}

async fn test_ingest_packet_exported_func_empty_message() {
  todo!()
}

pub async fn test_gossip_mismatched_keys_runner<R: Runtime>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = test_config::<R>()
    .with_secret_key(Some(SecretKey::Aes192(*b"4W6DGn2VQVqDEceOdmuRTQ==")))
    .with_name(Name::try_from("m1").unwrap());

  let m1 = get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
    .await
    .unwrap();
  m1.bootstrap().await.unwrap();

  let mut c2 = test_config::<R>()
    .with_secret_key(Some(SecretKey::Aes192(*b"XhX/w702/JKKK7/7OtM9Ww==")))
    .with_name(Name::try_from("m2").unwrap());
  let mut bind_addr = c2.bind_addr;
  bind_addr.set_port(m1.inner.opts.bind_addr.port());
  c2.bind_addr = bind_addr;

  let m2 = get_showbiz::<VoidDelegate, _, R>(Some(|_| c2))
    .await
    .unwrap();
  m2.bootstrap().await.unwrap();

  // Make sure we get this error on the joining side
  if let Ok((num, e)) = m2
    .join(
      [(
        m1.inner.opts.bind_addr.into(),
        Name::try_from("m1").unwrap(),
      )]
      .into_iter()
      .collect(),
    )
    .await
  {
    assert_eq!(num, 0);

    for (_, err) in e {
      if let Error::Transport(TransportError::Security(err)) = &err[0] {
        assert_eq!(err, &SecurityError::NoInstalledKeys);
      } else {
        panic!("expected SecurityError::NoInstalledKeys, got {:?}", err);
      }
    }
  }
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

#[tracing_test::traced_test]
#[test]
fn test_gossip_mismatched_keys() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_gossip_mismatched_keys_runner::<TokioRuntime>());
  }
  // {
  //   AsyncStdRuntime::block_on(test_gossip_mismatched_keys_runner::<AsyncStdRuntime>());
  // }
  // {
  //   SmolRuntime::block_on(test_gossip_mismatched_keys_runner::<SmolRuntime>());
  // }
}

async fn test_handle_command() {
  todo!()
}
