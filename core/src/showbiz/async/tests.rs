use std::{
  net::{IpAddr, Ipv4Addr},
  sync::Mutex,
};

use agnostic::{async_std::AsyncStdRuntime, smol::SmolRuntime, tokio::TokioRuntime};

use crate::{delegate::VoidDelegate, security::SecretKey, transport::net::NetTransport};

use super::*;

static BIND_NUM: Mutex<u8> = Mutex::new(10u8);
static LOCALHOST: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

fn get_bind_addr_net(network: u8) -> IpAddr {
  let mut bind_num = BIND_NUM.lock().unwrap();
  let ip = IpAddr::V4(Ipv4Addr::new(127, 0, network, *bind_num));

  *bind_num += 1;
  if *bind_num == 255 {
    *bind_num = 10;
  }

  ip
}

#[allow(dead_code)]
fn get_bind_addr() -> IpAddr {
  get_bind_addr_net(0)
}

#[allow(dead_code)]
async fn yield_now<R: Runtime>() {
  R::sleep(Duration::from_millis(250)).await;
}

fn test_config_net<R: Runtime>(network: u8) -> Options<NetTransport<R>> {
  let bind_addr = SocketAddr::new(get_bind_addr_net(network), 0);
  Options::lan()
    .with_bind_addr(bind_addr)
    .with_name(bind_addr.to_string().try_into().unwrap())
}

fn test_config<R: Runtime>() -> Options<NetTransport<R>> {
  test_config_net(0)
}

pub(crate) async fn get_showbiz<D: Delegate, F, R: Runtime>(
  f: Option<F>,
) -> Result<Showbiz<D, NetTransport<R>, R>, Error<D, NetTransport<R>>>
where
  F: FnOnce(Options<NetTransport<R>>) -> Options<NetTransport<R>>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = if let Some(f) = f {
    f(test_config())
  } else {
    test_config()
  };

  Showbiz::<_, _, R>::new(c).await
}

async fn test_create_secret_key_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let cases = vec![
    ("size-16", SecretKey::Aes128([0; 16])),
    ("size-24", SecretKey::Aes192([0; 24])),
    ("size-32", SecretKey::Aes256([0; 32])),
  ];

  for (name, key) in cases {
    let c = Options::<NetTransport<R>>::lan()
      .with_bind_addr(LOCALHOST)
      .with_secret_key(Some(key));

    let m = get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
      .await
      .unwrap();
    if let Err(e) = m.bootstrap().await {
      panic!("name: {} key '{:?}' error: {:?}", name, key, e);
    }
    yield_now::<R>().await;
    assert!(m.shutdown().await.is_ok());
  }
}

#[test]
fn test_create_secret_key() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_create_secret_key_runner::<TokioRuntime>());
  }
  {
    AsyncStdRuntime::block_on(test_create_secret_key_runner::<AsyncStdRuntime>());
  }
  {
    SmolRuntime::block_on(test_create_secret_key_runner::<SmolRuntime>());
  }
}

async fn test_create_secret_key_empty_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(LOCALHOST);

  let m = get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
    .await
    .unwrap();
  m.bootstrap().await.unwrap();
  yield_now::<R>().await;
  assert!(m.shutdown().await.is_ok());
}

#[test]
fn test_create_secret_key_empty() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_create_secret_key_empty_runner::<TokioRuntime>());
  }
  {
    AsyncStdRuntime::block_on(test_create_secret_key_empty_runner::<AsyncStdRuntime>());
  }
  {
    SmolRuntime::block_on(test_create_secret_key_empty_runner::<SmolRuntime>());
  }
}

async fn test_create_keyring_only_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(LOCALHOST);

  let m = Showbiz::<VoidDelegate, NetTransport<R>, R>::with_keyring(
    SecretKeyring::new(SecretKey::Aes128([0; 16])),
    c,
  )
  .await
  .unwrap();

  m.bootstrap().await.unwrap();
  yield_now::<R>().await;
  assert!(m.encryption_enabled());
  assert!(m.shutdown().await.is_ok());
}

#[test]
fn test_create_keyring_only() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_create_keyring_only_runner::<TokioRuntime>());
  }
  {
    AsyncStdRuntime::block_on(test_create_keyring_only_runner::<AsyncStdRuntime>());
  }
  {
    SmolRuntime::block_on(test_create_keyring_only_runner::<SmolRuntime>());
  }
}

async fn test_create_keyring_and_primary_key_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan()
    .with_bind_addr(LOCALHOST)
    .with_secret_key(Some(SecretKey::Aes128([1; 16])));

  let m = Showbiz::<VoidDelegate, NetTransport<R>, R>::with_keyring(
    SecretKeyring::new(SecretKey::Aes128([0; 16])),
    c,
  )
  .await
  .unwrap();

  m.bootstrap().await.unwrap();
  yield_now::<R>().await;
  assert!(m.encryption_enabled());
  assert_eq!(
    m.inner.keyring.as_ref().unwrap().keys().next().unwrap(),
    SecretKey::Aes128([1; 16])
  );
  assert!(m.shutdown().await.is_ok());
}

#[test]
fn test_create_keyring_and_primary_key() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_create_keyring_and_primary_key_runner::<TokioRuntime>());
  }
  {
    AsyncStdRuntime::block_on(test_create_keyring_and_primary_key_runner::<AsyncStdRuntime>());
  }
  {
    SmolRuntime::block_on(test_create_keyring_and_primary_key_runner::<SmolRuntime>());
  }
}

async fn test_create_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(LOCALHOST);
  let m = get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
    .await
    .unwrap();
  m.bootstrap().await.unwrap();
  yield_now::<R>().await;
  assert_eq!(m.members().await.len(), 1);
  assert!(m.shutdown().await.is_ok());
}

#[test]
fn test_create() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_create_runner::<TokioRuntime>());
  }
  {
    AsyncStdRuntime::block_on(test_create_runner::<AsyncStdRuntime>());
  }
  {
    SmolRuntime::block_on(test_create_runner::<SmolRuntime>());
  }
}

#[tokio::test]
async fn test_resolve_addr() {
  todo!()
}

#[tokio::test]
async fn test_resolve_addr_tcp_first() {
  todo!()
}

#[tokio::test]
async fn test_members() {
  todo!()
}

#[tokio::test]
async fn test_join() {
  todo!()
}

#[tokio::test]
async fn test_join_with_labels() {
  todo!()
}

#[tokio::test]
async fn test_join_with_labels_and_encryption() {
  todo!()
}

#[tokio::test]
async fn test_join_different_networks_unique_mask() {
  todo!()
}

#[tokio::test]
async fn test_join_different_networks_multi_masks() {
  todo!()
}

#[tokio::test]
async fn test_join_cancel() {
  todo!()
}

#[tokio::test]
async fn test_join_cancel_passive() {
  todo!()
}

#[tokio::test]
async fn test_leave() {
  todo!()
}

#[tokio::test]
async fn test_join_shutdown() {
  todo!()
}

#[tokio::test]
async fn test_join_dead_node() {
  todo!()
}

#[tokio::test]
async fn test_join_ipv6() {
  todo!()
}

#[tokio::test]
async fn test_delegate_meta() {
  todo!()
}

#[tokio::test]
async fn test_delegate_meta_update() {
  todo!()
}

#[tokio::test]
async fn test_user_data() {
  todo!()
}

#[tokio::test]
async fn test_send_to() {
  todo!()
}

#[tokio::test]
async fn test_advertise_addr() {
  todo!()
}

#[tokio::test]
async fn test_conflict_delegate() {
  todo!()
}

#[tokio::test]
async fn test_ping_delegate() {
  todo!()
}

#[tokio::test]
async fn test_encrypted_gossip_transition() {
  todo!()
}
