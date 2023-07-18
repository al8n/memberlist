use std::net::{IpAddr, Ipv4Addr};

#[cfg(test)]
use agnostic::{async_std::AsyncStdRuntime, smol::SmolRuntime, tokio::TokioRuntime};
use parking_lot::Mutex;

use crate::{
  delegate::VoidDelegate,
  options::parse_cidrs,
  security::{EncryptionAlgo, SecretKey},
  transport::net::NetTransport,
  CompressionAlgo, Label,
};

use super::*;

static BIND_NUM: Mutex<u8> = Mutex::new(10u8);
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn get_bind_addr_net(network: u8) -> SocketAddr {
  let mut bind_num = BIND_NUM.lock();
  let ip = IpAddr::V4(Ipv4Addr::new(127, 0, network, *bind_num));

  *bind_num += 1;
  if *bind_num == 255 {
    *bind_num = 10;
  }

  SocketAddr::new(ip, 7949)
}

#[allow(dead_code)]
pub(crate) fn get_bind_addr() -> SocketAddr {
  get_bind_addr_net(0)
}

#[allow(dead_code)]
async fn yield_now<R: Runtime>() {
  R::sleep(Duration::from_millis(250)).await;
}

fn test_config_net<R: Runtime>(network: u8) -> Options<NetTransport<R>> {
  let bind_addr = get_bind_addr_net(network);
  Options::lan()
    .with_bind_addr(bind_addr.ip())
    .with_bind_port(Some(0))
    .with_name(Name::from_string(bind_addr.ip().to_string()).unwrap())
}

pub(crate) fn test_config<R: Runtime>() -> Options<NetTransport<R>> {
  test_config_net(0)
}

pub(crate) async fn get_showbiz<D: Delegate, F, R: Runtime>(
  f: Option<F>,
) -> Result<Showbiz<D, NetTransport<R>>, Error<D, NetTransport<R>>>
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

  Showbiz::new(c).await
}

pub async fn test_create_secret_key_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let cases = vec![
    ("size-16", SecretKey::Aes128([0; 16])),
    ("size-24", SecretKey::Aes192([0; 24])),
    ("size-32", SecretKey::Aes256([0; 32])),
  ];

  for (_, key) in cases {
    let c = Options::<NetTransport<R>>::lan()
      .with_bind_addr(get_bind_addr().ip())
      .with_secret_key(Some(key));

    get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
      .await
      .unwrap();
    yield_now::<R>().await;
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

pub async fn test_create_secret_key_empty_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(get_bind_addr().ip());

  get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
    .await
    .unwrap();
  yield_now::<R>().await;
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

pub async fn test_create_keyring_only_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(get_bind_addr().ip());

  let m = Showbiz::<VoidDelegate, NetTransport<R>>::with_keyring(
    SecretKeyring::new(SecretKey::Aes128([0; 16])),
    c,
  )
  .await
  .unwrap();

  yield_now::<R>().await;
  assert!(m.encryption_enabled());
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

pub async fn test_create_keyring_and_primary_key_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c = Options::<NetTransport<R>>::lan()
    .with_bind_addr(get_bind_addr().ip())
    .with_secret_key(Some(SecretKey::Aes128([1; 16])));

  let m = Showbiz::<VoidDelegate, NetTransport<R>>::with_keyring(
    SecretKeyring::new(SecretKey::Aes128([0; 16])),
    c,
  )
  .await
  .unwrap();

  yield_now::<R>().await;
  assert!(m.encryption_enabled());
  assert_eq!(
    m.inner.keyring.as_ref().unwrap().keys().next().unwrap(),
    SecretKey::Aes128([1; 16])
  );
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

pub async fn test_create_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(get_bind_addr().ip());
  let m = get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
    .await
    .unwrap();
  yield_now::<R>().await;
  assert_eq!(m.members().await.len(), 1);
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

pub async fn test_resolve_addr_runner() {
  let _mu = TEST_LOCK.lock();
  // let m = get_showbiz(None).await.unwrap();

  // struct TestCase {
  //   name: &'static str,
  //   in_: ,

  // }

  // let cases = vec![
  //   TestCase {

  //   },

  // ];

  // m.resolve_addr(addr, port)
  // for c in cases {
  //   m.resolve_addr(addr, port).await;
  // }
}

#[tracing_test::traced_test]
#[test]
fn test_resolve_addr() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_resolve_addr_runner());
  }
  {
    AsyncStdRuntime::block_on(test_resolve_addr_runner());
  }
  {
    SmolRuntime::block_on(test_resolve_addr_runner());
  }
}

async fn test_resolve_addr_tcp_first() {
  todo!()
}

async fn test_members() {
  todo!()
}

pub async fn test_join_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c1 = test_config::<R>().with_compression_algo(CompressionAlgo::None);
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  let c2 = test_config::<R>().with_compression_algo(CompressionAlgo::None);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join(
      [(
        (m1.inner.opts.bind_addr, m1.inner.opts.bind_port.unwrap()).into(),
        m1.inner.opts.name.clone(),
      )]
      .into_iter(),
    )
    .await
    .unwrap();
  assert_eq!(num, 1);
}

#[tracing_test::traced_test]
#[test]
fn test_join() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_runner::<TokioRuntime>());
  }

  // TODO: fix async-std runtime
  // {
  //   AsyncStdRuntime::block_on(test_join_runner::<AsyncStdRuntime>());
  // }

  // TODO: fix smol runtime
  // {
  //   SmolRuntime::block_on(test_join_runner::<SmolRuntime>());
  // }
}

pub async fn test_join_with_compression_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c1 = test_config::<R>();
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  let c2 = test_config::<R>().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap();
  assert_eq!(num, 1);
}

#[tracing_test::traced_test]
#[test]
fn test_join_with_compression() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_with_compression_runner::<TokioRuntime>());
  }

  // {
  //   AsyncStdRuntime::block_on(test_join_runner::<AsyncStdRuntime>());
  // }
  // {
  //   SmolRuntime::block_on(test_join_runner::<SmolRuntime>());
  // }
}

pub async fn test_join_with_encryption_runner<R>(algo: EncryptionAlgo)
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c1 = test_config::<R>()
    .with_compression_algo(CompressionAlgo::None)
    .with_encryption_algo(algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])));
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  let c2 = test_config::<R>()
    .with_compression_algo(CompressionAlgo::None)
    .with_encryption_algo(algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap();
  assert_eq!(num, 1);
}

#[tracing_test::traced_test]
#[test]
fn test_join_with_encryption() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_with_encryption_runner::<TokioRuntime>(
      EncryptionAlgo::None,
    ));
    runtime.block_on(test_join_with_encryption_runner::<TokioRuntime>(
      EncryptionAlgo::NoPadding,
    ));
    runtime.block_on(test_join_with_encryption_runner::<TokioRuntime>(
      EncryptionAlgo::PKCS7,
    ));
  }
  // {
  //   AsyncStdRuntime::block_on(test_join_with_encryption_runner::<AsyncStdRuntime>(EncryptionAlgo::None));
  //   AsyncStdRuntime::block_on(test_join_with_encryption_runner::<AsyncStdRuntime>(EncryptionAlgo::NoPadding));
  //   AsyncStdRuntime::block_on(test_join_with_encryption_runner::<AsyncStdRuntime>(EncryptionAlgo::PKCS7));
  // }
  // {
  //   SmolRuntime::block_on(test_join_with_encryption_runner::<AsyncStdRuntime>(EncryptionAlgo::None));
  //   SmolRuntime::block_on(test_join_with_encryption_runner::<AsyncStdRuntime>(EncryptionAlgo::NoPadding));
  //   SmolRuntime::block_on(test_join_with_encryption_runner::<AsyncStdRuntime>(EncryptionAlgo::PKCS7));
  // }
}

pub async fn test_join_with_encryption_and_compression_runner<R>(
  encryption_algo: EncryptionAlgo,
  compression_algo: CompressionAlgo,
) where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c1 = test_config::<R>()
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])));
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  let c2 = test_config::<R>()
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap();

  assert_eq!(num, 1);
}

#[tracing_test::traced_test]
#[test]
fn test_join_with_encryption_and_compression() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_with_encryption_and_compression_runner::<
      TokioRuntime,
    >(EncryptionAlgo::NoPadding, CompressionAlgo::Lzw));
    runtime.block_on(test_join_with_encryption_and_compression_runner::<
      TokioRuntime,
    >(EncryptionAlgo::PKCS7, CompressionAlgo::Lzw));
  }
  // {
  //   AsyncStdRuntime::block_on(test_join_with_encryption_and_compression_runner::<AsyncStdRuntime>(EncryptionAlgo::NoPadding, CompressionAlgo::Lzw));
  //   AsyncStdRuntime::block_on(test_join_with_encryption_and_compression_runner::<AsyncStdRuntime>(EncryptionAlgo::PKCS7, CompressionAlgo::Lzw));
  // }
  // {
  //   SmolRuntime::block_on(test_join_with_encryption_and_compression_runner::<AsyncStdRuntime>(EncryptionAlgo::NoPadding, CompressionAlgo::Lzw));
  //   SmolRuntime::block_on(test_join_with_encryption_and_compression_runner::<AsyncStdRuntime>(EncryptionAlgo::PKCS7, CompressionAlgo::Lzw));
  // }
}

const TEST_KEYS: &[SecretKey] = &[
  SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
  SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
  SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
];

pub async fn test_join_with_labels<R>(
  encryption_algo: EncryptionAlgo,
  compression_algo: CompressionAlgo,
  key: Option<SecretKey>,
) where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let c1 = test_config::<R>()
    .with_label(Label::from_static("blah"))
    .with_encryption_algo(encryption_algo)
    .with_compression_algo(compression_algo)
    .with_name("node1".try_into().unwrap())
    .with_secret_key(key);
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  // Create a second node
  let c2 = test_config::<R>()
    .with_name("node2".try_into().unwrap())
    .with_label(Label::from_static("blah"))
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(key)
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join(std::iter::once((
      m1.inner.opts.bind_addr.into(),
      Name::from_str_unchecked("node1"),
    )))
    .await
    .unwrap();
  assert_eq!(num, 1);

  // Check the hosts
  // assert_eq!(m1.alive_members().await, 2);
  assert_eq!(m1.estimate_num_nodes(), 2);
  // assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);

  // Create a third node that uses no label
  let c3 = test_config::<R>()
    .with_name("node3".try_into().unwrap())
    .with_label(Label::from_static(""))
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(key)
    .with_bind_port(m1.inner.opts.bind_port);
  let m3 = Showbiz::<VoidDelegate, _>::new(c3).await.unwrap();
  let JoinError { num_joined, .. } = m3
    .join(std::iter::once((
      m1.inner.opts.bind_addr.into(),
      Name::from_str_unchecked("node1"),
    )))
    .await
    .unwrap_err();
  assert_eq!(num_joined, 0);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 2);
  assert_eq!(m1.estimate_num_nodes(), 2);
  assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);
  assert_eq!(m3.alive_members().await, 1);
  assert_eq!(m3.estimate_num_nodes(), 1);

  // Create a fourth node that uses a mismatched label
  let c4 = test_config::<R>()
    .with_name("node4".try_into().unwrap())
    .with_label(Label::from_static("not-blah"))
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(key)
    .with_bind_port(m1.inner.opts.bind_port);
  let m4 = Showbiz::<VoidDelegate, _>::new(c4).await.unwrap();

  let JoinError { num_joined, .. } = m4
    .join(std::iter::once((
      m1.inner.opts.bind_addr.into(),
      Name::from_str_unchecked("node1"),
    )))
    .await
    .unwrap_err();
  assert_eq!(num_joined, 0);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 2);
  assert_eq!(m1.estimate_num_nodes(), 2);
  assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);
  assert_eq!(m3.alive_members().await, 1);
  assert_eq!(m3.estimate_num_nodes(), 1);
  assert_eq!(m4.alive_members().await, 1);
  assert_eq!(m4.estimate_num_nodes(), 1);
}

#[tracing_test::traced_test]
#[test]
fn _test_join_with_labels() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::None,
      CompressionAlgo::None,
      None,
    ));
  }
}

#[tracing_test::traced_test]
#[test]
fn _test_join_with_labels_and_compression() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::None,
      CompressionAlgo::Lzw,
      None,
    ));
  }
}

#[tracing_test::traced_test]
#[test]
fn _test_join_with_labels_and_encryption() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::NoPadding,
      CompressionAlgo::None,
      Some(TEST_KEYS[0]),
    ));
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::PKCS7,
      CompressionAlgo::None,
      Some(TEST_KEYS[0]),
    ));
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::None,
      CompressionAlgo::None,
      Some(TEST_KEYS[0]),
    ));
  }
}

#[tracing_test::traced_test]
#[test]
fn _test_join_with_labels_and_compression_and_encryption() {
  {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::NoPadding,
      CompressionAlgo::Lzw,
      Some(TEST_KEYS[0]),
    ));
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::PKCS7,
      CompressionAlgo::Lzw,
      Some(TEST_KEYS[0]),
    ));
    runtime.block_on(test_join_with_labels::<TokioRuntime>(
      EncryptionAlgo::None,
      CompressionAlgo::Lzw,
      Some(TEST_KEYS[0]),
    ));
  }
}

pub async fn test_join_different_networks_unique_mask_runner<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let _mu = TEST_LOCK.lock();
  let (cidrs, _) = parse_cidrs(&["127.0.0.0/8"]);
  let c1 = test_config_net::<R>(0).with_allowed_cidrs(Some(cidrs.clone()));
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  // Create a second node
  let c2 = test_config_net::<R>(1).with_allowed_cidrs(Some(cidrs));
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let Ok(num) = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
  else {
    panic!("join failed");
  };

  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);
}

#[tracing_test::traced_test]
#[test]
fn test_join_different_networks_unique_mask() {
  // {
  //   let runtime = tokio::runtime::Runtime::new().unwrap();
  //   runtime.block_on(test_join_different_networks_unique_mask_runner::<
  //     TokioRuntime,
  //   >());
  // }

  // {
  //   AsyncStdRuntime::block_on(test_join_runner::<AsyncStdRuntime>());
  // }
  // {
  //   SmolRuntime::block_on(test_join_runner::<SmolRuntime>());
  // }
}

async fn test_join_different_networks_multi_masks() {
  todo!()
}

async fn test_join_cancel() {
  todo!()
}

async fn test_join_cancel_passive() {
  todo!()
}

async fn test_leave() {
  todo!()
}

async fn test_join_shutdown() {
  todo!()
}

async fn test_join_dead_node() {
  todo!()
}

async fn test_join_ipv6() {
  todo!()
}

async fn test_delegate_meta() {
  todo!()
}

async fn test_delegate_meta_update() {
  todo!()
}

async fn test_user_data() {
  todo!()
}

async fn test_send_to() {
  todo!()
}

async fn test_advertise_addr() {
  todo!()
}

async fn test_conflict_delegate() {
  todo!()
}

async fn test_ping_delegate() {
  todo!()
}

async fn test_encrypted_gossip_transition() {
  todo!()
}
