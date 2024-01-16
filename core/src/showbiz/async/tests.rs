use std::{
  cell::RefCell,
  net::{IpAddr, Ipv4Addr},
  sync::Mutex,
};

use agnostic::Delay;
use bytes::{BufMut, BytesMut};

use crate::{
  delegate::{MockDelegate, MockDelegateError, VoidDelegate},
  options::parse_cidrs,
  security::{EncryptionAlgo, SecretKey},
  transport::net::NetTransport,
  CompressionAlgo, Label, ServerState,
};

use super::*;

static BIND_NUM: Mutex<u8> = Mutex::new(10u8);

fn get_bind_addr_net(network: u8) -> SocketAddr {
  let mut bind_num = BIND_NUM.lock().unwrap();
  let ip = IpAddr::V4(Ipv4Addr::new(127, 0, network, *bind_num));

  *bind_num += 1;
  if *bind_num == 255 {
    *bind_num = 10;
  }

  SocketAddr::new(ip, 7949)
}

pub(crate) fn get_bind_addr() -> SocketAddr {
  get_bind_addr_net(0)
}

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

pub(crate) async fn get_showbiz<F, R: Runtime>(
  f: Option<F>,
) -> Result<Showbiz<NetTransport<R>>, Error<NetTransport<R>, VoidDelegate>>
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

  Showbiz::new_in(None, c).await.map(|(_, _, this)| this)
}

pub async fn test_create_secret_key<R>()
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

  for (_, key) in cases {
    let c = Options::<NetTransport<R>>::lan()
      .with_bind_addr(get_bind_addr().ip())
      .with_secret_key(Some(key));

    get_showbiz::<_, R>(Some(|_| c)).await.unwrap();
    yield_now::<R>().await;
  }
}

pub async fn test_create_secret_key_empty<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(get_bind_addr().ip());

  get_showbiz::<_, R>(Some(|_| c)).await.unwrap();
  yield_now::<R>().await;
}

pub async fn test_create_keyring_only<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan()
    .with_bind_addr(get_bind_addr().ip())
    .with_secret_keyring(Some(SecretKeyring::new(SecretKey::Aes128([0; 16]))));

  let m = Showbiz::<NetTransport<R>>::new(c).await.unwrap();

  yield_now::<R>().await;
  assert!(m.encryption_enabled());
}

pub async fn test_create_keyring_and_primary_key<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan()
    .with_bind_addr(get_bind_addr().ip())
    .with_secret_key(Some(SecretKey::Aes128([1; 16])))
    .with_secret_keyring(Some(SecretKeyring::new(SecretKey::Aes128([0; 16]))));

  let m = Showbiz::<NetTransport<R>>::new(c).await.unwrap();

  yield_now::<R>().await;
  assert!(m.encryption_enabled());
  assert_eq!(
    m.inner
      .opts
      .secret_keyring
      .as_ref()
      .unwrap()
      .keys()
      .next()
      .unwrap(),
    SecretKey::Aes128([1; 16])
  );
}

pub async fn test_create<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(get_bind_addr().ip());
  let m = Showbiz::new(c).await.unwrap();
  yield_now::<R>().await;
  assert_eq!(m.members().await.len(), 1);
}

pub async fn test_resolve_addr<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  todo!()
}

pub async fn test_resolve_addr_tcp_first<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  todo!()
}

pub async fn test_join<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>().with_compression_algo(CompressionAlgo::None);
  let m1 = Showbiz::new(c1).await.unwrap();

  let c2 = test_config::<R>().with_compression_algo(CompressionAlgo::None);
  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many(
      [(
        (m1.inner.opts.bind_addr, m1.inner.opts.bind_port.unwrap()).into(),
        m1.inner.opts.name.clone(),
      )]
      .into_iter(),
    )
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);
}

pub async fn test_join_with_compression<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>();
  let m1 = Showbiz::new(c1).await.unwrap();

  let c2 = test_config::<R>().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);
}

pub async fn test_join_with_encryption<R>(algo: EncryptionAlgo)
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>()
    .with_compression_algo(CompressionAlgo::None)
    .with_encryption_algo(algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])));
  let m1 = Showbiz::new(c1).await.unwrap();

  let c2 = test_config::<R>()
    .with_compression_algo(CompressionAlgo::None)
    .with_encryption_algo(algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);
}

pub async fn test_join_with_encryption_and_compression<R>(
  encryption_algo: EncryptionAlgo,
  compression_algo: CompressionAlgo,
) where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>()
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])));
  let m1 = Showbiz::new(c1).await.unwrap();

  let c2 = test_config::<R>()
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(Some(SecretKey::Aes128([0; 16])))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();

  assert_eq!(num, 1);
}

pub const TEST_KEYS: &[SecretKey] = &[
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
  let c1 = test_config::<R>()
    .with_label(Label::from_static("blah"))
    .with_encryption_algo(encryption_algo)
    .with_compression_algo(compression_algo)
    .with_name("node1".try_into().unwrap())
    .with_secret_key(key);
  let m1 = Showbiz::new(c1).await.unwrap();

  // Create a second node
  let c2 = test_config::<R>()
    .with_name("node2".try_into().unwrap())
    .with_label(Label::from_static("blah"))
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(key)
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many(std::iter::once((
      m1.inner.opts.bind_addr.into(),
      Name::from_str_unchecked("node1"),
    )))
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 2);
  assert_eq!(m1.estimate_num_nodes(), 2);
  assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);

  // Create a third node that uses no label
  let c3 = test_config::<R>()
    .with_name("node3".try_into().unwrap())
    .with_label(Label::from_static(""))
    .with_compression_algo(compression_algo)
    .with_encryption_algo(encryption_algo)
    .with_secret_key(key)
    .with_bind_port(m1.inner.opts.bind_port);
  let m3 = Showbiz::new(c3).await.unwrap();
  let JoinError { joined, .. } = m3
    .join_many(std::iter::once((
      m1.inner.opts.bind_addr.into(),
      Name::from_str_unchecked("node1"),
    )))
    .await
    .unwrap_err();
  assert_eq!(joined.len(), 0);

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
  let m4 = Showbiz::new(c4).await.unwrap();

  let JoinError { joined, .. } = m4
    .join_many(std::iter::once((
      m1.inner.opts.bind_addr.into(),
      Name::from_str_unchecked("node1"),
    )))
    .await
    .unwrap_err();
  assert_eq!(joined.len(), 0);

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

pub async fn test_join_different_networks_unique_mask<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (cidrs, _) = parse_cidrs(&["127.0.0.0/8"]);
  let c1 = test_config_net::<R>(0).with_allowed_cidrs(Some(cidrs.clone()));
  let m1 = Showbiz::new(c1).await.unwrap();

  // Create a second node
  let c2 = test_config_net::<R>(1)
    .with_allowed_cidrs(Some(cidrs))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();

  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);
}

pub async fn test_join_different_networks_multi_masks<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let (cidrs, _) = parse_cidrs(&["127.0.0.0/24", "127.0.1.0/24"]);
  let c1 = test_config_net::<R>(0).with_allowed_cidrs(Some(cidrs.clone()));
  let m1 = Showbiz::new(c1).await.unwrap();

  // Create a second node
  let c2 = test_config_net::<R>(1)
    .with_allowed_cidrs(Some(cidrs.clone()))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();
  join_and_test_member_ship(
    &m2,
    [(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter(),
    2,
  )
  .await;
  // Create a rogue node that allows all networks
  // It should see others, but will not be seen by others
  let c3 = test_config_net::<R>(2)
    .with_allowed_cidrs(Some(parse_cidrs(&["127.0.0.0/8"]).0))
    .with_bind_port(m1.inner.opts.bind_port);
  let m3 = Showbiz::new(c3).await.unwrap();
  // The rogue can see others, but others cannot see it
  join_and_test_member_ship(
    &m3,
    [(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter(),
    3,
  )
  .await;

  // m1 and m2 should not see newcomer however
  assert_eq!(m1.alive_members().await, 2);
  assert_eq!(m1.estimate_num_nodes(), 2);

  assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);

  // Another rogue, this time with a config that denies itself
  // Create a rogue node that allows all networks
  // It should see others, but will not be seen by others
  let c4 = test_config_net::<R>(2)
    .with_allowed_cidrs(Some(cidrs.clone()))
    .with_bind_port(m1.inner.opts.bind_port);
  let m4 = Showbiz::new(c4).await.unwrap();
  // This time, the node should not even see itself, so 2 expected nodes
  join_and_test_member_ship(
    &m4,
    [
      (m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone()),
      (m2.inner.opts.bind_addr.into(), m2.inner.opts.name.clone()),
    ]
    .into_iter(),
    2,
  )
  .await;

  // m1 and m2 should not see newcomer however
  assert_eq!(m1.alive_members().await, 2);
  assert_eq!(m1.estimate_num_nodes(), 2);
  assert_eq!(m2.alive_members().await, 2);
  assert_eq!(m2.estimate_num_nodes(), 2);
}

async fn join_and_test_member_ship<D: Delegate, R>(
  this: &Showbiz<NetTransport<R>, D>,
  members_to_join: impl Iterator<Item = (Address, Name)>,
  expected_members: usize,
) where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let members_to_join_num = members_to_join.size_hint().0;
  let num = this.join_many(members_to_join).await.unwrap().len();
  assert_eq!(num, members_to_join_num);
  assert_eq!(this.members().await.len(), expected_members);
}

pub async fn test_join_cancel<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>();
  let m1 = Showbiz::with_delegate(MockDelegate::cancel_merge(), c1)
    .await
    .unwrap();

  let c2 = test_config::<R>().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::with_delegate(MockDelegate::cancel_merge(), c2)
    .await
    .unwrap();

  let err = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap_err();
  // JoinError
  assert_eq!(err.joined.len(), 0);
  let Error::Delegate(e) = err.errors.into_iter().next().unwrap().1 else {
    panic!("Expected a MockDelegateError::CustomMergeCancelled");
  };
  assert_eq!(e, MockDelegateError::CustomMergeCancelled);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 1);
  assert_eq!(m2.alive_members().await, 1);

  // Check delegate invocation
  assert!(m1.delegate.as_ref().unwrap().is_invoked());
  assert!(m2.delegate.as_ref().unwrap().is_invoked());
}

pub async fn test_join_cancel_passive<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>();
  let m1 = Showbiz::with_delegate(MockDelegate::cancel_alive(c1.name.clone()), c1)
    .await
    .unwrap();

  let c2 = test_config::<R>().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::with_delegate(MockDelegate::cancel_alive(c2.name.clone()), c2)
    .await
    .unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 1);
  assert_eq!(m2.alive_members().await, 1);

  // Check delegate invocation
  assert_eq!(m1.delegate.as_ref().unwrap().count(), 2);
  assert_eq!(m2.delegate.as_ref().unwrap().count(), 2);
}

pub async fn test_join_shutdown<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let new_config = || -> Options<NetTransport<R>> {
    test_config()
      .with_probe_interval(Duration::from_millis(1))
      .with_probe_timeout(Duration::from_micros(100))
      .with_suspicion_max_timeout_mult(1)
  };

  let c1 = new_config();
  let m1 = Showbiz::new(c1).await.unwrap();

  // Create a second node
  let c2 = new_config().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 2);

  let start = Instant::now();
  let mut msg = String::new();
  while start.elapsed() < Duration::from_secs(20) {
    let n = m2.members().await.len();

    let (done, msg1) = (n == 1, format!("expected 1 node, got {n}"));
    if done {
      return;
    }
    msg = msg1;
    std::thread::sleep(Duration::from_secs(5));
  }
  panic!("timeout waiting for condition {}", msg);
}

async fn wait_for_condition<'a, Fut, F>(mut f: F)
where
  F: FnMut() -> Fut,
  Fut: Future<Output = (bool, String)> + 'a,
{
  let start = Instant::now();
  let mut msg = String::new();
  while start.elapsed() < Duration::from_secs(20) {
    let (done, msg1) = f().await;
    if done {
      return;
    }
    msg = msg1;
    std::thread::sleep(Duration::from_secs(5));
  }
  panic!("timeout waiting for condition {}", msg);
}

pub async fn test_join_dead_node<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>().with_tcp_timeout(Duration::from_millis(50));
  let m1 = Showbiz::new(c1).await.unwrap();

  // Create a second "node", which is just a TCP listener that
  // does not ever respond. This is to test our deadlines
  let addr2 = get_bind_addr();
  let _listener = std::net::TcpListener::bind(addr2).unwrap();
  // Ensure we don't hang forever
  let mut _delay = R::delay(Duration::from_millis(100), async {
    panic!("should have timed out by now");
  });

  let _err = m1
    .join_many([(addr2.into(), Name::from_static_unchecked("fake"))].into_iter())
    .await
    .unwrap_err();
  _delay.cancel().await;
}

#[allow(clippy::await_holding_lock)]
pub async fn test_join_ipv6<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  if !is_ipv6_loopback_available() {
    return;
  }

  // Since this binds to all interfaces we need to exclude other tests
  // from grabbing an interface.
  let _mu = BIND_NUM.lock().unwrap();

  let c1 = Options::<NetTransport<R>>::lan()
    .with_name(Name::from_static_unchecked("A"))
    .with_bind_addr("::1".parse().unwrap())
    .with_bind_port(Some(0));

  let m1 = Showbiz::new(c1).await.unwrap();

  // Create a second node
  let c2 = Options::<NetTransport<R>>::lan()
    .with_name(Name::from_static_unchecked("B"))
    .with_bind_addr("::1".parse().unwrap())
    .with_bind_port(Some(0));

  let m2 = Showbiz::new(c2).await.unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);

  // check the hosts
  assert_eq!(m1.members().await.len(), 2);
  assert_eq!(m2.members().await.len(), 2);
}

static INIT: std::sync::Once = std::sync::Once::new();
static mut IPV6_LOOPBACK_AVAILABLE: bool = false;

fn is_ipv6_loopback_available() -> bool {
  use pnet::datalink;
  use std::net::Ipv6Addr;
  INIT.call_once(|| {
    for iface in datalink::interfaces() {
      // Skip if not a loopback interface
      if !iface.is_loopback() {
        continue;
      }

      for ip_network in iface.ips {
        if let IpAddr::V6(ipv6) = ip_network.ip() {
          if ipv6 == Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1) {
            unsafe {
              IPV6_LOOPBACK_AVAILABLE = true;
            }
            return;
          }
        }
      }
    }
    unsafe {
      IPV6_LOOPBACK_AVAILABLE = false;
    }
    println!("IPv6 loopback address '::1' not found, disabling tests that require it");
  });

  unsafe { IPV6_LOOPBACK_AVAILABLE }
}

pub async fn test_leave<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let new_config = || test_config::<R>().with_gossip_interval(Duration::from_millis(1));
  let c1 = new_config();
  let m1 = Showbiz::new(c1).await.unwrap();

  let bind_port = m1.inner.opts.bind_port;

  // Create a second node
  let c2 = new_config().with_bind_port(bind_port);
  let m2 = Showbiz::new(c2).await.unwrap();

  join_and_test_member_ship(
    &m2,
    [(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter(),
    2,
  )
  .await;

  // Leave
  m1.leave(Duration::from_secs(1)).await.unwrap();

  // Wait for leave
  R::sleep(Duration::from_millis(10)).await;

  // m1 should think dead
  assert_eq!(m1.members().await.len(), 1);
  assert_eq!(m2.members().await.len(), 1);

  let nodes = m2.inner.nodes.read().await;
  let idx = nodes.node_map.get(&m1.inner.id).unwrap();
  assert_eq!(nodes.nodes[*idx].state.state, ServerState::Left);
}

#[allow(clippy::mutable_key_type)]
pub async fn test_delegate_meta<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>();
  let d1 = MockDelegate::new();
  d1.set_meta(Bytes::from_static(b"web"));
  let m1 = Showbiz::with_delegate(d1, c1).await.unwrap();

  let bind_port = m1.inner.opts.bind_port;

  // Create a second node
  let c2 = test_config::<R>().with_bind_port(bind_port);
  let d2 = MockDelegate::new();
  d2.set_meta(Bytes::from_static(b"lb"));

  let m2 = Showbiz::with_delegate(d2, c2).await.unwrap();

  m1.join_many([(m2.inner.opts.bind_addr.into(), m2.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();

  yield_now::<R>().await;

  // Check the roles of members of m1
  let m1m = m1.members().await;
  assert_eq!(m1m.len(), 2);
  let roles = m1m
    .iter()
    .map(|m| (m.id.clone(), m.meta.clone()))
    .collect::<HashMap<_, _>>();
  assert_eq!(roles.get(&m1.inner.id).unwrap().as_ref(), b"web");
  assert_eq!(roles.get(&m2.inner.id).unwrap().as_ref(), b"lb");

  // Check the roles of members of m2
  let m2m = m2.members().await;
  assert_eq!(m2m.len(), 2);
  let roles = m2m
    .iter()
    .map(|m| (m.id.clone(), m.meta.clone()))
    .collect::<HashMap<_, _>>();
  assert_eq!(roles.get(&m1.inner.id).unwrap().as_ref(), b"web");
  assert_eq!(roles.get(&m2.inner.id).unwrap().as_ref(), b"lb");
}

#[allow(clippy::mutable_key_type)]
pub async fn test_delegate_meta_update<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>();
  let d1 = MockDelegate::new();
  d1.set_meta(Bytes::from_static(b"web"));
  let m1 = Showbiz::with_delegate(d1, c1).await.unwrap();

  let bind_port = m1.inner.opts.bind_port;

  // Create a second node
  let c2 = test_config::<R>().with_bind_port(bind_port);
  let d2 = MockDelegate::new();
  d2.set_meta(Bytes::from_static(b"lb"));

  let m2 = Showbiz::with_delegate(d2, c2).await.unwrap();

  m1.join_many([(m2.inner.opts.bind_addr.into(), m2.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();

  yield_now::<R>().await;

  // Update the meta data roles
  m1.delegate
    .as_ref()
    .unwrap()
    .set_meta(Bytes::from_static(b"api"));
  m2.delegate
    .as_ref()
    .unwrap()
    .set_meta(Bytes::from_static(b"db"));

  m1.update_node(Duration::ZERO).await.unwrap();
  m2.update_node(Duration::ZERO).await.unwrap();

  yield_now::<R>().await;

  // Check the roles of members of m1
  let m1m = m1.members().await;
  assert_eq!(m1m.len(), 2);
  let roles = m1m
    .iter()
    .map(|m| (m.id.clone(), m.meta.clone()))
    .collect::<HashMap<_, _>>();
  assert_eq!(roles.get(&m1.inner.id).unwrap().as_ref(), b"api");
  assert_eq!(roles.get(&m2.inner.id).unwrap().as_ref(), b"db");

  // Check the roles of members of m2
  let m2m = m2.members().await;
  assert_eq!(m2m.len(), 2);
  let roles = m2m
    .iter()
    .map(|m| (m.id.clone(), m.meta.clone()))
    .collect::<HashMap<_, _>>();
  assert_eq!(roles.get(&m1.inner.id).unwrap().as_ref(), b"api");
  assert_eq!(roles.get(&m2.inner.id).unwrap().as_ref(), b"db");
}

pub async fn test_user_data<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let new_config = || {
    test_config::<R>()
      .with_gossip_interval(Duration::from_millis(100))
      .with_push_pull_interval(Duration::from_millis(100))
  };

  let d1 = MockDelegate::user_data();
  d1.set_state(Bytes::from_static(b"something"));
  let c1 = new_config();
  let m1 = Showbiz::with_delegate(d1, c1).await.unwrap();
  let bind_port = m1.inner.opts.bind_port;

  let bcasts = (0..=255)
    .map(|i| {
      let mut msg = Message::new();
      msg.put_u8(i);
      msg
    })
    .collect::<Vec<_>>();

  // Create a second node
  let d2 = MockDelegate::user_data();
  d2.set_state(Bytes::from_static(b"my state"));
  d2.set_broadcasts(bcasts.clone());
  let c2 = new_config().with_bind_port(bind_port);
  let m2 = Showbiz::with_delegate(d2, c2).await.unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();

  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 2);

  // wait for a little while
  {
    R::sleep(Duration::from_secs(5)).await;

    let msgs1 = m1.delegate.as_ref().unwrap().get_messages();

    // Ensure we got the messages. Ordering of messages is not guaranteed so just
    // check we got them both in either order.
    for i in 0..msgs1.len() {
      assert_eq!(msgs1[i].as_ref(), bcasts[i].underlying_bytes());
    }

    let rs1 = m1.delegate.as_ref().unwrap().get_remote_state();
    let rs2 = m2.delegate.as_ref().unwrap().get_remote_state();

    // Check the push/pull state
    assert_eq!(rs1.as_ref(), b"my state");
    assert_eq!(rs2.as_ref(), b"something");
  }
}

pub async fn test_ping_delegate<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let new_config = || test_config::<R>().with_probe_interval(Duration::from_millis(100));

  let c1 = new_config();
  let m1 = Showbiz::with_delegate(MockDelegate::ping(), c1)
    .await
    .unwrap();

  let bind_port = m1.inner.opts.bind_port;

  // Create a second node
  let c2 = new_config().with_bind_port(bind_port);
  let m2 = Showbiz::with_delegate(MockDelegate::ping(), c2)
    .await
    .unwrap();
  let mock = m2.delegate.as_ref().unwrap().clone();
  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);

  wait_until_size(m1.clone(), 2).await;
  wait_until_size(m2.clone(), 2).await;

  R::sleep(2 * m1.inner.opts.probe_interval).await;

  let local_node = m1.local_node().await;
  drop(m1);
  drop(m2);

  let (mother, mrtt, mpayload) = mock.get_contents().unwrap();
  assert_eq!(mother, local_node);
  assert!(mrtt > Duration::from_millis(0));
  assert_eq!(mpayload.as_ref(), b"whatever");
}

/// This test should follow the recommended upgrade guide:
/// https://www.consul.io/docs/agent/encryption.html#configuring-gossip-encryption-on-an-existing-cluster
///
/// We will use two nodes for this: m0 and m1
///
/// 0. Start with nodes without encryption.
/// 1. Set an encryption key and set `gossip_verify_incoming=false` and `gossip_verify_outgoing=false` to all nodes.
/// 2. Change `gossip_verify_outgoing=true` to all nodes.
/// 3. Change `gossip_verify_incoming=true` to all nodes.
pub async fn test_encrypted_gossip_transition<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pretty = RefCell::new(HashMap::new());
  let new_config = |short_name: Name, addr: Option<SocketAddr>| {
    let opts = match addr {
        Some(addr) => {
          Options::<NetTransport<R>>::lan()
            .with_bind_addr(addr.ip())
            .with_name(Name::from(addr.ip()))
        },
        None => {
          let addr = get_bind_addr();
          Options::<NetTransport<R>>::lan()
            .with_bind_addr(addr.ip())
            .with_name(Name::from(addr.ip()))
        },
    }
    // Set the gossip interval fast enough to get a reasonable test,
		// but slow enough to avoid "sendto: operation not permitted"
    .with_gossip_interval(Duration::from_millis(100))
    .with_bind_port(Some(0));

    pretty.borrow_mut().insert(opts.name.clone(), short_name);
    opts
  };

  let bind_port = RefCell::new(0);
  let create_ok = |opts: Options<NetTransport<R>>| async {
    let opts = if *bind_port.borrow() > 0 {
      opts.with_bind_port(Some(*bind_port.borrow()))
    } else {
      // try a range of port numbers until something sticks
      opts
    };

    let m = Showbiz::with_delegate(MockDelegate::new(), opts)
      .await
      .unwrap();
    if *bind_port.borrow() == 0 {
      *bind_port.borrow_mut() = m.inner.advertise.port();
    }
    m
  };

  let join_ok = |src: Showbiz<NetTransport<R>, MockDelegate>,
                 dst: Showbiz<NetTransport<R>, MockDelegate>,
                 num_nodes: usize| {
    let pretty = pretty.borrow();
    async move {
      let src_name = pretty.get(src.inner.id.name()).unwrap();
      let dst_name = pretty.get(dst.inner.id.name()).unwrap();
      tracing::info!(
        "node {}[{}] is joining node {}[{}]",
        src_name,
        src.inner.opts.name,
        dst_name,
        dst.inner.opts.name
      );

      let num = src
        .join_many([(dst.inner.advertise.into(), dst_name.clone())].into_iter())
        .await
        .unwrap()
        .len();
      assert_eq!(num, 1);

      wait_until_size(src.clone(), num_nodes).await;
      wait_until_size(dst.clone(), num_nodes).await;

      // Check the hosts
      assert_eq!(src.members().await.len(), num_nodes);
      assert_eq!(src.estimate_num_nodes() as usize, num_nodes);
      assert_eq!(dst.members().await.len(), num_nodes);
      assert_eq!(dst.estimate_num_nodes() as usize, num_nodes);
    }
  };

  let leave_ok = |src: Showbiz<NetTransport<R>, MockDelegate>, why: String| {
    let name = pretty.borrow().get(src.inner.id.name()).cloned().unwrap();
    async move {
      tracing::info!("node {}[{}] is leaving {}", name, src.inner.opts.name, why);
      src.leave(Duration::from_secs(1)).await.unwrap();
    }
  };

  let shutdown_ok = |src: Showbiz<NetTransport<R>, MockDelegate>, why: String| {
    let pretty = pretty.borrow();
    async move {
      let name = pretty.get(src.inner.id.name()).cloned().unwrap();
      tracing::info!(
        "node {}[{}] is shutting down {}",
        name,
        src.inner.opts.name,
        why
      );
      src.shutdown().await.unwrap();

      // Double check that it genuinely shutdown.
      wait_until_port_is_free(src).await;
    }
  };

  let leave_and_shutdown = |leaver: Showbiz<NetTransport<R>, MockDelegate>,
                            bystander: Showbiz<NetTransport<R>, MockDelegate>,
                            why: String| async {
    leave_ok(leaver.clone(), why.clone()).await;
    wait_until_size(bystander.clone(), 1).await;
    shutdown_ok(leaver, why).await;
    wait_until_size(bystander, 1).await;
  };

  // ==== STEP 0 ====

  // Create a first cluster of 2 nodes with no gossip encryption settings.
  let conf0 = new_config(Name::from_static_unchecked("m0"), None);
  let m0 = create_ok(conf0).await;

  let conf1 = new_config(Name::from_static_unchecked("m1"), None);
  let m1 = create_ok(conf1).await;

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
  let conf0 = new_config(Name::from_static_unchecked("m0"), None)
    .with_secret_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
    .with_gossip_verify_incoming(false)
    .with_gossip_verify_outgoing(false);
  let m0 = create_ok(conf0).await;

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
  let conf1 = new_config(Name::from_static_unchecked("m1"), None)
    .with_secret_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
    .with_gossip_verify_incoming(false)
    .with_gossip_verify_outgoing(false);
  let m1 = create_ok(conf1).await;

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
  let conf0 = new_config(Name::from_static_unchecked("m0"), None)
    .with_secret_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
    .with_gossip_verify_incoming(false);

  let m0 = create_ok(conf0).await;

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
  let conf1 = new_config(Name::from_static_unchecked("m1"), None)
    .with_secret_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")))
    .with_gossip_verify_incoming(false);
  let m1 = create_ok(conf1).await;

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
  let conf0 = new_config(Name::from_static_unchecked("m0"), None)
    .with_secret_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")));
  let m0 = create_ok(conf0).await;

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
  let conf1 = new_config(Name::from_static_unchecked("m1"), None)
    .with_secret_key(Some(SecretKey::Aes192(*b"Hi16ZXu2lNCRVwtr20khAg==")));
  let m1 = create_ok(conf1).await;

  // Join the first node. Both have encryption configured and fully in
  // enforcement.
  join_ok(m1, m0, 2).await;

  tracing::info!("==== STEP 3 complete: two node encrypted cluster locked down ====");
}

pub async fn test_send_to<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let new_config = || {
    test_config::<R>()
      .with_gossip_interval(Duration::from_millis(1))
      .with_push_pull_interval(Duration::from_millis(1))
  };

  let c1 = new_config();
  let m1 = Showbiz::with_delegate(MockDelegate::new(), c1)
    .await
    .unwrap();

  let bind_port = m1.inner.opts.bind_port;
  let c2 = new_config().with_bind_port(bind_port);
  let m2 = Showbiz::with_delegate(MockDelegate::new(), c2)
    .await
    .unwrap();

  let num = m2
    .join_many([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m2.members().await.len(), 2);

  // Try to do a direct send
  let m2_node = m2.inner.id.clone();

  let mut buf = BytesMut::new();
  buf.put_slice(b"ping");
  let msg = Message(buf);
  m1.send(&m2_node, msg).await.unwrap();

  let mut buf = BytesMut::new();
  buf.put_slice(b"pong");
  let msg = Message(buf);
  let m1_node = m1.inner.id.clone();
  m2.send(&m1_node, msg).await.unwrap();

  wait_for_condition(|| async {
    let msgs = m1.delegate.as_ref().unwrap().get_messages();
    (
      msgs.len() == 1,
      format!("expected 1 message, got {}", msgs.len()),
    )
  })
  .await;

  let msgs1 = m1.delegate.as_ref().unwrap().get_messages();
  assert_eq!(msgs1[0].as_ref(), b"pong");

  wait_for_condition(|| async {
    let msgs = m2.delegate.as_ref().unwrap().get_messages();
    (
      msgs.len() == 1,
      format!("expected 1 message, got {}", msgs.len()),
    )
  })
  .await;
  let msgs2 = m2.delegate.as_ref().unwrap().get_messages();
  assert_eq!(msgs2[0].as_ref(), b"ping");
}

fn reserve_port(ip: IpAddr, purpose: &str) -> u16 {
  use std::{
    io::ErrorKind,
    net::{TcpListener, UdpSocket},
  };

  for _ in 0..10 {
    let tcp_addr = SocketAddr::new(ip, 0);
    match TcpListener::bind(tcp_addr) {
      Ok(tcp_listener) => {
        let port = tcp_listener.local_addr().unwrap().port();

        let udp_addr = SocketAddr::new(ip, port);
        match UdpSocket::bind(udp_addr) {
          Ok(_) => {
            println!("Using dynamic bind port {} for {}", port, purpose);
            drop(tcp_listener); // Explicitly close the TCP listener
            return port;
          }
          Err(err) => {
            if err.kind() != ErrorKind::AddrInUse {
              panic!("Unexpected error: {:?}", err);
            }
          }
        }
      }
      Err(err) => {
        if err.kind() != ErrorKind::AddrInUse {
          panic!("Unexpected error: {:?}", err);
        }
      }
    }
  }

  panic!(
    "Could not find a free TCP+UDP port to listen on for {}",
    purpose
  );
}

pub async fn test_advertise_addr<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let bind_addr = get_bind_addr();
  let advertise_addr = get_bind_addr();

  let bind_port = reserve_port(bind_addr.ip(), "BIND");
  let advertise_port = reserve_port(advertise_addr.ip(), "ADVERTISE");

  let c = Options::<NetTransport<R>>::lan()
    .with_bind_addr(bind_addr.ip())
    .with_bind_port(Some(bind_port))
    .with_advertise_addr(Some(advertise_addr.ip()))
    .with_advertise_port(Some(advertise_port));

  let m = Showbiz::new(c).await.unwrap();

  yield_now::<R>().await;

  let members = m.members().await;
  assert_eq!(members.len(), 1);
  assert_eq!(members[0].id.addr().ip(), advertise_addr.ip());
  assert_eq!(members[0].id.addr().port(), advertise_port);
}

pub(crate) async fn retry<'a, R, F, Fut>(n: usize, w: Duration, mut f: F)
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
  F: FnMut() -> Fut + Clone,
  Fut: Future<Output = (bool, String)> + Send + Sync + 'a,
{
  for idx in 1..=n {
    let (failed, failed_msg) = f().await;
    if !failed {
      return;
    }
    if idx == n {
      panic!("failed after {} attempts: {}", n, failed_msg);
    }

    R::sleep(w).await;
  }
}

async fn wait_until_size<R>(m: Showbiz<NetTransport<R>, MockDelegate>, expected: usize)
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  retry::<R, _, _>(15, Duration::from_millis(500), || async {
    if m.alive_members().await != expected {
      return (
        true,
        format!(
          "expected {} nodes, got {}",
          expected,
          m.alive_members().await
        ),
      );
    }
    (false, "".to_string())
  })
  .await
}

fn is_port_free(addr: IpAddr, port: u16) -> std::io::Result<()> {
  let tcp_addr = SocketAddr::new(addr, port);
  let tcp_listener = std::net::TcpListener::bind(tcp_addr)?;
  drop(tcp_listener);

  let udp_addr = SocketAddr::new(addr, port);
  let udp_socket = std::net::UdpSocket::bind(udp_addr)?;
  drop(udp_socket);

  Ok(())
}

async fn wait_until_port_is_free<R>(member: Showbiz<NetTransport<R>, MockDelegate>)
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  // wait until we know for certain that m1 is dead dead
  let addr = member.inner.advertise;

  retry::<R, _, _>(15, Duration::from_millis(250), || async {
    if let Err(e) = is_port_free(addr.ip(), addr.port()) {
      return (true, format!("port is not yet free: {e}"));
    }
    (false, "".to_string())
  })
  .await;
}
