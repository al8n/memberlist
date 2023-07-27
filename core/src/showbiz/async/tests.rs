use std::{
  net::{IpAddr, Ipv4Addr},
  sync::Mutex,
};

use agnostic::Delay;

use crate::{
  delegate::{MockDelegate, MockDelegateError, VoidDelegate},
  options::parse_cidrs,
  security::{EncryptionAlgo, SecretKey},
  transport::net::NetTransport,
  CompressionAlgo, Label,
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

    get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
      .await
      .unwrap();
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

  get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
    .await
    .unwrap();
  yield_now::<R>().await;
}

pub async fn test_create_keyring_only<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
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

pub async fn test_create_keyring_and_primary_key<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
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

pub async fn test_create<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::<NetTransport<R>>::lan().with_bind_addr(get_bind_addr().ip());
  let m = get_showbiz::<VoidDelegate, _, R>(Some(|_| c))
    .await
    .unwrap();
  yield_now::<R>().await;
  assert_eq!(m.members().await.len(), 1);
}

pub async fn test_resolve_addr() {

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

// #[test]
// fn test_resolve_addr() {
//   {
//     let runtime = tokio::runtime::Runtime::new().unwrap();
//     runtime.block_on(test_resolve_addr());
//   }
//   {
//     AsyncStdRuntime::block_on(test_resolve_addr());
//   }
//   {
//     SmolRuntime::block_on(test_resolve_addr());
//   }
// }

async fn test_resolve_addr_tcp_first() {
  todo!()
}

async fn test_members() {
  todo!()
}

pub async fn test_join<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
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
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  let c2 = test_config::<R>().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
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
  let m3 = Showbiz::<VoidDelegate, _>::new(c3).await.unwrap();
  let JoinError { joined, .. } = m3
    .join(std::iter::once((
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
  let m4 = Showbiz::<VoidDelegate, _>::new(c4).await.unwrap();

  let JoinError { joined, .. } = m4
    .join(std::iter::once((
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
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  // Create a second node
  let c2 = test_config_net::<R>(1)
    .with_allowed_cidrs(Some(cidrs))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
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
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  // Create a second node
  let c2 = test_config_net::<R>(1)
    .with_allowed_cidrs(Some(cidrs.clone()))
    .with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();
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
  let m3 = Showbiz::<VoidDelegate, _>::new(c3).await.unwrap();
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
  let m4 = Showbiz::<VoidDelegate, _>::new(c4).await.unwrap();
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
  this: &Showbiz<D, NetTransport<R>>,
  members_to_join: impl Iterator<Item = (Address, Name)>,
  expected_members: usize,
) where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let members_to_join_num = members_to_join.size_hint().0;
  let num = this.join(members_to_join).await.unwrap().len();
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
  let m1 = Showbiz::<MockDelegate, _>::with_delegate(MockDelegate::cancel_merge(), c1)
    .await
    .unwrap();

  let c2 = test_config::<R>().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<MockDelegate, _>::with_delegate(MockDelegate::cancel_merge(), c2)
    .await
    .unwrap();

  let err = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
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
  assert!(m1.inner.delegate.as_ref().unwrap().is_invoked());
  assert!(m2.inner.delegate.as_ref().unwrap().is_invoked());
}

pub async fn test_join_cancel_passive<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>();
  let m1 =
    Showbiz::<MockDelegate, _>::with_delegate(MockDelegate::cancel_alive(c1.name.clone()), c1)
      .await
      .unwrap();

  let c2 = test_config::<R>().with_bind_port(m1.inner.opts.bind_port);
  let m2 =
    Showbiz::<MockDelegate, _>::with_delegate(MockDelegate::cancel_alive(c2.name.clone()), c2)
      .await
      .unwrap();

  let num = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
    .await
    .unwrap()
    .len();
  assert_eq!(num, 1);

  // Check the hosts
  assert_eq!(m1.alive_members().await, 1);
  assert_eq!(m2.alive_members().await, 1);

  // Check delegate invocation
  assert_eq!(m1.inner.delegate.as_ref().unwrap().count(), 2);
  assert_eq!(m2.inner.delegate.as_ref().unwrap().count(), 2);
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
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  // Create a second node
  let c2 = new_config().with_bind_port(m1.inner.opts.bind_port);
  let m2 = Showbiz::<VoidDelegate, _>::new(c2).await.unwrap();

  let num = m2
    .join([(m1.inner.opts.bind_addr.into(), m1.inner.opts.name.clone())].into_iter())
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

// async fn wait_for_condition(f: Arc<impl Future<Output = (bool, String)>>) {
//   let start = Instant::now();
//   let mut msg = String::new();
//   while start.elapsed() < Duration::from_secs(20) {
//     let (done, msg1) = (*f.clone()).await;
//     if done {
//       return;
//     }
//     msg = msg1;
//     std::thread::sleep(Duration::from_secs(5));
//   }
//   panic!("timeout waiting for condition {}", msg);
// }

pub async fn test_join_dead_node<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c1 = test_config::<R>().with_tcp_timeout(Duration::from_millis(50));
  let m1 = Showbiz::<VoidDelegate, _>::new(c1).await.unwrap();

  // Create a second "node", which is just a TCP listener that
  // does not ever respond. This is to test our deadlines
  let addr2 = get_bind_addr();
  let _listener = std::net::TcpListener::bind(addr2).unwrap();
  // Ensure we don't hang forever
  let mut _delay = R::delay(Duration::from_millis(100), async {
    panic!("should have timed out by now");
  });

  let err = m1
    .join([(addr2.into(), Name::from_static_unchecked("fake"))].into_iter())
    .await
    .unwrap_err();
  eprintln!("{}", err);
  _delay.cancel().await;
}

pub async fn test_join_ipv6<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  todo!()
}

async fn test_leave() {
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
