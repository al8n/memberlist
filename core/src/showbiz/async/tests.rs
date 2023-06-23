use std::{
  net::{IpAddr, Ipv4Addr},
  sync::Mutex,
};

use agnostic::tokio::TokioRuntime;

use crate::{delegate::VoidDelegate, transport::net::NetTransport, SecretKey};

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
async fn yield_now() {
  TokioRuntime::sleep(Duration::from_millis(250)).await;
}

fn test_config_net(network: u8) -> Options<NetTransport<TokioRuntime>> {
  let bind_addr = SocketAddr::new(get_bind_addr_net(network), 0);
  Options::lan()
    .with_bind_addr(bind_addr)
    .with_name(bind_addr.to_string().try_into().unwrap())
    .with_require_node_names(true)
}

fn test_config() -> Options<NetTransport<TokioRuntime>> {
  test_config_net(0)
}

pub(crate) async fn get_showbiz<D: Delegate, F>(
  f: Option<F>,
) -> Result<
  Showbiz<D, NetTransport<TokioRuntime>, TokioRuntime>,
  Error<D, NetTransport<TokioRuntime>>,
>
where
  F: FnOnce(Options<NetTransport<TokioRuntime>>) -> Options<NetTransport<TokioRuntime>>,
{
  let c = if let Some(f) = f {
    f(test_config())
  } else {
    test_config()
  };

  Showbiz::new(c, TokioRuntime).await
}

#[tokio::test]
async fn test_create_secret_key() {
  let cases = vec![
    ("size-16", SecretKey::Aes128([0; 16])),
    ("size-24", SecretKey::Aes192([0; 24])),
    ("size-32", SecretKey::Aes256([0; 32])),
  ];

  for (name, key) in cases {
    let c = Options::<NetTransport<TokioRuntime>>::lan()
      .with_bind_addr(LOCALHOST)
      .with_secret_key(Some(key));

    let m = get_showbiz::<VoidDelegate, _>(Some(|_| c)).await.unwrap();
    if let Err(e) = m.bootstrap().await {
      panic!("name: {} key '{:?}' error: {:?}", name, key, e);
    }
    yield_now().await;
    assert!(m.shutdown().await.is_ok());
  }
}

#[tokio::test]
async fn test_create_secret_key_empty() {
  let c = Options::<NetTransport<TokioRuntime>>::lan().with_bind_addr(LOCALHOST);

  let m = get_showbiz::<VoidDelegate, _>(Some(|_| c)).await.unwrap();
  m.bootstrap().await.unwrap();
  yield_now().await;
  assert!(m.shutdown().await.is_ok());
}
