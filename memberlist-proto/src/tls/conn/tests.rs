use crate::{Instant, streams::conn::StreamConns, tls::options::tests::test_server};
use core::{net::SocketAddr, time::Duration};
use smol_str::SmolStr;
use std::sync::Arc;

use crate::{
  streams::{bridge::StreamBridge, test_support::TEST_RELIABLE_MAX},
  tls::records::TlsRecords,
};

fn a_bridge() -> StreamBridge<SmolStr, SocketAddr, TlsRecords> {
  let records = TlsRecords::server(Arc::new(test_server())).unwrap();
  StreamBridge::new(
    records,
    Instant::now() + Duration::from_secs(10),
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    TEST_RELIABLE_MAX,
  )
}

#[test]
fn allocate_is_monotonic_and_distinct() {
  let mut c: StreamConns<SmolStr, SocketAddr, TlsRecords> = StreamConns::new();
  let a = c.allocate();
  let b = c.allocate();
  assert_ne!(a, b);
  assert_eq!(b.get(), a.get() + 1);
}

#[test]
fn insert_then_remove_clears_the_entry() {
  let mut c: StreamConns<SmolStr, SocketAddr, TlsRecords> = StreamConns::new();
  let id = c.allocate();
  c.insert(id, a_bridge());
  assert_eq!(c.len(), 1);
  assert!(c.get_mut(id).is_some());
  assert!(c.remove(id).is_some());
  assert_eq!(c.len(), 0, "terminal teardown removes the bridge");
  assert!(c.get_mut(id).is_none());
}
