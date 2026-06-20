//! Tests for the generic per-exchange connection map
//! ([`crate::streams::conn::StreamConns`]) driving the TLS record layer
//! ([`TlsRecords`](records::TlsRecords)).

use crate::{streams::conn::StreamConns, tls::records::TlsRecords};
use core::net::SocketAddr;
use smol_str::SmolStr;

#[cfg(all(compression, encryption))]
use crate::{
  Instant,
  streams::{bridge::StreamBridge, test_support::TEST_RELIABLE_MAX},
  tls::options::tests::test_server,
};
#[cfg(all(compression, encryption))]
use core::time::Duration;
#[cfg(all(compression, encryption))]
use std::sync::Arc;

#[cfg(all(compression, encryption))]
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

#[cfg(all(compression, encryption))]
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
