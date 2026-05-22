//! Tests for the generic per-exchange connection map
//! ([`crate::streams::conn::StreamConns`]) driving the plain-TCP record layer
//! ([`RawRecords`](records::RawRecords)).

#[cfg(test)]
mod tests {
  use crate::streams::conn::StreamConns;
  use smol_str::SmolStr;
  use std::{
    net::SocketAddr,
    time::{Duration, Instant},
  };

  use crate::{streams::bridge::StreamBridge, tcp::records::RawRecords};

  fn a_bridge() -> StreamBridge<SmolStr, SocketAddr, RawRecords> {
    let records = RawRecords::acceptor(Some(b"c".to_vec()), false);
    StreamBridge::new(records, Instant::now() + Duration::from_secs(10))
  }

  #[test]
  fn allocate_is_monotonic_and_distinct() {
    let mut c: StreamConns<SmolStr, SocketAddr, RawRecords> = StreamConns::new();
    let a = c.allocate();
    let b = c.allocate();
    assert_ne!(a, b);
    assert_eq!(b.get(), a.get() + 1);
  }

  #[test]
  fn insert_then_remove_clears_the_entry() {
    let mut c: StreamConns<SmolStr, SocketAddr, RawRecords> = StreamConns::new();
    let id = c.allocate();
    c.insert(id, a_bridge());
    assert_eq!(c.len(), 1);
    assert!(c.get_mut(id).is_some());
    assert!(c.remove(id).is_some());
    assert_eq!(c.len(), 0, "terminal teardown removes the bridge");
    assert!(c.get_mut(id).is_none());
  }
}
