//! Tests for the generic per-exchange connection map
//! ([`crate::streams::conn::StreamConns`]) driving the plain-TCP record layer
//! ([`RawRecords`](records::RawRecords)).

#[cfg(test)]
mod tests {
  use crate::{Instant, streams::conn::StreamConns};
  use core::{net::SocketAddr, time::Duration};
  use smol_str::SmolStr;

  use crate::{
    config::EndpointOptions,
    endpoint::Endpoint,
    event::PushPullKind,
    streams::{
      LabelOptions, StreamAction, StreamEndpoint,
      bridge::StreamBridge,
      test_support::{addr, test_peer_to_socket, test_sni_provider},
    },
    tcp::records::RawRecords,
  };

  /// A reliable-unit ceiling distinct from every other constant in play so a
  /// test asserting a bridge carries it cannot pass by coincidence.
  const TEST_RELIABLE_MAX: usize = 4096;

  fn a_bridge() -> StreamBridge<SmolStr, SocketAddr, RawRecords> {
    let records = RawRecords::acceptor(Some(b"c".to_vec()), false);
    StreamBridge::new(
      records,
      Instant::now() + Duration::from_secs(10),
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      TEST_RELIABLE_MAX,
    )
  }

  /// A bridge built by the coordinator must carry the configured
  /// `max_stream_frame_size` as its reliable-unit ceiling — the ceiling tracks
  /// the user-tunable config value, not a hard-coded constant. A custom 17 MiB
  /// limit (above the old 16 MiB constant, below the 64 MiB default) pins the
  /// wiring: the bridge's ceiling must be exactly the configured value.
  #[test]
  fn bridge_reliable_max_tracks_configured_max_stream_frame_size() {
    let now = Instant::now();
    let custom_max = 17 * 1024 * 1024;
    let cfg =
      EndpointOptions::new(SmolStr::new("n"), addr(7300)).with_max_stream_frame_size(custom_max);
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
      ep,
      LabelOptions::new_in(Some(b"cluster-x".to_vec()), ()),
      test_sni_provider(),
      test_peer_to_socket(),
    );

    // Drive an outbound dial so the coordinator builds a bridge.
    coord.start_push_pull(addr(7301), PushPullKind::Refresh, now);
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {other:?}"),
    };

    assert_eq!(
      coord.bridge_reliable_max(exchange),
      Some(custom_max),
      "the bridge's reliable-unit ceiling must be the configured \
       max_stream_frame_size, not a hard-coded constant",
    );
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
