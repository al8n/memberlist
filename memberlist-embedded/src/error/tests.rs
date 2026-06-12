use core::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::error::Error;

use super::{GossipMtuTooLarge, InitError};

#[test]
fn every_init_error_variant_displays_and_reports_its_source() {
  let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
  // (variant, whether `source()` should be `Some`).
  let cases: [(InitError, bool); 8] = [
    (InitError::NonRoutableAdvertiseAddr(addr), false),
    (InitError::AdvertisePortMismatch, false),
    (InitError::ZeroPort, false),
    (InitError::ZeroCloseTimeout, false),
    (
      InitError::GossipMtuTooLarge(GossipMtuTooLarge {
        gossip_mtu: 70_000,
        ceiling: 65_000,
      }),
      false,
    ),
    (memberlist_proto::EndpointInitError::Entropy.into(), true),
    (memberlist_proto::EncryptionError::AuthFailed.into(), true),
    (memberlist_proto::ChecksumError::Mismatch.into(), true),
  ];
  for (err, has_source) in cases {
    assert!(!err.to_string().is_empty(), "Display non-empty for {err:?}");
    assert!(!format!("{err:?}").is_empty(), "Debug non-empty");
    assert_eq!(
      err.source().is_some(),
      has_source,
      "source presence for {err:?}"
    );
  }
}

#[test]
fn gossip_mtu_too_large_display_carries_both_values() {
  let m = GossipMtuTooLarge {
    gossip_mtu: 70_000,
    ceiling: 65_000,
  };
  let shown = m.to_string();
  assert!(
    shown.contains("70000") && shown.contains("65000"),
    "got {shown}"
  );
  assert_eq!(
    m,
    GossipMtuTooLarge {
      gossip_mtu: 70_000,
      ceiling: 65_000
    }
  );
}
