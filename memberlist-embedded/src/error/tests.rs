use core::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::error::Error;

use super::{GossipMtuTooLarge, InitError};

#[test]
fn every_init_error_variant_displays_and_reports_its_source() {
  let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
  // (variant, whether `source()` should be `Some`). The transform-backed variants
  // exist only when a backend is compiled in, so they join the list under the same
  // cfgs that gate them on `InitError` itself.
  #[allow(unused_mut)]
  let mut cases: std::vec::Vec<(InitError, bool)> = std::vec![
    (InitError::NonRoutableAdvertiseAddr(addr), false),
    (InitError::AdvertisePortMismatch, false),
    (InitError::ZeroPort, false),
    (InitError::ZeroCloseTimeout, false),
    (InitError::ZeroGossipReadCap, false),
    (InitError::ZeroMaxPendingSeeds, false),
    (InitError::ZeroMaxPendingDials, false),
    (
      InitError::GossipMtuTooLarge(GossipMtuTooLarge {
        gossip_mtu: 70_000,
        ceiling: 65_000,
      }),
      false,
    ),
    (
      InitError::GossipRecvCapacityTooLarge(crate::GOSSIP_READ_CAP),
      false,
    ),
    (
      memberlist_proto::EndpointInitError::AwarenessMultiplierZero.into(),
      true,
    ),
  ];
  #[cfg(encryption)]
  cases.push((memberlist_proto::EncryptionError::AuthFailed.into(), true));
  #[cfg(checksum)]
  cases.push((memberlist_proto::ChecksumError::Mismatch.into(), true));
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
