use super::*;
use crate::interface::{
  EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address, Medium, Route,
};
use core::net::{Ipv4Addr, SocketAddr};

fn sample_cidr() -> IpCidr {
  IpCidr::new(IpAddress::v4(224, 0, 0, 1), 24)
}

fn sample_route() -> Route {
  Route::new_ipv4_gateway(Ipv4Address::new(10, 0, 0, 1))
}

fn sample_socket_addr() -> SocketAddr {
  SocketAddr::new(Ipv4Addr::new(10, 0, 0, 1).into(), 7946)
}

// A non-unicast (multicast) MAC: the low bit of the first octet set.
fn multicast_mac() -> HardwareAddress {
  HardwareAddress::Ethernet(EthernetAddress([0x01, 0, 0, 0, 0, 1]))
}

// Build one representative value of every `InitError` variant so the Display
// and Debug arms are all exercised.
fn all_variants() -> Vec<InitError> {
  vec![
    InitError::MediumMismatch(MediumMismatch {
      expected: Medium::Ethernet,
      actual: Medium::Ip,
    }),
    InitError::UnsupportedMedium,
    InitError::NonUnicastHardwareAddress(multicast_mac()),
    InitError::NonUnicastIpAddress(sample_cidr()),
    InitError::NonRoutableAdvertiseAddr(sample_socket_addr()),
    InitError::MissingIpAddress,
    InitError::TooManyIpAddresses,
    InitError::TooManyRoutes,
    InitError::ZeroPort,
    InitError::AdvertisePortMismatch,
    InitError::AdvertiseAddrNotLocal(sample_socket_addr()),
    InitError::NonUnicastRouteGateway(sample_route()),
    InitError::RouteFamilyMismatch(sample_route()),
    InitError::Entropy,
    InitError::Endpoint(EndpointInitError::AwarenessMultiplierZero),
    InitError::Encryption(memberlist_proto::EncryptionError::AuthFailed),
    InitError::Checksum(memberlist_proto::ChecksumError::Mismatch),
    InitError::GossipMtuTooLarge(GossipMtuTooLarge {
      gossip_mtu: 70_000,
      ceiling: 65_467,
    }),
    InitError::UdpArenaTooLarge,
    InitError::TcpPoolTooSmall,
    InitError::ZeroTcpSocketBuffer,
    InitError::TcpRxBufferTooLarge,
    InitError::ZeroUdpPackets,
    InitError::ZeroCloseTimeout,
  ]
}

#[test]
fn every_variant_displays_and_debugs_non_empty() {
  for err in all_variants() {
    assert!(
      !format!("{err}").is_empty(),
      "Display non-empty for {err:?}"
    );
    assert!(!format!("{err:?}").is_empty(), "Debug non-empty");
  }
}

#[test]
fn gossip_mtu_too_large_payload_display() {
  let payload = GossipMtuTooLarge {
    gossip_mtu: 70_000,
    ceiling: 65_467,
  };
  let shown = format!("{payload}");
  assert!(shown.contains("70000"), "{shown}");
  assert!(shown.contains("65467"), "{shown}");
  // Copy + PartialEq are derived.
  assert_eq!(payload, payload);
  assert!(!format!("{payload:?}").is_empty());
}

#[test]
fn medium_mismatch_payload_display() {
  let payload = MediumMismatch {
    expected: Medium::Ethernet,
    actual: Medium::Ip,
  };
  assert!(!format!("{payload}").is_empty());
  assert_eq!(payload, payload);
  assert!(!format!("{payload:?}").is_empty());
}

#[test]
fn from_endpoint_init_error() {
  let err: InitError = EndpointInitError::AwarenessMultiplierZero.into();
  assert!(matches!(
    err,
    InitError::Endpoint(EndpointInitError::AwarenessMultiplierZero)
  ));
}

#[test]
fn from_encryption_error() {
  let err: InitError = memberlist_proto::EncryptionError::NoMatchingKey.into();
  assert!(matches!(err, InitError::Encryption(_)));
}

#[test]
fn from_checksum_error() {
  let err: InitError = memberlist_proto::ChecksumError::Mismatch.into();
  assert!(matches!(err, InitError::Checksum(_)));
}

// `from_embedded` maps each embedded failure mode to its driver equivalent,
// and routes any future (non_exhaustive) variant to a generic endpoint error.
#[test]
fn from_embedded_maps_each_mode() {
  use memberlist_embedded::InitError as E;

  assert!(matches!(
    InitError::from_embedded(E::ZeroPort),
    InitError::ZeroPort
  ));
  assert!(matches!(
    InitError::from_embedded(E::AdvertisePortMismatch),
    InitError::AdvertisePortMismatch
  ));
  assert!(matches!(
    InitError::from_embedded(E::ZeroCloseTimeout),
    InitError::ZeroCloseTimeout
  ));
  assert!(matches!(
    InitError::from_embedded(E::NonRoutableAdvertiseAddr(sample_socket_addr())),
    InitError::NonRoutableAdvertiseAddr(_)
  ));
  assert!(matches!(
    InitError::from_embedded(E::Endpoint(EndpointInitError::AwarenessMultiplierZero)),
    InitError::Endpoint(_)
  ));
  assert!(matches!(
    InitError::from_embedded(E::Encryption(memberlist_proto::EncryptionError::AuthFailed)),
    InitError::Encryption(_)
  ));
  assert!(matches!(
    InitError::from_embedded(E::Checksum(memberlist_proto::ChecksumError::Mismatch)),
    InitError::Checksum(_)
  ));

  // The carried ceiling/value survive the GossipMtuTooLarge remap.
  let mapped = InitError::from_embedded(E::GossipMtuTooLarge(
    memberlist_embedded::GossipMtuTooLarge {
      gossip_mtu: 99_999,
      ceiling: 65_467,
    },
  ));
  match mapped {
    InitError::GossipMtuTooLarge(g) => {
      assert_eq!(g.gossip_mtu, 99_999);
      assert_eq!(g.ceiling, 65_467);
    }
    other => panic!("expected GossipMtuTooLarge, got {other:?}"),
  }
}

// Under `std` the `Error::source` chains only for the wrapping variants.
#[cfg(feature = "std")]
#[test]
fn source_chains_only_for_wrapping_variants() {
  use std::error::Error as _;

  assert!(
    InitError::Endpoint(EndpointInitError::AwarenessMultiplierZero)
      .source()
      .is_some()
  );
  assert!(
    InitError::Encryption(memberlist_proto::EncryptionError::AuthFailed)
      .source()
      .is_some()
  );
  assert!(
    InitError::Checksum(memberlist_proto::ChecksumError::Mismatch)
      .source()
      .is_some()
  );
  // A leaf variant carries no source.
  assert!(InitError::ZeroPort.source().is_none());
  assert!(InitError::Entropy.source().is_none());
  assert!(
    InitError::GossipMtuTooLarge(GossipMtuTooLarge {
      gossip_mtu: 70_000,
      ceiling: 65_467,
    })
    .source()
    .is_none()
  );
}
