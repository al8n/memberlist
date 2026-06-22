use super::*;
use core::str::FromStr;

#[test]
fn unreliable_transport_as_str_and_from_str_round_trip() {
  for mode in [UnreliableTransport::Datagram, UnreliableTransport::Udp] {
    assert_eq!(UnreliableTransport::from_str(mode.as_str()).unwrap(), mode);
  }
  assert_eq!(UnreliableTransport::Datagram.as_str(), "datagram");
  assert_eq!(UnreliableTransport::Udp.as_str(), "udp");
  assert_eq!(
    UnreliableTransport::default(),
    UnreliableTransport::Datagram
  );
}

#[test]
fn unreliable_transport_display_matches_as_str() {
  assert_eq!(
    std::format!("{}", UnreliableTransport::Datagram),
    "datagram"
  );
  assert_eq!(std::format!("{}", UnreliableTransport::Udp), "udp");
}

#[test]
fn unreliable_transport_from_str_rejects_unknown() {
  let err = UnreliableTransport::from_str("tcp").unwrap_err();
  // The opaque parse error is the unit-payload type; equality holds across instances.
  assert_eq!(err, ParseUnreliableTransportError(()));
  assert!(UnreliableTransport::from_str("").is_err());
  assert!(UnreliableTransport::from_str("Datagram").is_err());
}

#[test]
fn unreliable_transport_is_variant_projections() {
  assert!(UnreliableTransport::Datagram.is_datagram());
  assert!(!UnreliableTransport::Datagram.is_udp());
  assert!(UnreliableTransport::Udp.is_udp());
  assert!(!UnreliableTransport::Udp.is_datagram());
}

#[cfg(feature = "serde")]
#[test]
fn unreliable_transport_serde_snake_case_round_trip() {
  for (mode, json) in [
    (UnreliableTransport::Datagram, "\"datagram\""),
    (UnreliableTransport::Udp, "\"udp\""),
  ] {
    assert_eq!(serde_json::to_string(&mode).unwrap(), json);
    let back: UnreliableTransport = serde_json::from_str(json).unwrap();
    assert_eq!(back, mode);
  }
  assert!(serde_json::from_str::<UnreliableTransport>("\"tcp\"").is_err());
}

#[test]
fn datagram_send_status_as_str_for_every_variant() {
  assert_eq!(DatagramSendStatus::Queued.as_str(), "queued");
  assert_eq!(DatagramSendStatus::NotReady.as_str(), "not_ready");
  assert_eq!(DatagramSendStatus::TooLarge.as_str(), "too_large");
}

#[test]
fn datagram_send_status_display_and_is_variant_projections() {
  assert_eq!(std::format!("{}", DatagramSendStatus::Queued), "queued");
  assert_eq!(
    std::format!("{}", DatagramSendStatus::NotReady),
    "not_ready"
  );
  assert_eq!(
    std::format!("{}", DatagramSendStatus::TooLarge),
    "too_large"
  );

  assert!(DatagramSendStatus::Queued.is_queued());
  assert!(DatagramSendStatus::NotReady.is_not_ready());
  assert!(DatagramSendStatus::TooLarge.is_too_large());
  assert!(!DatagramSendStatus::Queued.is_too_large());
}
