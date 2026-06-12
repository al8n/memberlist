use super::*;
use std::error::Error as _;

#[test]
fn join_all_failed_accessors_and_display() {
  let payload = JoinAllFailed::new(5, 0);
  assert_eq!(payload.requested(), 5);
  assert_eq!(payload.contacted(), 0);
  let shown = format!("{payload}");
  assert!(
    shown.contains('5'),
    "Display mentions the requested count: {shown}"
  );
  assert!(!shown.is_empty());
  // Debug is derived and non-empty.
  assert!(!format!("{payload:?}").is_empty());
}

#[test]
fn invalid_gossip_mtu_accessors_and_display() {
  let payload = InvalidGossipMtu::new(70_000, 65_467);
  assert_eq!(payload.configured(), 70_000);
  assert_eq!(payload.ceiling(), 65_467);
  let shown = format!("{payload}");
  assert!(!shown.is_empty());
  assert!(shown.contains("70000"));
  assert!(!format!("{payload:?}").is_empty());
}

#[test]
fn gossip_mtu_too_small_accessors_and_display() {
  let payload = GossipMtuTooSmall::new(64, 512);
  assert_eq!(payload.configured(), 64);
  assert_eq!(payload.minimum(), 512);
  let shown = format!("{payload}");
  assert!(!shown.is_empty());
  assert!(shown.contains("512"));
  assert!(!format!("{payload:?}").is_empty());
}

#[test]
fn invalid_advertise_addr_accessors_and_display() {
  let addr: std::net::SocketAddr = "0.0.0.0:7946".parse().unwrap();
  let payload = InvalidAdvertiseAddr::new(addr, "wildcard bind".to_string());
  assert_eq!(payload.addr(), addr);
  assert_eq!(payload.reason(), "wildcard bind");
  let shown = format!("{payload}");
  assert!(!shown.is_empty());
  assert!(shown.contains("wildcard bind"));
  assert!(!format!("{payload:?}").is_empty());
}

#[test]
fn invalid_option_accessors_and_display() {
  let payload = InvalidOption::new("peek_budget", "must be nonzero".to_string());
  assert_eq!(payload.option(), "peek_budget");
  assert_eq!(payload.reason(), "must be nonzero");
  let shown = format!("{payload}");
  assert!(!shown.is_empty());
  assert!(shown.contains("peek_budget"));
  assert!(shown.contains("must be nonzero"));
  assert!(!format!("{payload:?}").is_empty());
}

// Every `MemberlistError` variant must render a non-empty Display and Debug.
#[test]
fn every_variant_displays_and_debugs() {
  let label_err = memberlist_proto::label::validate_label(&vec![b'x'; 254])
    .expect_err("a 254-byte label is over the 253-byte max");

  let variants = [
    MemberlistError::Io(io::Error::other("disk")),
    MemberlistError::Encryption(memberlist_proto::EncryptionError::AuthFailed),
    MemberlistError::Checksum(memberlist_proto::ChecksumError::Mismatch),
    MemberlistError::Frame(memberlist_proto::FrameError::Empty),
    MemberlistError::Resolve(io::Error::other("dns")),
    MemberlistError::JoinAllFailed(JoinAllFailed::new(3, 0)),
    MemberlistError::LeaveTimeout,
    MemberlistError::Shutdown,
    MemberlistError::NotRunning,
    MemberlistError::Proto(memberlist_proto::Error::AckPayloadExceedsMtu(
      memberlist_proto::SizeExceeded::new(1500, 1400),
    )),
    MemberlistError::InvalidGossipMtu(InvalidGossipMtu::new(70_000, 65_467)),
    MemberlistError::GossipMtuTooSmall(GossipMtuTooSmall::new(64, 512)),
    MemberlistError::InvalidAdvertiseAddr(InvalidAdvertiseAddr::new(
      "0.0.0.0:7946".parse().unwrap(),
      "wildcard".to_string(),
    )),
    MemberlistError::InvalidOption(InvalidOption::new("peek_budget", "nonzero".to_string())),
    MemberlistError::CommandSend,
    MemberlistError::ReplyClosed,
    MemberlistError::PingTimeout,
    MemberlistError::SendFailed,
    MemberlistError::InvalidLabel(label_err),
  ];

  for err in &variants {
    assert!(
      !format!("{err}").is_empty(),
      "Display non-empty for {err:?}"
    );
    assert!(!format!("{err:?}").is_empty(), "Debug non-empty");
  }
}

#[test]
fn transparent_variants_delegate_display() {
  // The variants that wrap a self-describing typed error are
  // `#[error(transparent)]`: their `Display` is exactly the inner error's,
  // with no added prefix. Callers reach the typed inner by matching the
  // variant; `source()` delegates to the inner (so a chain-walk shows the
  // message once, not duplicated).
  assert_eq!(
    MemberlistError::Io(io::Error::other("boom")).to_string(),
    io::Error::other("boom").to_string()
  );
  assert_eq!(
    MemberlistError::Encryption(memberlist_proto::EncryptionError::AuthFailed).to_string(),
    memberlist_proto::EncryptionError::AuthFailed.to_string()
  );
  assert_eq!(
    MemberlistError::Checksum(memberlist_proto::ChecksumError::Mismatch).to_string(),
    memberlist_proto::ChecksumError::Mismatch.to_string()
  );
  assert_eq!(
    MemberlistError::Frame(memberlist_proto::FrameError::Empty).to_string(),
    memberlist_proto::FrameError::Empty.to_string()
  );
}

// The `#[from]` derives let `?` lift the wire/io errors into `MemberlistError`.
#[test]
fn from_conversions_select_the_right_variant() {
  let from_io: MemberlistError = io::Error::other("boom").into();
  assert!(matches!(from_io, MemberlistError::Io(_)));

  let from_enc: MemberlistError = memberlist_proto::EncryptionError::AuthFailed.into();
  assert!(matches!(from_enc, MemberlistError::Encryption(_)));

  let from_checksum: MemberlistError = memberlist_proto::ChecksumError::Mismatch.into();
  assert!(matches!(from_checksum, MemberlistError::Checksum(_)));

  let from_frame: MemberlistError = memberlist_proto::FrameError::Empty.into();
  assert!(matches!(from_frame, MemberlistError::Frame(_)));
}
