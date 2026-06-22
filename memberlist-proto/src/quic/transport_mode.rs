use derive_more::{Display, IsVariant};

/// Which wire the QUIC unreliable path (gossip + probes) rides. Chosen at
/// `QuicOptions` construction and the single source of truth for whether quinn's
/// datagram extension is enabled.
///
/// `Datagram` is the default: gossip and probe traffic ride QUIC datagrams
/// on the per-peer connection, sharing the connection's TLS 1.3 encryption
/// and peer attestation. `Udp` is the large-cluster opt-out: the same plain
/// UDP datagrams used by the TCP and TLS transports, bypassing the per-peer
/// connection entirely.
///
/// A `Udp`-mode endpoint does NOT enable or advertise the datagram extension
/// (the constructor forces `datagram_receive_buffer_size = None`), so a
/// cross-mode peer dialing it sees no datagram capability and falls back to
/// plain UDP. The mode is therefore both a pure-UDP opt-out and a mixed-mode
/// interop guarantee — a `Datagram`-mode peer never emits a datagram a
/// `Udp`-mode node would silently swallow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IsVariant, Display)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[display("{}", self.as_str())]
pub enum UnreliableTransport {
  /// QUIC datagrams over the per-peer connection — encrypted + attested. The
  /// constructor sizes quinn's datagram buffers so the extension is advertised.
  ///
  /// Keeping the per-peer pool warm is the caller's responsibility via the
  /// supplied `quinn_proto::TransportConfig`: set `keep_alive_interval` shorter
  /// than `max_idle_timeout` so an established connection survives between sparse
  /// gossip selections. Otherwise a peer not selected within one idle timeout is
  /// evicted, and the next datagram to it falls back to plain UDP until the
  /// connection is re-dialed and re-handshaked. The `TransportConfig` is
  /// caller-owned, so no keepalive value is forced here.
  #[default]
  Datagram,
  /// Plain unencrypted UDP datagrams (connectionless; the large-cluster opt-out).
  /// The constructor disables quinn's datagram extension for this endpoint, so
  /// it does not advertise datagram support and cross-mode peers use UDP.
  Udp,
}

impl UnreliableTransport {
  /// Returns a string representation of the transport mode.
  #[inline(always)]
  pub const fn as_str(&self) -> &str {
    match self {
      UnreliableTransport::Datagram => "datagram",
      UnreliableTransport::Udp => "udp",
    }
  }
}

/// Parse an [`UnreliableTransport`] from its lowercase name (`"datagram"` /
/// `"udp"`) — the inverse of [`UnreliableTransport::as_str`], usable as a config
/// value or CLI value parser independent of `clap`.
impl core::str::FromStr for UnreliableTransport {
  type Err = ParseUnreliableTransportError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(match s {
      "datagram" => Self::Datagram,
      "udp" => Self::Udp,
      _ => return Err(ParseUnreliableTransportError(())),
    })
  }
}

/// The error from [`UnreliableTransport::from_str`]: the input named no known
/// transport mode.
///
/// Opaque — the private unit field seals construction to this module, so the
/// error can gain detail later without a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unknown unreliable transport (expected \"datagram\" or \"udp\")")]
pub struct ParseUnreliableTransportError(());

/// Result of offering one unreliable datagram to the connection pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IsVariant, Display)]
#[display("{}", self.as_str())]
pub enum DatagramSendStatus {
  /// Accepted onto an established connection; it flows out via the normal
  /// `service_quinn` -> `poll_transmit` pump.
  Queued,
  /// No established connection yet (a dial was initiated), the peer does not
  /// support datagrams, or send failed. Best-effort: the caller falls back to
  /// plain UDP or drops. Never a membership signal.
  NotReady,
  /// The payload exceeds the connection's datagram `max_size`. The caller must
  /// fall back to UDP or split — never silently drop.
  TooLarge,
}

impl DatagramSendStatus {
  /// Returns a string representation of the outcome.
  #[inline(always)]
  pub const fn as_str(&self) -> &str {
    match self {
      DatagramSendStatus::Queued => "queued",
      DatagramSendStatus::NotReady => "not_ready",
      DatagramSendStatus::TooLarge => "too_large",
    }
  }
}

#[cfg(test)]
mod tests {
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
}
