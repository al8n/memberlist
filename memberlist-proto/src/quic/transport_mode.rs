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
