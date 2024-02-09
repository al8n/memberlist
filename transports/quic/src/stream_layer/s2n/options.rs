use s2n_quic::provider::limits::Limits;
use s2n_quic_transport::connection::limits::ValidationError;
use std::{path::PathBuf, time::Duration};

/// Options for the S2n stream layer.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone)]
pub struct Options {
  /// Maximum amount of data that may be buffered for sending at any time.
  ///
  /// Default is `3_750_000`, tuned for 150Mbps throughput with a 100ms RTT.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Gets the max unacknowledged data in bytes that may be send on a single stream."
      )
    ),
    setter(attrs(
      doc = "Sets the max unacknowledged data in bytes that may be send on a single stream."
    ))
  )]
  data_window: u64,

  /// Maximum duration of inactivity to accept before timing out the connection.
  ///
  /// Defaults to `30` seconds.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Gets the maximum duration of inactivity in ms to accept before timing out the connection."
      )
    ),
    setter(attrs(
      doc = "Sets the maximum duration of inactivity in ms to accept before timing out the connection."
    ))
  )]
  max_idle_timeout: Duration,

  /// Maximum number of incoming bidirectional streams that may be open concurrently by the remote peer.
  ///
  /// Defaults to `100`.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Gets the maximum number of incoming bidirectional streams that may be open concurrently by the remote peer."
      )
    ),
    setter(attrs(
      doc = "Sets the maximum number of incoming bidirectional streams that may be open concurrently by the remote peer."
    ))
  )]
  max_open_remote_bidirectional_streams: u64,

  /// Maximum number of incoming unidirectional streams that may be open concurrently by the remote peer.
  ///
  /// Defaults to `100`.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Gets the maximum number of incoming unidirectional streams that may be open concurrently by the remote peer."
      )
    ),
    setter(attrs(
      doc = "Sets the maximum number of incoming unidirectional streams that may be open concurrently by the remote peer."
    ))
  )]
  max_open_remote_unidirectional_streams: u64,

  /// Maximum number of outgoing bidirectional streams that may be open concurrently by the local peer.
  ///
  /// Defaults to `100`.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Gets the maximum number of incoming bidirectional streams that may be open concurrently by the local."
      )
    ),
    setter(attrs(
      doc = "Sets the maximum number of incoming bidirectional streams that may be open concurrently by the local."
    ))
  )]
  max_open_local_bidirectional_streams: u64,

  /// Maximum number of outgoing unidirectional streams that may be open concurrently by the local peer.
  ///
  /// Defaults to `100`.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Gets the maximum number of incoming unidirectional streams that may be open concurrently by the local."
      )
    ),
    setter(attrs(
      doc = "Sets the maximum number of incoming unidirectional streams that may be open concurrently by the local."
    ))
  )]
  max_open_local_unidirectional_streams: u64,

  /// Period of inactivity before sending a keep-alive packet.
  ///
  /// Defaults to `30` seconds.
  #[viewit(
    getter(
      const,
      attrs(doc = "Gets the period of inactivity before sending a keep-alive packet.")
    ),
    setter(attrs(doc = "Sets the period of inactivity before sending a keep-alive packet."))
  )]
  keep_alive_interval: Duration,

  /// Cert path
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Gets the cert path.")),
    setter(attrs(doc = "Sets the cert path."))
  )]
  cert_path: PathBuf,

  /// Key path
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Gets the key path.")),
    setter(attrs(doc = "Sets the key path."))
  )]
  key_path: PathBuf,
}

impl Options {
  /// Creates a new set of options with default values.
  #[inline(always)]
  pub const fn new(cert: PathBuf, key: PathBuf) -> Self {
    Self {
      data_window: 3_750_000,
      max_idle_timeout: Duration::from_secs(30),
      max_open_remote_bidirectional_streams: 100,
      max_open_local_bidirectional_streams: 100,
      max_open_local_unidirectional_streams: 100,
      max_open_remote_unidirectional_streams: 100,
      keep_alive_interval: Duration::from_secs(30),
      cert_path: cert,
      key_path: key,
    }
  }
}


impl TryFrom<&Options> for Limits {
  type Error = ValidationError;

  #[inline(always)]
  fn try_from(options: &Options) -> Result<Self, Self::Error> {
    Limits::new()
      .with_data_window(options.data_window)?
      .with_max_idle_timeout(options.max_idle_timeout)?
      .with_max_open_remote_bidirectional_streams(options.max_open_remote_bidirectional_streams)?
      .with_max_open_remote_unidirectional_streams(options.max_open_remote_unidirectional_streams)?
      .with_max_open_local_bidirectional_streams(options.max_open_local_bidirectional_streams)?
      .with_max_open_local_unidirectional_streams(options.max_open_local_unidirectional_streams)?
      .with_max_keep_alive_period(options.keep_alive_interval)
  }
}
