// Copyright 2017-2020 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use quinn::{EndpointConfig, MtuDiscoveryConfig, VarInt};
use smol_str::SmolStr;
use std::{sync::Arc, time::Duration};

/// Options for the stream layer.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Clone)]
pub struct Options {
  /// The server name used for TLS.
  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(doc = "Gets the server name used for TLS.")
    ),
    setter(attrs(doc = "Sets the server name used for TLS."))
  )]
  server_name: SmolStr,

  /// In QUIC, if you're trying to connect to a unreachable address,
  /// you might not get an immediate error because of how QUIC and the underlying networking stack work.
  /// QUIC attempts to establish connections asynchronously, and depending on the configuration,
  /// it might retry or wait for a timeout before reporting a failure.
  /// To handle such scenarios more gracefully and detect errors like attempting
  /// to connect to a unreachable address more promptly, you need to set this config properly.
  ///
  /// Default value is 100ms.
  #[viewit(
    getter(const, attrs(doc = "Gets the timeout of the connect phase.")),
    setter(attrs(doc = "Sets the timeout for connecting to an address."))
  )]
  connect_timeout: Duration,

  /// Maximum duration of inactivity in ms to accept before timing out the connection.
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
  max_idle_timeout: u32,

  /// Period of inactivity before sending a keep-alive packet.
  /// Must be set lower than the idle_timeout of both
  /// peers to be effective.
  ///
  /// See [`quinn::TransportConfig::keep_alive_interval`] for more
  /// info.
  #[viewit(
    getter(
      const,
      attrs(doc = "Gets the period of inactivity before sending a keep-alive packet.")
    ),
    setter(attrs(doc = "Sets the period of inactivity before sending a keep-alive packet."))
  )]
  keep_alive_interval: Duration,

  /// Maximum number of incoming bidirectional streams that may be open
  /// concurrently by the remote peer.
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
  max_concurrent_stream_limit: u32,

  /// Max unacknowledged data in bytes that may be send on a single stream.
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
  max_stream_data: u32,

  /// Max unacknowledged data in bytes that may be send in total on all streams
  /// of a connection.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Gets the max unacknowledged data in bytes that may be send in total on all streams of a connection."
      )
    ),
    setter(attrs(
      doc = "Sets the max unacknowledged data in bytes that may be send in total on all streams of a connection."
    ))
  )]
  max_connection_data: u32,

  /// TLS client config for the inner [`quinn::ClientConfig`].
  #[viewit(
    getter(
      const,
      attrs(doc = "Gets the TLS client config for the inner [`quinn::ClientConfig`].")
    ),
    setter(attrs(doc = "Sets the TLS client config for the inner [`quinn::ClientConfig`]."))
  )]
  client_tls_config: Arc<rustls::ClientConfig>,
  /// TLS server config for the inner [`quinn::ServerConfig`].
  #[viewit(
    getter(
      const,
      attrs(doc = "Gets the TLS server config for the inner [`quinn::ServerConfig`].")
    ),
    setter(attrs(doc = "Sets the TLS server config for the inner [`quinn::ServerConfig`]."))
  )]
  server_tls_config: Arc<rustls::ServerConfig>,

  /// Quinn [`EndpointConfig`](quinn::EndpointConfig).
  #[viewit(
    getter(
      const,
      attrs(doc = "Gets the quinn's [`EndpointConfig`](quinn::EndpointConfig).")
    ),
    setter(attrs(doc = "Sets the quinn's [`EndpointConfig`](quinn::EndpointConfig)."))
  )]
  endpoint_config: EndpointConfig,

  /// Parameters governing MTU discovery. See [`MtuDiscoveryConfig`] for details.
  #[viewit(vis = "", getter(skip), setter(skip))]
  mtu_discovery_config: Option<MtuDiscoveryConfig>,
}

impl Options {
  /// Creates a new configuration object with default values.
  pub fn new(
    server_name: impl Into<SmolStr>,
    server_tls_config: rustls::ServerConfig,
    client_tls_config: rustls::ClientConfig,
    endpoint_config: EndpointConfig,
  ) -> Self {
    Self {
      server_name: server_name.into(),
      client_tls_config: Arc::new(client_tls_config),
      server_tls_config: Arc::new(server_tls_config),
      endpoint_config,
      max_idle_timeout: Duration::from_secs(10).as_millis() as u32,
      max_concurrent_stream_limit: 256,
      keep_alive_interval: Duration::from_secs(8),
      max_connection_data: 15_000_000,
      connect_timeout: Duration::from_secs(10),

      // Ensure that one stream is not consuming the whole connection.
      max_stream_data: 10_000_000,
      mtu_discovery_config: Some(Default::default()),
    }
  }
}

#[viewit::viewit]
/// Represents the inner configuration for [`quinn`].
#[derive(Debug, Clone)]
pub(super) struct QuinnOptions {
  server_name: SmolStr,
  client_config: quinn::ClientConfig,
  server_config: quinn::ServerConfig,
  endpoint_config: quinn::EndpointConfig,
  connect_timeout: Duration,
  max_stream_data: usize,
  max_connection_data: usize,
  max_open_streams: usize,
}

impl From<Options> for QuinnOptions {
  fn from(config: Options) -> QuinnOptions {
    let Options {
      server_name,
      client_tls_config,
      server_tls_config,
      max_idle_timeout,
      max_concurrent_stream_limit,
      keep_alive_interval,
      max_connection_data,
      max_stream_data,
      endpoint_config,
      mtu_discovery_config,
      connect_timeout,
    } = config;
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(max_concurrent_stream_limit.into());
    transport.max_concurrent_bidi_streams(max_concurrent_stream_limit.into());
    // Disable datagrams.
    transport.datagram_receive_buffer_size(None);
    transport.keep_alive_interval(Some(keep_alive_interval));
    transport.max_idle_timeout(Some(VarInt::from_u32(max_idle_timeout).into()));
    transport.allow_spin(false);
    transport.stream_receive_window(max_stream_data.into());
    transport.receive_window(max_connection_data.into());
    transport.mtu_discovery_config(mtu_discovery_config);
    let transport = Arc::new(transport);

    let mut server_config = quinn::ServerConfig::with_crypto(server_tls_config);
    server_config.transport = Arc::clone(&transport);
    // Disables connection migration.
    // Long-term this should be enabled, however we then need to handle address change
    // on connections in the `Connection`.
    server_config.migration(false);

    let mut client_config = quinn::ClientConfig::new(client_tls_config);
    client_config.transport_config(transport);

    QuinnOptions {
      server_name,
      client_config,
      server_config,
      endpoint_config,
      max_stream_data: max_stream_data as usize,
      max_connection_data: max_connection_data as usize,
      max_open_streams: max_concurrent_stream_limit as usize,
      connect_timeout,
    }
  }
}
