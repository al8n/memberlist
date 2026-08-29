//! Build a [`QuicOptions`] from cert / key / CA files on disk.
//!
//! [`QuicOptions::new`](super::QuicOptions::new) accepts fully caller-built
//! `quinn_proto::EndpointConfig` / `ServerConfig` / `ClientConfig` and makes no
//! security policy choice. This module is the convenience layer a CLI / daemon
//! uses to construct those from PEM files plus a single
//! [`ClientAuthMode`](crate::tls::ClientAuthMode) choice and a few transport
//! knobs.
//!
//! # Why this needs the `tls` feature
//!
//! QUIC's reliable path is secured by its embedded TLS 1.3 handshake, so the
//! from-files builder reuses the exact rustls `ServerConfig` / `ClientConfig`
//! assembly from [`crate::tls::TlsConfigOptions`] (same cert/key/CA loading,
//! same [`ClientAuthMode`](crate::tls::ClientAuthMode) auth-mode logic) and
//! then wraps the result into quinn's QUIC crypto configs. It is therefore
//! gated on **both** `quic` and `tls`; enabling only `quic` still lets a caller
//! build a [`QuicOptions`] by hand via [`QuicOptions::new`](super::QuicOptions::new).
//!
//! # Crypto provider and reset key
//!
//! Like the TLS builder, this reuses the process default `CryptoProvider`
//! (install one first — see [`crate::tls::TlsConfigOptions`]). The endpoint's
//! stateless-reset HMAC key comes from `quinn_proto::EndpointConfig::default()`,
//! which seeds a random key from the same crypto backend the `quic-rustls-*` /
//! `quic-*-ring` feature pulls into the graph. `QuicOptions::new` then forces
//! `grease_quic_bit = false` and the demux invariants on top.
//!
//! # Authentication models
//!
//! The [`ClientAuthMode`](crate::tls::ClientAuthMode) knob selects mutual TLS
//! (`ClusterCa`, the default) or server-authentication only (`TrustedNetwork`),
//! exactly as documented on [`crate::tls::TlsConfigOptions`] — QUIC
//! authenticates the peer during its per-connection TLS 1.3 handshake.

use core::time::Duration;
use std::{path::PathBuf, sync::Arc};

use quinn_proto::{
  ClientConfig as QuinnClientConfig, EndpointConfig, IdleTimeout,
  ServerConfig as QuinnServerConfig, TransportConfig,
  crypto::rustls::{QuicClientConfig, QuicServerConfig},
};

use crate::tls::{
  ClientAuthMode, TlsConfigError,
  config::{build_client_config, build_server_config, load_certs, load_private_key, load_roots},
};

use super::{
  DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE, DEFAULT_MAX_PENDING_INBOUND_TOTAL,
  DEFAULT_PENDING_SOURCE_PREFIX_V4, DEFAULT_PENDING_SOURCE_PREFIX_V6, QuicOptions,
  UnreliableTransport,
};

/// The cluster-uniform server name installed on [`QuicOptions::new`] when none
/// is configured. Every outbound dial presents this identity to the operator's
/// `ServerCertVerifier`.
const DEFAULT_SERVER_NAME: &str = "localhost";

fn default_server_name() -> String {
  DEFAULT_SERVER_NAME.to_string()
}

// Named serde defaults for the source-normalization knobs. A bare
// `#[serde(default)]` would deserialize an omitted field to the type default
// (`0` for the prefixes, `None` for the budget) — and `0` is a legitimate
// whole-family bucket that `QuicOptions::validate` ACCEPTS, so a config file
// upgraded across this change but not updated would silently collapse every
// IPv4 source into ONE 16-slot bucket = a trivial total-inbound DoS. These
// functions pin the omitted-field defaults to the same values `QuicOptions::new`
// installs.
fn default_pending_source_prefix_v4() -> u8 {
  DEFAULT_PENDING_SOURCE_PREFIX_V4
}

fn default_pending_source_prefix_v6() -> u8 {
  DEFAULT_PENDING_SOURCE_PREFIX_V6
}

fn default_max_pending_inbound_total() -> Option<usize> {
  Some(DEFAULT_MAX_PENDING_INBOUND_TOTAL)
}

fn default_max_pending_connections_per_source() -> Option<usize> {
  Some(DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE)
}

/// Force QUIC 0-RTT (early data) OFF on the managed path's rustls pair. The
/// coordinator's effect-finality boundary defers every application effect to a
/// connection's establishment, so early data buys nothing here, and the shared
/// TLS assembly must never re-enable it. rustls already defaults these off
/// (client `enable_early_data = false`, server `max_early_data_size = 0`), but
/// forcing them keeps that guarantee independent of any future default or
/// shared-builder change — the config-path analogue of the `IdleTimeoutZero`
/// guard in [`QuicConfigOptions::build`].
fn force_early_data_off(client: &mut rustls::ClientConfig, server: &mut rustls::ServerConfig) {
  client.enable_early_data = false;
  server.max_early_data_size = 0;
}

/// Construct a [`QuicOptions`] from cert / key / CA files on disk plus a single
/// [`ClientAuthMode`](crate::tls::ClientAuthMode) choice and a few transport
/// tunables.
///
/// `cert_file`, `key_file`, and `ca_file` are required PEM paths (no default);
/// `client_auth` defaults to mutual TLS. `max_idle_timeout` and
/// `keep_alive_interval` are optional `humantime`-formatted durations applied
/// to the QUIC `TransportConfig`; `unreliable_transport` selects the gossip
/// wire (QUIC datagrams vs plain UDP). Call [`Self::build`] to assemble.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct QuicConfigOptions {
  /// Path to the PEM file holding this node's certificate chain.
  cert_file: PathBuf,
  /// Path to the PEM file holding this node's private key.
  key_file: PathBuf,
  /// Path to the PEM file holding the cluster CA certificate(s).
  ca_file: PathBuf,
  /// Client-authentication policy. Defaults to
  /// [`ClientAuthMode::ClusterCa`](crate::tls::ClientAuthMode::ClusterCa)
  /// (mutual TLS over the cluster CA).
  #[cfg_attr(feature = "serde", serde(default))]
  client_auth: ClientAuthMode,
  /// The cluster-uniform server name (SNI) presented on every outbound dial and
  /// matched against the peer's certificate at verification. Must match the
  /// peers' certificate SAN; defaults to `"localhost"` (suitable only for tests).
  #[cfg_attr(feature = "serde", serde(default = "default_server_name"))]
  server_name: String,
  /// Optional QUIC idle timeout — an established connection with no traffic for
  /// this long is closed. `None` (the default) leaves quinn's default.
  #[cfg_attr(feature = "serde", serde(default, with = "humantime_serde::option"))]
  max_idle_timeout: Option<Duration>,
  /// Optional QUIC keep-alive interval — set shorter than `max_idle_timeout` to
  /// keep an established connection warm between sparse gossip selections.
  /// `None` (the default) sends no keep-alives.
  #[cfg_attr(feature = "serde", serde(default, with = "humantime_serde::option"))]
  keep_alive_interval: Option<Duration>,
  /// Which wire the unreliable (gossip + probe) path rides. Defaults to
  /// [`UnreliableTransport::Datagram`].
  #[cfg_attr(feature = "serde", serde(default))]
  unreliable_transport: UnreliableTransport,
  /// Per-normalized-source ceiling on concurrent pending (half-open) inbound
  /// handshakes, or `None` for no bound. The NAT / CGNAT tuning knob: honest
  /// peers sharing one public IP (or one v6 subnet) share this allowance.
  /// Defaults to [`Some`]`(`[`DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE`](super::DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE)`)`.
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_max_pending_connections_per_source")
  )]
  max_pending_connections_per_source: Option<usize>,
  /// IPv4 prefix length used to normalize an inbound source into its admission
  /// bucket. `/32` keys each host by its full address so UDP source-port rotation
  /// cannot bypass the per-source cap; a shorter prefix collapses a subnet into
  /// one bucket. Must be `<= 32`. Defaults to [`DEFAULT_PENDING_SOURCE_PREFIX_V4`](super::DEFAULT_PENDING_SOURCE_PREFIX_V4) (`/32`).
  #[cfg_attr(feature = "serde", serde(default = "default_pending_source_prefix_v4"))]
  pending_source_prefix_v4: u8,
  /// IPv6 prefix length used to normalize an inbound source into its admission
  /// bucket. `/64` (the SLAAC subnet a host owns) is the default — `/128` would
  /// reopen the bypass as IPv6 source-address rotation. Must be `<= 128`.
  /// Defaults to [`DEFAULT_PENDING_SOURCE_PREFIX_V6`](super::DEFAULT_PENDING_SOURCE_PREFIX_V6) (`/64`).
  #[cfg_attr(feature = "serde", serde(default = "default_pending_source_prefix_v6"))]
  pending_source_prefix_v6: u8,
  /// Coordinator-wide ceiling on the aggregate half-open inbound population, or
  /// `None` for no bound. Stops a subnet flood spread thin across many source
  /// keys from pinning the whole global connection budget in half-open
  /// handshakes. Defaults to [`Some`]`(`[`DEFAULT_MAX_PENDING_INBOUND_TOTAL`](super::DEFAULT_MAX_PENDING_INBOUND_TOTAL)`)`.
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_max_pending_inbound_total")
  )]
  max_pending_inbound_total: Option<usize>,
}

impl QuicConfigOptions {
  /// Construct from the three required file paths with the default auth mode
  /// (mutual TLS), no timeout tuning, and the default unreliable transport.
  pub fn new(cert_file: PathBuf, key_file: PathBuf, ca_file: PathBuf) -> Self {
    Self {
      cert_file,
      key_file,
      ca_file,
      client_auth: ClientAuthMode::ClusterCa,
      server_name: default_server_name(),
      max_idle_timeout: None,
      keep_alive_interval: None,
      unreliable_transport: UnreliableTransport::Datagram,
      max_pending_connections_per_source: default_max_pending_connections_per_source(),
      pending_source_prefix_v4: default_pending_source_prefix_v4(),
      pending_source_prefix_v6: default_pending_source_prefix_v6(),
      max_pending_inbound_total: default_max_pending_inbound_total(),
    }
  }

  /// Builder: set the client-authentication mode.
  #[must_use]
  #[inline(always)]
  pub const fn with_client_auth(mut self, mode: ClientAuthMode) -> Self {
    self.client_auth = mode;
    self
  }

  /// Builder: set the cluster-uniform server name (SNI). Must match the peers'
  /// certificate SAN.
  #[must_use]
  #[inline(always)]
  pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
    self.server_name = name.into();
    self
  }

  /// Builder: set the QUIC idle timeout.
  #[must_use]
  #[inline(always)]
  pub const fn with_max_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
    self.max_idle_timeout = timeout;
    self
  }

  /// Builder: set the QUIC keep-alive interval.
  #[must_use]
  #[inline(always)]
  pub const fn with_keep_alive_interval(mut self, interval: Option<Duration>) -> Self {
    self.keep_alive_interval = interval;
    self
  }

  /// Builder: set the unreliable transport mode.
  #[must_use]
  #[inline(always)]
  pub const fn with_unreliable_transport(mut self, mode: UnreliableTransport) -> Self {
    self.unreliable_transport = mode;
    self
  }

  /// Builder: set the per-normalized-source pending-handshake ceiling (`None`
  /// removes the bound). The NAT tuning knob.
  #[must_use]
  #[inline(always)]
  pub const fn with_max_pending_connections_per_source(mut self, max: Option<usize>) -> Self {
    self.max_pending_connections_per_source = max;
    self
  }

  /// Builder: set the IPv4 source-normalization prefix (`<= 32`).
  #[must_use]
  #[inline(always)]
  pub const fn with_pending_source_prefix_v4(mut self, prefix: u8) -> Self {
    self.pending_source_prefix_v4 = prefix;
    self
  }

  /// Builder: set the IPv6 source-normalization prefix (`<= 128`).
  #[must_use]
  #[inline(always)]
  pub const fn with_pending_source_prefix_v6(mut self, prefix: u8) -> Self {
    self.pending_source_prefix_v6 = prefix;
    self
  }

  /// Builder: set the coordinator-wide half-open inbound budget (`None` removes
  /// the bound).
  #[must_use]
  #[inline(always)]
  pub const fn with_max_pending_inbound_total(mut self, max: Option<usize>) -> Self {
    self.max_pending_inbound_total = max;
    self
  }

  /// The configured certificate-chain file path.
  #[inline(always)]
  pub fn cert_file(&self) -> &PathBuf {
    &self.cert_file
  }

  /// The configured private-key file path.
  #[inline(always)]
  pub fn key_file(&self) -> &PathBuf {
    &self.key_file
  }

  /// The configured CA-certificate file path.
  #[inline(always)]
  pub fn ca_file(&self) -> &PathBuf {
    &self.ca_file
  }

  /// The configured client-authentication mode.
  #[inline(always)]
  pub const fn client_auth(&self) -> ClientAuthMode {
    self.client_auth
  }

  /// The configured QUIC idle timeout, if any.
  #[inline(always)]
  pub const fn max_idle_timeout(&self) -> Option<Duration> {
    self.max_idle_timeout
  }

  /// The configured QUIC keep-alive interval, if any.
  #[inline(always)]
  pub const fn keep_alive_interval(&self) -> Option<Duration> {
    self.keep_alive_interval
  }

  /// The configured unreliable transport mode.
  #[inline(always)]
  pub const fn unreliable_transport(&self) -> UnreliableTransport {
    self.unreliable_transport
  }

  /// The configured per-normalized-source pending-handshake ceiling, if any.
  #[inline(always)]
  pub const fn max_pending_connections_per_source(&self) -> Option<usize> {
    self.max_pending_connections_per_source
  }

  /// The configured IPv4 source-normalization prefix.
  #[inline(always)]
  pub const fn pending_source_prefix_v4(&self) -> u8 {
    self.pending_source_prefix_v4
  }

  /// The configured IPv6 source-normalization prefix.
  #[inline(always)]
  pub const fn pending_source_prefix_v6(&self) -> u8 {
    self.pending_source_prefix_v6
  }

  /// The configured coordinator-wide half-open inbound budget, if any.
  #[inline(always)]
  pub const fn max_pending_inbound_total(&self) -> Option<usize> {
    self.max_pending_inbound_total
  }

  /// Load the PEM material and assemble the rustls server/client pair under the
  /// configured [`ClientAuthMode`](crate::tls::ClientAuthMode), with
  /// memberlist's QUIC 0-RTT (early-data) policy FORCED off on both sides.
  ///
  /// Factored out of [`Self::build`] so a unit test can introspect the rustls
  /// configs BEFORE quinn's `QuicServerConfig`/`QuicClientConfig` `try_from`
  /// wrapping hides those fields.
  fn assemble_rustls_configs(
    &self,
  ) -> Result<(rustls::ServerConfig, rustls::ClientConfig), QuicConfigError> {
    let provider = rustls::crypto::CryptoProvider::get_default()
      .cloned()
      .ok_or(TlsConfigError::NoCryptoProvider)?;

    let certs = load_certs(&self.cert_file)?;
    let key = load_private_key(&self.key_file)?;
    let roots = load_roots(&self.ca_file)?;

    let mut rustls_server = build_server_config(&provider, &roots, &certs, &key, self.client_auth)?;
    let mut rustls_client = build_client_config(provider, roots, certs, key, self.client_auth)?;

    force_early_data_off(&mut rustls_client, &mut rustls_server);

    Ok((rustls_server, rustls_client))
  }

  /// Load the PEM files and assemble a [`QuicOptions`] under the configured
  /// [`ClientAuthMode`](crate::tls::ClientAuthMode) and transport tunables.
  ///
  /// Reuses the rustls server/client assembly from
  /// [`crate::tls::TlsConfigOptions`], wraps the result into quinn's QUIC crypto
  /// configs, builds an `EndpointConfig` with a random stateless-reset key, and
  /// installs a cluster-uniform SNI. Requires the process default
  /// `CryptoProvider` to be installed (see the module docs). Forces QUIC 0-RTT
  /// (early data) off on both sides — see [`Self::assemble_rustls_configs`].
  pub fn build(&self) -> Result<QuicOptions, QuicConfigError> {
    let (rustls_server, rustls_client) = self.assemble_rustls_configs()?;

    let qsc = QuicServerConfig::try_from(Arc::new(rustls_server))?;
    let server = QuinnServerConfig::with_crypto(Arc::new(qsc));
    let qcc = QuicClientConfig::try_from(Arc::new(rustls_client))?;
    let client = QuinnClientConfig::new(Arc::new(qcc));

    // The stateless-reset HMAC key is seeded from the backend the `quic-*`
    // crypto feature pulls into the graph (`EndpointConfig::default` is gated on
    // ring / aws-lc-rs being present).
    let endpoint = EndpointConfig::default();

    let mut transport = TransportConfig::default();
    // Connection migration stays enabled (quinn's default). The membership
    // identity a connection is keyed on is its logical/advertised address and is
    // immutable for the connection's life, independent of any post-migration
    // transport 4-tuple — so migration is transparent to the connection table.
    if let Some(t) = self.max_idle_timeout {
      // A zero idle-timeout transport parameter DISABLES the idle timeout (RFC
      // 9000 §18.2), removing the bound on stale-connection self-healing — the
      // opposite of the field's intent. quinn's `IdleTimeout::try_from` encodes
      // `as_millis()`, so any sub-millisecond duration (1ns .. 999_999ns) also
      // encodes to a disabling zero; reject on the encoded millisecond value, not
      // just an exactly-zero `Duration`. An unset timeout takes quinn's finite
      // default.
      if t.as_millis() == 0 {
        return Err(QuicConfigError::IdleTimeoutZero);
      }
      let idle = IdleTimeout::try_from(t).map_err(|_| QuicConfigError::IdleTimeoutTooLarge(t))?;
      transport.max_idle_timeout(Some(idle));
    }
    if let Some(k) = self.keep_alive_interval {
      transport.keep_alive_interval(Some(k));
    }

    Ok(
      QuicOptions::new(
        endpoint,
        server,
        client,
        transport,
        self.server_name.as_str(),
        self.unreliable_transport,
      )
      // Plumb the admission knobs — the per-source cap is the primary NAT
      // escape hatch and both prefixes plus the budget govern the source-key
      // normalization, so they must be reachable without a raw `QuicOptions`.
      .with_max_pending_connections_per_source(self.max_pending_connections_per_source)
      .with_pending_source_prefix_v4(self.pending_source_prefix_v4)
      .with_pending_source_prefix_v6(self.pending_source_prefix_v6)
      .with_max_pending_inbound_total(self.max_pending_inbound_total),
    )
  }
}

// `clap::Args` is delegated to a private mirror rather than derived on the
// public struct: every field except the three required paths carries a default,
// and a derived `update_from_arg_matches` treats every defaulted arg as present,
// so a `try_update_from` carrying one unrelated flag would reset all of them
// back to their defaults. The manual `update_from_arg_matches` applies a field
// only when its value came from the command line or an env var, so an unset
// defaulted field is a no-op on update.
//
// The `Option<usize>` admission caps are mirrored as plain `usize` args (with a
// default value): the CLI can therefore set a finite bound — including `0`
// (fail-closed) — but cannot express `None` (unbounded), which stays reachable
// via serde (`null`) or the programmatic builder. `From` wraps the parsed value
// in `Some`.
#[cfg(feature = "clap")]
#[cfg_attr(docsrs, doc(cfg(feature = "clap")))]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  #[derive(Args)]
  struct QuicConfigOptionsCli {
    #[arg(id = "quic-cert", long = "quic-cert", env = "MEMBERLIST_QUIC_CERT")]
    cert_file: PathBuf,
    #[arg(id = "quic-key", long = "quic-key", env = "MEMBERLIST_QUIC_KEY")]
    key_file: PathBuf,
    #[arg(id = "quic-ca", long = "quic-ca", env = "MEMBERLIST_QUIC_CA")]
    ca_file: PathBuf,
    #[arg(
      id = "quic-client-auth",
      long = "quic-client-auth",
      env = "MEMBERLIST_QUIC_CLIENT_AUTH",
      value_enum,
      default_value_t = ClientAuthMode::ClusterCa
    )]
    client_auth: ClientAuthMode,
    #[arg(
      id = "quic-server-name",
      long = "quic-server-name",
      env = "MEMBERLIST_QUIC_SERVER_NAME",
      default_value = "localhost"
    )]
    server_name: String,
    #[arg(
      id = "quic-max-idle-timeout",
      long = "quic-max-idle-timeout",
      env = "MEMBERLIST_QUIC_MAX_IDLE_TIMEOUT",
      value_parser = humantime::parse_duration
    )]
    max_idle_timeout: Option<Duration>,
    #[arg(
      id = "quic-keep-alive-interval",
      long = "quic-keep-alive-interval",
      env = "MEMBERLIST_QUIC_KEEP_ALIVE_INTERVAL",
      value_parser = humantime::parse_duration
    )]
    keep_alive_interval: Option<Duration>,
    #[arg(
      id = "quic-unreliable-transport",
      long = "quic-unreliable-transport",
      env = "MEMBERLIST_QUIC_UNRELIABLE_TRANSPORT",
      value_enum,
      default_value_t = UnreliableTransport::default()
    )]
    unreliable_transport: UnreliableTransport,
    #[arg(
      id = "quic-max-pending-per-source",
      long = "quic-max-pending-per-source",
      env = "MEMBERLIST_QUIC_MAX_PENDING_PER_SOURCE",
      default_value_t = DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE
    )]
    max_pending_connections_per_source: usize,
    #[arg(
      id = "quic-pending-source-prefix-v4",
      long = "quic-pending-source-prefix-v4",
      env = "MEMBERLIST_QUIC_PENDING_SOURCE_PREFIX_V4",
      default_value_t = DEFAULT_PENDING_SOURCE_PREFIX_V4
    )]
    pending_source_prefix_v4: u8,
    #[arg(
      id = "quic-pending-source-prefix-v6",
      long = "quic-pending-source-prefix-v6",
      env = "MEMBERLIST_QUIC_PENDING_SOURCE_PREFIX_V6",
      default_value_t = DEFAULT_PENDING_SOURCE_PREFIX_V6
    )]
    pending_source_prefix_v6: u8,
    #[arg(
      id = "quic-max-pending-inbound-total",
      long = "quic-max-pending-inbound-total",
      env = "MEMBERLIST_QUIC_MAX_PENDING_INBOUND_TOTAL",
      default_value_t = DEFAULT_MAX_PENDING_INBOUND_TOTAL
    )]
    max_pending_inbound_total: usize,
  }

  impl From<QuicConfigOptionsCli> for QuicConfigOptions {
    fn from(c: QuicConfigOptionsCli) -> Self {
      Self {
        cert_file: c.cert_file,
        key_file: c.key_file,
        ca_file: c.ca_file,
        client_auth: c.client_auth,
        server_name: c.server_name,
        max_idle_timeout: c.max_idle_timeout,
        keep_alive_interval: c.keep_alive_interval,
        unreliable_transport: c.unreliable_transport,
        max_pending_connections_per_source: Some(c.max_pending_connections_per_source),
        pending_source_prefix_v4: c.pending_source_prefix_v4,
        pending_source_prefix_v6: c.pending_source_prefix_v6,
        max_pending_inbound_total: Some(c.max_pending_inbound_total),
      }
    }
  }

  impl Args for QuicConfigOptions {
    fn augment_args(cmd: Command) -> Command {
      QuicConfigOptionsCli::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      QuicConfigOptionsCli::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for QuicConfigOptions {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      QuicConfigOptionsCli::from_arg_matches(m).map(Into::into)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // Apply ONLY operator-supplied overrides — args whose value came from the
      // command line or an env var, not a clap default.
      let supplied = |id: &str| {
        matches!(
          m.value_source(id),
          Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
        )
      };
      if supplied("quic-cert") {
        if let Some(v) = m.get_one::<PathBuf>("quic-cert") {
          self.cert_file = v.clone();
        }
      }
      if supplied("quic-key") {
        if let Some(v) = m.get_one::<PathBuf>("quic-key") {
          self.key_file = v.clone();
        }
      }
      if supplied("quic-ca") {
        if let Some(v) = m.get_one::<PathBuf>("quic-ca") {
          self.ca_file = v.clone();
        }
      }
      if supplied("quic-client-auth") {
        if let Some(v) = m.get_one::<ClientAuthMode>("quic-client-auth") {
          self.client_auth = *v;
        }
      }
      if supplied("quic-server-name") {
        if let Some(v) = m.get_one::<String>("quic-server-name") {
          self.server_name = v.clone();
        }
      }
      if supplied("quic-max-idle-timeout") {
        self.max_idle_timeout = m.get_one::<Duration>("quic-max-idle-timeout").copied();
      }
      if supplied("quic-keep-alive-interval") {
        self.keep_alive_interval = m.get_one::<Duration>("quic-keep-alive-interval").copied();
      }
      if supplied("quic-unreliable-transport") {
        if let Some(v) = m.get_one::<UnreliableTransport>("quic-unreliable-transport") {
          self.unreliable_transport = *v;
        }
      }
      if supplied("quic-max-pending-per-source") {
        self.max_pending_connections_per_source =
          m.get_one::<usize>("quic-max-pending-per-source").copied();
      }
      if supplied("quic-pending-source-prefix-v4") {
        if let Some(v) = m.get_one::<u8>("quic-pending-source-prefix-v4") {
          self.pending_source_prefix_v4 = *v;
        }
      }
      if supplied("quic-pending-source-prefix-v6") {
        if let Some(v) = m.get_one::<u8>("quic-pending-source-prefix-v6") {
          self.pending_source_prefix_v6 = *v;
        }
      }
      if supplied("quic-max-pending-inbound-total") {
        self.max_pending_inbound_total = m
          .get_one::<usize>("quic-max-pending-inbound-total")
          .copied();
      }
      Ok(())
    }
  }
};

/// A failure while building a [`QuicOptions`] from files.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum QuicConfigError {
  /// A cert / key / CA loading or rustls assembly failure — the shared TLS
  /// config error (see [`TlsConfigError`]).
  #[error(transparent)]
  Tls(#[from] TlsConfigError),
  /// The rustls config could not be converted into a QUIC crypto config (no
  /// QUIC-compatible TLS 1.3 cipher suite available).
  #[error(transparent)]
  Quinn(#[from] quinn_proto::crypto::rustls::NoInitialCipherSuite),
  /// The configured `max_idle_timeout` exceeds the QUIC varint encoding range.
  #[error("max_idle_timeout {0:?} is too large for the QUIC idle-timeout encoding")]
  IdleTimeoutTooLarge(Duration),
  /// The configured `max_idle_timeout` encodes to a disabling zero — it is zero,
  /// or a sub-millisecond duration that quinn's millisecond encoding rounds to
  /// zero. Per RFC 9000 §18.2 a zero idle-timeout transport parameter means the
  /// idle timeout is DISABLED — the opposite of what the field name suggests —
  /// which would remove the bound on stale-connection self-healing. Leave it
  /// unset for quinn's finite default, or configure at least one millisecond.
  #[error(
    "max_idle_timeout encodes to a disabling zero (RFC 9000 §18.2); leave it \
     unset for the default finite timeout, or configure at least 1ms"
  )]
  IdleTimeoutZero,
}

#[cfg(test)]
mod tests;
