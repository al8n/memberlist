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

use super::{QuicOptions, UnreliableTransport};

/// The cluster-uniform server name installed on [`QuicOptions::new`] when none
/// is configured. Every outbound dial presents this identity to the operator's
/// `ServerCertVerifier`.
const DEFAULT_SERVER_NAME: &str = "localhost";

fn default_server_name() -> String {
  DEFAULT_SERVER_NAME.to_string()
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

  /// Load the PEM files and assemble a [`QuicOptions`] under the configured
  /// [`ClientAuthMode`](crate::tls::ClientAuthMode) and transport tunables.
  ///
  /// Reuses the rustls server/client assembly from
  /// [`crate::tls::TlsConfigOptions`], wraps the result into quinn's QUIC crypto
  /// configs, builds an `EndpointConfig` with a random stateless-reset key, and
  /// installs a cluster-uniform SNI. Requires the process default
  /// `CryptoProvider` to be installed (see the module docs).
  pub fn build(&self) -> Result<QuicOptions, QuicConfigError> {
    let provider = rustls::crypto::CryptoProvider::get_default()
      .cloned()
      .ok_or(TlsConfigError::NoCryptoProvider)?;

    let certs = load_certs(&self.cert_file)?;
    let key = load_private_key(&self.key_file)?;
    let roots = load_roots(&self.ca_file)?;

    let rustls_server = build_server_config(&provider, &roots, &certs, &key, self.client_auth)?;
    let rustls_client = build_client_config(provider, roots, certs, key, self.client_auth)?;

    let qsc = QuicServerConfig::try_from(Arc::new(rustls_server))?;
    let server = QuinnServerConfig::with_crypto(Arc::new(qsc));
    let qcc = QuicClientConfig::try_from(Arc::new(rustls_client))?;
    let client = QuinnClientConfig::new(Arc::new(qcc));

    // The stateless-reset HMAC key is seeded from the backend the `quic-*`
    // crypto feature pulls into the graph (`EndpointConfig::default` is gated on
    // ring / aws-lc-rs being present).
    let endpoint = EndpointConfig::default();

    let mut transport = TransportConfig::default();
    if let Some(t) = self.max_idle_timeout {
      let idle = IdleTimeout::try_from(t).map_err(|_| QuicConfigError::IdleTimeoutTooLarge(t))?;
      transport.max_idle_timeout(Some(idle));
    }
    if let Some(k) = self.keep_alive_interval {
      transport.keep_alive_interval(Some(k));
    }

    Ok(QuicOptions::new(
      endpoint,
      server,
      client,
      transport,
      self.server_name.as_str(),
      self.unreliable_transport,
    ))
  }
}

// `clap::Args` is delegated to a private mirror rather than derived on the
// public struct: `client_auth`, `server_name`, and `unreliable_transport` each
// carry a default, and a derived `update_from_arg_matches` treats every
// defaulted arg as present, so a `try_update_from` carrying one unrelated flag
// would reset all three back to their defaults. The manual
// `update_from_arg_matches` applies a field only when its value came from the
// command line or an env var, so an unset defaulted field is a no-op on update.
#[cfg(feature = "clap")]
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
}

#[cfg(test)]
mod tests;
