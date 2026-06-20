//! Build a [`TlsOptions`] from cert / key / CA files on disk.
//!
//! [`TlsOptions::new`](super::TlsOptions::new) accepts fully caller-built
//! `rustls::ServerConfig` / `ClientConfig` and makes no security policy choice.
//! This module is the convenience layer a CLI / daemon uses to construct those
//! two configs from PEM files plus a single [`ClientAuthMode`] choice, so an
//! operator configures the TLS reliable path with file paths and one auth flag
//! rather than hand-assembling rustls builders.
//!
//! # Crypto provider
//!
//! The builder reuses the **process default `CryptoProvider`** — the
//! application MUST install one before calling [`TlsConfigOptions::build`]
//! (e.g. `rustls::crypto::ring::default_provider().install_default()` or the
//! aws-lc-rs equivalent), matching the crate's backend-agnostic stance. A build
//! without an installed provider fails cleanly with
//! [`TlsConfigError::NoCryptoProvider`] rather than panicking.
//!
//! # Authentication models
//!
//! The single [`ClientAuthMode`] knob selects between the two deployment
//! models documented on [`super::TlsOptions`]:
//!
//! - [`ClientAuthMode::ClusterCa`] (default) — **mutual TLS**. The server
//!   installs a `WebPkiClientVerifier` over the CA roots and the client
//!   presents its own cert/key, so a peer without a cluster-CA-signed cert
//!   fails the handshake in both directions.
//! - [`ClientAuthMode::TrustedNetwork`] — **server-auth only**. The server
//!   accepts any client that completes the server-authenticated handshake; the
//!   client presents no cert. Use only when the network itself enforces cluster
//!   membership.

use std::{path::PathBuf, sync::Arc};

use rustls::{
  RootCertStore,
  pki_types::{CertificateDer, PrivateKeyDer},
  server::WebPkiClientVerifier,
};

use super::TlsOptions;

/// Client-authentication policy for the TLS reliable path. Selects between
/// mutual TLS over a cluster CA and server-authentication only. See the module
/// docs for the security semantics of each.
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Default, derive_more::IsVariant, derive_more::Display,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[display("{}", self.as_str())]
pub enum ClientAuthMode {
  /// Mutual TLS: the server verifies the client's certificate against the
  /// cluster CA roots and the client presents its own cluster-CA-signed
  /// certificate. A peer without a cluster-CA-signed cert fails the handshake.
  /// This is the default.
  #[default]
  ClusterCa,
  /// Server-authentication only: the server accepts any client that completes
  /// the server-authenticated handshake and the client presents no certificate.
  /// Use only when the network itself enforces cluster membership.
  TrustedNetwork,
}

impl ClientAuthMode {
  /// Returns the kebab-case string representation of the mode.
  #[inline(always)]
  pub const fn as_str(&self) -> &str {
    match self {
      ClientAuthMode::ClusterCa => "cluster-ca",
      ClientAuthMode::TrustedNetwork => "trusted-network",
    }
  }
}

/// Parse a [`ClientAuthMode`] from its kebab-case name (`"cluster-ca"` /
/// `"trusted-network"`) — the inverse of [`ClientAuthMode::as_str`], usable as
/// a config value or CLI value parser independent of `clap`.
impl core::str::FromStr for ClientAuthMode {
  type Err = ParseClientAuthModeError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(match s {
      "cluster-ca" => Self::ClusterCa,
      "trusted-network" => Self::TrustedNetwork,
      _ => return Err(ParseClientAuthModeError(())),
    })
  }
}

/// The error from [`ClientAuthMode`]'s `from_str`: the input named no known
/// client-auth mode.
///
/// Opaque — the private unit field seals construction to this module, so the
/// error can gain detail later without a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unknown client auth mode (expected \"cluster-ca\" or \"trusted-network\")")]
pub struct ParseClientAuthModeError(());

/// Construct a [`TlsOptions`] from cert / key / CA files on disk plus a single
/// [`ClientAuthMode`] choice.
///
/// `cert_file`, `key_file`, and `ca_file` are required PEM paths (no default);
/// `client_auth` defaults to [`ClientAuthMode::ClusterCa`] (mutual TLS). Call
/// [`Self::build`] to load the files and assemble the rustls server and client
/// configs. The process default `CryptoProvider` must be installed first — see
/// the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct TlsConfigOptions {
  /// Path to the PEM file holding this node's certificate chain.
  cert_file: PathBuf,
  /// Path to the PEM file holding this node's private key.
  key_file: PathBuf,
  /// Path to the PEM file holding the cluster CA certificate(s) used to verify
  /// peers (and, in [`ClientAuthMode::ClusterCa`], to verify inbound clients).
  ca_file: PathBuf,
  /// Client-authentication policy. Defaults to [`ClientAuthMode::ClusterCa`]
  /// (mutual TLS over the cluster CA).
  #[cfg_attr(feature = "serde", serde(default))]
  client_auth: ClientAuthMode,
}

impl TlsConfigOptions {
  /// Construct from the three required file paths with the default
  /// ([`ClientAuthMode::ClusterCa`]) auth mode.
  pub fn new(cert_file: PathBuf, key_file: PathBuf, ca_file: PathBuf) -> Self {
    Self {
      cert_file,
      key_file,
      ca_file,
      client_auth: ClientAuthMode::ClusterCa,
    }
  }

  /// Builder: set the client-authentication mode.
  #[must_use]
  #[inline(always)]
  pub const fn with_client_auth(mut self, mode: ClientAuthMode) -> Self {
    self.client_auth = mode;
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

  /// Load the PEM files and assemble a [`TlsOptions`] (server + client config)
  /// under the configured [`ClientAuthMode`].
  ///
  /// Requires the process default `CryptoProvider` to be installed (see the
  /// module docs); fails with [`TlsConfigError::NoCryptoProvider`] otherwise.
  /// Surfaces a precise [`TlsConfigError`] for a missing / unreadable / empty
  /// cert or key file, an unparsable CA, or any rustls assembly error.
  pub fn build(&self) -> Result<TlsOptions, TlsConfigError> {
    let provider = rustls::crypto::CryptoProvider::get_default()
      .cloned()
      .ok_or(TlsConfigError::NoCryptoProvider)?;

    let certs = load_certs(&self.cert_file)?;
    let key = load_private_key(&self.key_file)?;
    let roots = load_roots(&self.ca_file)?;

    let server = build_server_config(&provider, &roots, &certs, &key, self.client_auth)?;
    let client = build_client_config(provider, roots, certs, key, self.client_auth)?;

    Ok(TlsOptions::new(server, client))
  }
}

// `clap::Args` is delegated to a private mirror rather than derived on the
// public struct: `client_auth` carries a `default_value_t`, and a derived
// `update_from_arg_matches` treats every defaulted arg as present, so a
// `try_update_from` carrying one unrelated flag would reset `client_auth` back
// to its default. The manual `update_from_arg_matches` applies a field only
// when its value came from the command line or an env var, so an unset
// defaulted field is a no-op on update.
#[cfg(feature = "clap")]
#[cfg_attr(docsrs, doc(cfg(feature = "clap")))]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  #[derive(Args)]
  struct TlsConfigOptionsCli {
    #[arg(id = "tls-cert", long = "tls-cert", env = "MEMBERLIST_TLS_CERT")]
    cert_file: PathBuf,
    #[arg(id = "tls-key", long = "tls-key", env = "MEMBERLIST_TLS_KEY")]
    key_file: PathBuf,
    #[arg(id = "tls-ca", long = "tls-ca", env = "MEMBERLIST_TLS_CA")]
    ca_file: PathBuf,
    #[arg(
      id = "tls-client-auth",
      long = "tls-client-auth",
      env = "MEMBERLIST_TLS_CLIENT_AUTH",
      value_enum,
      default_value_t = ClientAuthMode::ClusterCa
    )]
    client_auth: ClientAuthMode,
  }

  impl From<TlsConfigOptionsCli> for TlsConfigOptions {
    fn from(c: TlsConfigOptionsCli) -> Self {
      Self {
        cert_file: c.cert_file,
        key_file: c.key_file,
        ca_file: c.ca_file,
        client_auth: c.client_auth,
      }
    }
  }

  impl Args for TlsConfigOptions {
    fn augment_args(cmd: Command) -> Command {
      TlsConfigOptionsCli::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      TlsConfigOptionsCli::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for TlsConfigOptions {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      TlsConfigOptionsCli::from_arg_matches(m).map(Into::into)
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
      if supplied("tls-cert") {
        if let Some(v) = m.get_one::<PathBuf>("tls-cert") {
          self.cert_file = v.clone();
        }
      }
      if supplied("tls-key") {
        if let Some(v) = m.get_one::<PathBuf>("tls-key") {
          self.key_file = v.clone();
        }
      }
      if supplied("tls-ca") {
        if let Some(v) = m.get_one::<PathBuf>("tls-ca") {
          self.ca_file = v.clone();
        }
      }
      if supplied("tls-client-auth") {
        if let Some(v) = m.get_one::<ClientAuthMode>("tls-client-auth") {
          self.client_auth = *v;
        }
      }
      Ok(())
    }
  }
};

/// Load a PEM certificate chain from `path`. Errors if the file is unreadable
/// or contains no certificates.
pub(crate) fn load_certs(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
  let bytes = std::fs::read(path).map_err(|e| TlsConfigError::ReadFile(path.clone(), e))?;
  let certs = rustls_pemfile::certs(&mut &bytes[..])
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| TlsConfigError::ReadFile(path.clone(), e))?;
  if certs.is_empty() {
    return Err(TlsConfigError::NoCerts(path.clone()));
  }
  Ok(certs)
}

/// Load a single PEM private key from `path`. Errors if the file is unreadable
/// or contains no key.
pub(crate) fn load_private_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
  let bytes = std::fs::read(path).map_err(|e| TlsConfigError::ReadFile(path.clone(), e))?;
  rustls_pemfile::private_key(&mut &bytes[..])
    .map_err(|e| TlsConfigError::ReadFile(path.clone(), e))?
    .ok_or_else(|| TlsConfigError::NoKey(path.clone()))
}

/// Load the CA certificates from `path` into a [`RootCertStore`]. Errors if the
/// file is unreadable, contains no certificates, or any cert is malformed.
pub(crate) fn load_roots(path: &PathBuf) -> Result<RootCertStore, TlsConfigError> {
  let certs = load_certs(path)?;
  let mut roots = RootCertStore::empty();
  for cert in certs {
    roots.add(cert)?;
  }
  Ok(roots)
}

/// Assemble the rustls `ServerConfig` for `mode`. In [`ClientAuthMode::ClusterCa`]
/// the server installs a `WebPkiClientVerifier` over `roots` (mutual TLS); in
/// [`ClientAuthMode::TrustedNetwork`] it disables client auth. Shared by the TLS
/// and QUIC from-files builders.
pub(crate) fn build_server_config(
  provider: &Arc<rustls::crypto::CryptoProvider>,
  roots: &RootCertStore,
  certs: &[CertificateDer<'static>],
  key: &PrivateKeyDer<'static>,
  mode: ClientAuthMode,
) -> Result<rustls::ServerConfig, TlsConfigError> {
  let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])?;
  let server = match mode {
    ClientAuthMode::ClusterCa => {
      let verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots.clone()), provider.clone())
          .build()?;
      builder.with_client_cert_verifier(verifier)
    }
    ClientAuthMode::TrustedNetwork => builder.with_no_client_auth(),
  };
  Ok(server.with_single_cert(certs.to_vec(), key.clone_key())?)
}

/// Assemble the rustls `ClientConfig` for `mode`. In [`ClientAuthMode::ClusterCa`]
/// the client presents `certs` / `key` (mutual TLS); in
/// [`ClientAuthMode::TrustedNetwork`] it presents no certificate. Shared by the
/// TLS and QUIC from-files builders. Consumes `roots`, `certs`, and `key`.
pub(crate) fn build_client_config(
  provider: Arc<rustls::crypto::CryptoProvider>,
  roots: RootCertStore,
  certs: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
  mode: ClientAuthMode,
) -> Result<rustls::ClientConfig, TlsConfigError> {
  let builder = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_root_certificates(roots);
  let client = match mode {
    ClientAuthMode::ClusterCa => builder.with_client_auth_cert(certs, key)?,
    ClientAuthMode::TrustedNetwork => builder.with_no_client_auth(),
  };
  Ok(client)
}

/// A failure while building a [`TlsOptions`] from files.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
  /// A configured PEM file could not be read (or its bytes could not be parsed
  /// as PEM). Carries the offending path and the underlying I/O error.
  #[error("failed to read {0}: {1}")]
  ReadFile(PathBuf, std::io::Error),
  /// The certificate file contained no certificates.
  #[error("no certificates found in {0}")]
  NoCerts(PathBuf),
  /// The key file contained no private key.
  #[error("no private key found in {0}")]
  NoKey(PathBuf),
  /// No process-default `CryptoProvider` is installed. The application must
  /// install one (e.g. `rustls::crypto::ring::default_provider().install_default()`)
  /// before building.
  #[error(
    "no default rustls CryptoProvider installed; call install_default() on your chosen backend \
     before building TLS options"
  )]
  NoCryptoProvider,
  /// The client-certificate verifier could not be built from the CA roots.
  #[error(transparent)]
  Verifier(#[from] rustls::server::VerifierBuilderError),
  /// A rustls assembly error (e.g. an invalid certificate or key).
  #[error(transparent)]
  Rustls(#[from] rustls::Error),
}

#[cfg(test)]
mod tests;
