//! TLS options bundle for the coordinator.
//!
//! # Cluster authentication on the TLS reliable path
//!
//! The composed coordinator treats the **TLS handshake as the
//! security layer** for the reliable path (push/pull, reliable-ping fallback,
//! reliable user messages). Memberlist's wire-level AES-GCM and wire-label are
//! not applied inside a TLS connection — the TLS record layer supersedes them;
//! cluster isolation comes from the operator's choice of TLS authentication
//! policy on the supplied `ServerConfig` / `ClientConfig`.
//!
//! Two deployment models, both expressed by the caller-built configs:
//!
//! - **Trusted-network**: a `ServerConfig` built with `with_no_client_auth()`.
//!   Any client that completes the server-authenticated TLS handshake can open
//!   a reliable exchange. Use only when the network enforces cluster
//!   membership (private VPC, mTLS-terminating ingress, …).
//!
//! - **Cluster-CA mTLS** (recommended on open networks): a `ServerConfig`
//!   built with a `ClientCertVerifier` (e.g.
//!   `WebPkiClientVerifier::builder_with_provider(cluster_ca_roots, provider)
//!       .build()?`) whose roots are the cluster CA, and a `ClientConfig` that
//!   presents a cluster-CA-signed client cert (`with_client_auth_cert`). A
//!   peer without a cluster-CA-signed cert fails the mutual handshake before
//!   any `Stream` exists — so no merge, no `UserDataReceived`, no reliable-ping
//!   ack.
//!
//! `TlsOptions::new` does NOT make this choice for the operator. It accepts a
//! fully caller-built `rustls::ServerConfig` / `ClientConfig` and only
//! Arc-wraps them. The crypto backend is the caller's choice (ring / aws-lc-rs
//! / FIPS via the matching `tls-rustls-*` feature); the coordinator names no
//! crypto crate.

use std::sync::Arc;

/// Immutable TLS options bundle handed to the coordinator. Accessor-only.
pub struct TlsOptions {
  server: Arc<rustls::ServerConfig>,
  client: Arc<rustls::ClientConfig>,
}

impl TlsOptions {
  /// Bundle a caller-built server config (carrying its `ClientCertVerifier`)
  /// and client config. Arc-wraps them and mutates nothing security-relevant —
  /// the operator owns the mTLS policy. See the module docs for the two
  /// supported cluster-auth deployment models.
  pub fn new(server: rustls::ServerConfig, client: rustls::ClientConfig) -> Self {
    Self {
      server: Arc::new(server),
      client: Arc::new(client),
    }
  }

  /// The server config used to accept inbound reliable connections.
  #[inline(always)]
  pub fn server_ref(&self) -> &rustls::ServerConfig {
    &self.server
  }

  /// Return a cheap clone of the server config arc.
  #[inline(always)]
  pub fn server_arc(&self) -> Arc<rustls::ServerConfig> {
    self.server.clone()
  }

  /// The client config used for outbound dials.
  #[inline(always)]
  pub fn client_ref(&self) -> &rustls::ClientConfig {
    &self.client
  }

  /// Return a cheap clone of the client config arc.
  #[inline(always)]
  pub fn client_arc(&self) -> Arc<rustls::ClientConfig> {
    self.client.clone()
  }
}

#[cfg(test)]
pub(crate) mod tests;
