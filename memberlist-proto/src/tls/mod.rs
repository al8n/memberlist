//! TLS-over-TCP record layer for the generic Sans-I/O stream transport.
//!
//! rustls is itself Sans-I/O — the TLS record layer lives in the deterministic
//! core and the driver moves only raw bytes. [`TlsRecords`] wraps a rustls
//! `ClientConnection` / `ServerConnection` behind a byte-only interface and
//! implements [`StreamTransport`].
//!
//! # Cluster label on the TLS reliable path
//!
//! Cluster isolation on the reliable path is the same one-time
//! `[LABELED_TAG=12][len][label]` exchange the plain-TCP and gossip planes use,
//! carried by the [`crate::streams::Labeled`] decorator — so the coordinator is
//! parameterised with `R = Labeled<TlsRecords>` and its options are
//! [`crate::streams::LabelOptions`] over [`TlsOptions`]
//! ([`LabelOptions<TlsOptions>`](crate::streams::LabelOptions)). The label is
//! the **decrypted plaintext** consumed before the bridge mints its `Stream`:
//! the dialer writes its label as the first application bytes inside the TLS
//! session (eager), and a labeled acceptor withholds BOTH its `Stream` mint and
//! its own label until the inbound label validates (lazy) — so a wrong-cluster
//! peer that completes only the TLS handshake learns nothing and merges
//! nothing. [`TlsOptions`] is unchanged: it never carries the label, which
//! rides [`LabelOptions`](crate::streams::LabelOptions) exclusively.
//!
//! A [`crate::streams::StreamEndpoint`] parameterised with
//! `R = Labeled<TlsRecords>` therefore carries reliable membership exchanges
//! over a per-exchange TLS-over-TCP connection — label-gated, then
//! TLS-encrypted (plain UDP carries the unreliable gossip on a separate
//! socket).

#[cfg(all(test, compression, encryption))]
mod bridge;
pub(crate) mod config;
#[cfg(test)]
mod conn;
mod options;
mod records;

use crate::Instant;

use rustls::pki_types::ServerName;

use crate::streams::transport::{Intake, StreamTransport};

pub use config::{ClientAuthMode, ParseClientAuthModeError, TlsConfigError, TlsConfigOptions};
pub use options::TlsOptions;
pub use records::TlsRecords;

/// The TLS record layer as a transport-agnostic [`StreamTransport`] plug.
///
/// Adapts the inherent [`TlsRecords`] surface — whose `handle_transport_data`
/// takes only `&[u8]` and returns `Result<records::Intake, rustls::Error>` —
/// to the unified trait surface: the `now` argument is accepted and dropped
/// (rustls is timer-free; deadlines live in the bridge), and the rustls `Err`
/// (mTLS reject, decrypt error, malformed record) is mapped to
/// [`Intake::Failed`] — the trait's terminal-reject variant — while the local
/// `records::Intake::{Done, Pending}` map straight across.
impl StreamTransport for TlsRecords {
  type Options = TlsOptions;
  type DialContext = ServerName<'static>;
  type ConstructError = rustls::Error;

  fn dial_context<A>(
    _addr: &A,
    server_name: Option<&str>,
  ) -> Result<ServerName<'static>, &'static str> {
    let sn = server_name.ok_or("tls dial_context called without server_name")?;
    ServerName::try_from(sn.to_owned()).map_err(|_| "tls server_name failed to parse")
  }

  fn dialer(opts: &Self::Options, ctx: Self::DialContext) -> Result<Self, Self::ConstructError> {
    TlsRecords::client(opts.client_arc(), ctx)
  }

  fn acceptor(opts: &Self::Options) -> Result<Self, Self::ConstructError> {
    TlsRecords::server(opts.server_arc())
  }

  fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    // The inherent method is timer-free, so `_now` is dropped. A rustls `Err`
    // is a terminal protocol failure (mTLS reject / decrypt / malformed
    // record) and maps to the unified `Intake::Failed`; `records::Intake` has
    // no `Failed` of its own — the failure rides the `Result`.
    match TlsRecords::handle_transport_data(self, input) {
      Ok(records::Intake::Done) => Intake::Done,
      Ok(records::Intake::Pending(n)) => Intake::Pending(n),
      Err(_) => Intake::Failed,
    }
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    TlsRecords::poll_transport_transmit(self, out)
  }

  fn is_handshaking(&self) -> bool {
    TlsRecords::is_handshaking(self)
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    TlsRecords::read_plaintext(self, out)
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> bool {
    TlsRecords::write_plaintext(self, plaintext)
  }

  fn set_send_capacity(&mut self, max_frame: usize) {
    TlsRecords::set_send_capacity(self, max_frame)
  }

  fn send_close_notify(&mut self) {
    TlsRecords::send_close_notify(self)
  }

  fn peer_has_closed(&self) -> bool {
    TlsRecords::peer_has_closed(self)
  }

  fn clear_outbound(&mut self) {
    // Drop the staged outbound plaintext (`pending`), so a failed exchange can
    // never feed a partial reply into rustls and onto the wire. rustls still
    // owns any ciphertext already fed on a prior transmit and exposes no API to
    // discard it; for the reliable plane that is harmless because each exchange
    // owns its socket, which is torn down on failure.
    TlsRecords::clear_outbound(self)
  }

  fn is_secure() -> bool {
    true
  }
}

#[cfg(test)]
mod tests;
