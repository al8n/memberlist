//! QUIC crypto/config construction for the coordinator.
//!
//! # Cluster authentication on the QUIC reliable path
//!
//! The composed super-state-machine treats **QUIC's TLS 1.3 handshake as the
//! security layer** for the reliable path (push/pull, reliable-ping fallback,
//! reliable user messages). Memberlist's wire-level AES-GCM encryption is
//! skipped on QUIC — layering it atop QUIC's TLS is redundant double-
//! encryption. Memberlist's wire-label is similarly not applied
//! inside QUIC streams: cluster isolation comes from the operator's choice
//! of TLS authentication policy on the supplied `ServerConfig`.
//!
//! This is a deliberate asymmetry with the other two reliable transports.
//! Plain TCP (`Labeled<Passthrough>`) and TLS (`Labeled<TlsRecords>`) prepend
//! the cluster wire-label to each reliable stream, because neither layer
//! authenticates a peer's cluster membership on its own — there, the label is
//! the separation marker. QUIC authenticates the peer during the
//! per-connection TLS 1.3 handshake before any stream opens, so a second
//! in-stream label would add nothing. When several clusters share one CA, the
//! per-peer SNI server name (see [`SniProvider`]) is the discriminator: a peer
//! that presents the wrong server name does not complete the handshake the
//! local endpoint expects. QUIC's unreliable gossip path is plain UDP and
//! carries the cluster label like every other gossip plane.
//!
//! Two deployment models are supported by giving the caller full control
//! over the `quinn_proto::ServerConfig`:
//!
//! - **Trusted-network deployments**: build the server with
//!   `rustls::ServerConfig::builder_with_provider(...)
//!       .with_protocol_versions(&[&rustls::version::TLS13])?
//!       .with_no_client_auth()
//!       .with_single_cert(chain, key)?`.
//!   Any client that completes the server-authenticated TLS handshake can
//!   open streams. Use only when the network itself enforces cluster
//!   membership (private VPC, mTLS-terminating ingress proxy, …).
//!
//! - **Cluster-CA mTLS** (recommended for open networks): build the server
//!   with a `ClientCertVerifier` (typically
//!   `WebPkiClientVerifier::builder(root_store).build()?`) whose root store
//!   is the cluster CA. Peers without a cert signed by that CA fail the
//!   handshake — and a failed handshake produces no `EndpointEvent` side
//!   effects (no merge, no `UserDataReceived`, no reliable-ping ack).
//!
//! `QuicOptions::new` does NOT make this choice for the operator. It
//! accepts a fully caller-built `quinn_proto::EndpointConfig` /
//! `ServerConfig` / `ClientConfig` and only enforces the two
//! composition prerequisites the demux invariant demands:
//! `grease_quic_bit = false` (forced on the supplied `EndpointOptions`)
//! and `max_concurrent_uni_streams = 0` (forced on the supplied
//! `TransportConfig`).
//!
//! # Crypto backend
//!
//! The coordinator is crypto-backend-agnostic: it names no crypto
//! crate. The caller builds the `EndpointOptions` (whose stateless-reset
//! HMAC key comes from their chosen backend's `HmacKey`), the
//! `ServerConfig`, and the `ClientConfig` with whatever provider they
//! want (ring, aws-lc-rs, FIPS, …). Enable the matching `quic-*`
//! crypto-backend feature so that backend is present in the dependency
//! graph; the `EndpointOptions` is then typically built as
//! `EndpointOptions::new(Arc::new(<backend>::hmac::Key::new(HMAC_SHA256,
//! reset_key)))`.
//!
//! The sim path uses a self-signed cert + accept-any verifier with the
//! ring backend. Behavioral determinism comes from the injected virtual
//! clock, not from any test-only crypto hook.

use core::net::SocketAddr;
use std::sync::Arc;

use super::UnreliableTransport;

/// Per-peer SNI lookup. The coordinator calls this once per outbound dial,
/// passing the dialed `SocketAddr`; the returned string is forwarded to
/// `quinn_proto::Endpoint::connect(client, addr, server_name)` and reaches the
/// operator's `ServerCertVerifier::verify_server_cert(_, _, server_name, _, _)`
/// at handshake time.
pub type SniProvider = Arc<dyn Fn(&SocketAddr) -> Arc<str> + Send + Sync>;

/// Immutable QUIC config bundle handed to the coordinator. Accessor-only.
pub struct QuicOptions {
  endpoint: Arc<quinn_proto::EndpointConfig>,
  server: Arc<quinn_proto::ServerConfig>,
  client: quinn_proto::ClientConfig,
  /// Per-peer TLS verification identity. The coordinator invokes this closure
  /// once per outbound dial with the dialed `SocketAddr`; the returned string
  /// is forwarded to `quinn_proto::Endpoint::connect(client, addr,
  /// server_name)` and surfaces at the operator's
  /// `ServerCertVerifier::verify_server_cert(_, _, server_name, _, _)` call.
  ///
  /// Default mode (built by [`Self::new`]) is cluster-uniform: a closure that
  /// returns the same string for every peer. Per-peer SAN deployments — where
  /// each peer's cert names its own hostname/IP and the verifier matches the
  /// supplied `server_name` against the SAN — supply a closure via
  /// [`Self::new_with_sni_provider`] that maps each peer `SocketAddr` to the
  /// expected identity (rustls's `ServerCertVerifier` receives only the
  /// supplied `server_name`, not the dialed `SocketAddr`, so a custom verifier
  /// cannot re-derive per-peer SAN identity from the address alone).
  sni_provider: SniProvider,
  /// Which wire the unreliable path (gossip + probes) routes over, chosen at
  /// construction. In [`UnreliableTransport::Datagram`] mode the constructor
  /// enables quinn's datagram extension (the buffers are sized); in
  /// [`UnreliableTransport::Udp`] mode it forces `datagram_receive_buffer_size`
  /// to `None` (quinn enables datagrams by default, so this must be explicit) so
  /// the endpoint does NOT advertise datagram support and a cross-mode peer
  /// falls back to plain UDP. The driver reads this via
  /// [`Self::unreliable_transport`] to route unreliable sends.
  unreliable_transport: UnreliableTransport,
}

impl QuicOptions {
  /// Build from a caller-built endpoint config, server config, client
  /// config, and the single `transport` config the coordinator owns.
  ///
  /// The CALLER builds `endpoint`, `server`, and `client` — including
  /// the stateless-reset HMAC key (on the `EndpointOptions`), the TLS
  /// versions, cert chains, and (critically) the **client-cert verifier
  /// on the server side**. The crypto backend is the caller's choice;
  /// the coordinator names no crypto crate. The constructor's job is
  /// the two composition prerequisites the inbound first-byte demux
  /// invariant depends on, NOT the operator's security policy. See the
  /// module docs for the two supported cluster-auth deployment models
  /// (trusted-network with `with_no_client_auth` vs cluster-CA mTLS with
  /// `with_client_cert_verifier`).
  ///
  /// `endpoint` carries the stateless-reset HMAC key the caller seeded.
  /// The constructor forces `grease_quic_bit = false` on it (see below).
  ///
  /// `transport` is the single `quinn_proto::TransportConfig` the
  /// constructor installs on both the `ServerConfig` and the supplied
  /// `ClientConfig` (any `transport_config` already set on `server` /
  /// `client` is overwritten — the composed unit owns the transport
  /// policy of its endpoint, and a divergence between server and client
  /// direction would be a footgun). The caller controls every other
  /// tunable (`max_idle_timeout`, `stream_receive_window`, MTU
  /// discovery, congestion controller, …) by populating that
  /// `TransportConfig` before handing it in. The constructor
  /// unconditionally forces `max_concurrent_uni_streams = 0` on the
  /// supplied value (mutating it before moving it into a shared `Arc`,
  /// so a caller's surviving `Arc` view cannot accidentally re-enable
  /// remote-initiated unidirectional streams): memberlist's reliable
  /// exchanges (push/pull, reliable-ping fallback, reliable
  /// user-message) are ALL bidirectional, so a peer that opens a
  /// remote-initiated uni stream has no legitimate destination.
  /// Advertising zero credit is the protocol-layer refusal —
  /// quinn-proto's `Streams::open(Dir::Uni)` returns `None` on the
  /// sender side once `state.next[Uni] >= state.max[Uni]`, and no
  /// `Recv` is ever allocated on the receiver for an unwelcome
  /// uni-stream frame, so no remote uni-stream state can accumulate.
  ///
  /// quinn-proto APIs used here:
  /// - `EndpointOptions::grease_quic_bit(bool) -> &mut Self`.
  /// - `TransportConfig::max_concurrent_uni_streams(VarInt) -> &mut Self`.
  /// - `ServerConfig::transport_config(Arc<TransportConfig>) -> &mut Self`.
  /// - `ClientConfig::transport_config(Arc<TransportConfig>) -> &mut Self`.
  ///
  /// `server_name` installs the cluster-uniform TLS verification identity:
  /// every outbound dial reaches the operator's `ServerCertVerifier` with the
  /// same string. Per-peer SAN deployments must use
  /// [`Self::new_with_sni_provider`] instead — rustls's
  /// `ServerCertVerifier::verify_server_cert(_, _, server_name, _, _)` does not
  /// receive the dialed `SocketAddr`, so a verifier cannot re-derive per-peer
  /// SAN identity from the supplied address.
  ///
  /// `unreliable_transport` picks the wire for the unreliable path (gossip +
  /// probes) and is the single source of truth for whether quinn's datagram
  /// extension is enabled: [`UnreliableTransport::Datagram`] sizes the datagram
  /// buffers (the extension is advertised), [`UnreliableTransport::Udp`] forces
  /// `datagram_receive_buffer_size` to `None` so the endpoint does NOT advertise
  /// datagram support and cross-mode peers fall back to plain UDP.
  pub fn new(
    endpoint: quinn_proto::EndpointConfig,
    server: quinn_proto::ServerConfig,
    client: quinn_proto::ClientConfig,
    transport: quinn_proto::TransportConfig,
    server_name: impl Into<Arc<str>>,
    unreliable_transport: UnreliableTransport,
  ) -> Self {
    let name: Arc<str> = server_name.into();
    let provider: SniProvider = Arc::new(move |_addr: &SocketAddr| name.clone());
    Self::new_with_sni_provider(
      endpoint,
      server,
      client,
      transport,
      provider,
      unreliable_transport,
    )
  }

  /// Like [`Self::new`] but takes a per-peer SNI provider. The closure is
  /// invoked once per outbound dial with the dialed `SocketAddr`; the
  /// returned string is forwarded to
  /// `quinn_proto::Endpoint::connect(client, addr, server_name)` and reaches
  /// the operator's `ServerCertVerifier` at handshake time. Use this for
  /// per-peer SAN deployments where the verifier must see a different
  /// `server_name` for each peer.
  ///
  /// `unreliable_transport` governs the datagram extension exactly as for
  /// [`Self::new`]: enabled in [`UnreliableTransport::Datagram`] mode, left at
  /// quinn defaults (disabled, unadvertised) in [`UnreliableTransport::Udp`].
  pub fn new_with_sni_provider(
    mut endpoint: quinn_proto::EndpointConfig,
    mut server: quinn_proto::ServerConfig,
    mut client: quinn_proto::ClientConfig,
    mut transport: quinn_proto::TransportConfig,
    sni_provider: SniProvider,
    unreliable_transport: UnreliableTransport,
  ) -> Self {
    // Disable QUIC-bit greasing (RFC 9287). `EndpointOptions::new`
    // defaults `grease_quic_bit` to `true`, which advertises the
    // `grease_quic_bit` transport parameter so the PEER may clear
    // `FIXED_BIT` (`0x40`) on its outgoing short-header packets — and
    // quinn-proto's encoder is allowed to do the same on packets it sends
    // to a peer that advertised greasing. The composed coordinator's
    // inbound first-byte demux (`demux::classify`) is collision-free only
    // when every QUIC short header carries `FIXED_BIT` set (so the first
    // byte is `>= 0x40`) and every long-header packet has
    // `LONG_HEADER_FORM` (`0x80`) set; a FIXED_BIT-cleared short header
    // would route as `Class::Memberlist` (first byte ∈ `1..=15`) or
    // `Class::Reject`, handing valid post-handshake QUIC ciphertext to
    // the memberlist codec and silently dropping ACKs / stream data /
    // close packets. Disabling greasing on the constructed
    // `EndpointOptions` stops us advertising the transport parameter — a
    // protocol-compliant peer will not clear FIXED_BIT in packets it
    // sends us — and stops our encoder from greasing outbound. A single
    // setter call covers both directions: the same `Arc<EndpointOptions>`
    // is installed on every `quinn_proto::Endpoint` the coordinator
    // builds (`mod.rs::new` calls `Endpoint::new(cfg.endpoint_arc(),
    // ...)`), so server and client share this policy. Tradeoff: a
    // non-compliant peer that greases anyway would be rejected at packet
    // decode (quinn-proto rejects FIXED_BIT-cleared packets when this
    // side has greasing off); acceptable — the demux invariant is a
    // hard composition prerequisite and we own the deployment.
    endpoint.grease_quic_bit(false);
    let endpoint = Arc::new(endpoint);

    // Force `max_concurrent_uni_streams = 0` on the caller's transport
    // config, then move it into a single shared `Arc` installed on
    // both the server and client. The mutation happens BEFORE the
    // `Arc::new`, so any reference the caller still holds to the
    // value (impossible if they `move`d it as the signature requires,
    // but defensive against API misuse) is consumed.
    transport.max_concurrent_uni_streams(quinn_proto::VarInt::from_u32(0));

    // Govern quinn's datagram extension by the chosen mode. The advertised
    // `max_datagram_frame_size` transport parameter is derived SOLELY from
    // `datagram_receive_buffer_size`: quinn maps `Some(n) -> advertise`,
    // `None -> do not advertise` (transport_parameters.rs builds the param as
    // `config.datagram_receive_buffer_size.map(...)`). It also gates the local
    // SEND path on the receive buffer being set — `datagrams().send(...)`
    // returns `SendDatagramError::Disabled` when it is `None`. Crucially,
    // `TransportConfig::default()` sets `datagram_receive_buffer_size =
    // Some(STREAM_RWND)`, so datagrams are ON by default; the mode must drive
    // BOTH directions explicitly rather than relying on the default.
    //
    // - `Datagram` mode: size both buffers so the extension is advertised and
    //   the send path is enabled.
    // - `Udp` mode: force `datagram_receive_buffer_size(None)` so the endpoint
    //   does NOT advertise datagram support. A `Datagram`-mode peer then sees
    //   `datagrams().max_size() == None` for this connection, reports
    //   `NotReady`, and falls back to plain UDP. This is the true pure-UDP
    //   opt-out AND the mixed-mode interop guarantee — a `Udp` node never
    //   silently swallows a datagram a peer believed it could deliver, because
    //   the peer never negotiated one.
    //
    // Both setters run before `Arc::new(transport)` so the values propagate into
    // the shared `Arc` installed on the server and client configs.
    // (`grease_quic_bit` and `max_concurrent_uni_streams` above are not
    // mode-dependent and stay forced unconditionally.)
    match unreliable_transport {
      UnreliableTransport::Datagram => {
        transport.datagram_receive_buffer_size(Some(64 * 1024));
        transport.datagram_send_buffer_size(64 * 1024);
      }
      UnreliableTransport::Udp => {
        transport.datagram_receive_buffer_size(None);
      }
    }

    let transport = Arc::new(transport);
    server.transport_config(transport.clone());
    client.transport_config(transport);

    let server = Arc::new(server);

    Self {
      endpoint,
      server,
      client,
      sni_provider,
      unreliable_transport,
    }
  }

  /// Resolve the TLS verification identity for an outbound dial to `peer`.
  /// Invokes the stored per-peer SNI closure (see the field docs on [`Self`]
  /// for the verifier-side semantics).
  #[inline(always)]
  pub fn sni_for(&self, peer: &SocketAddr) -> Arc<str> {
    (self.sni_provider)(peer)
  }

  /// Returns the endpoint config (carries the stateless-reset HMAC key).
  #[inline(always)]
  pub fn endpoint_ref(&self) -> &quinn_proto::EndpointConfig {
    &self.endpoint
  }

  /// Returns a cheap clone of the endpoint config arc.
  #[inline(always)]
  pub fn endpoint_arc(&self) -> Arc<quinn_proto::EndpointConfig> {
    self.endpoint.clone()
  }

  /// Returns the server config (TLS cert + key, QUIC transport parameters).
  #[inline(always)]
  pub fn server_ref(&self) -> &quinn_proto::ServerConfig {
    &self.server
  }

  /// Returns a cheap clone of the server config arc.
  #[inline(always)]
  pub fn server_arc(&self) -> Arc<quinn_proto::ServerConfig> {
    self.server.clone()
  }

  /// Returns the client config used for outbound dials.
  pub fn client(&self) -> &quinn_proto::ClientConfig {
    &self.client
  }

  /// Which wire the unreliable path (gossip + probes) rides, as chosen at
  /// construction. [`UnreliableTransport::Datagram`] has quinn's datagram
  /// extension enabled; [`UnreliableTransport::Udp`] does not advertise it. The
  /// driver reads this to route unreliable sends.
  #[inline(always)]
  pub fn unreliable_transport(&self) -> UnreliableTransport {
    self.unreliable_transport
  }
}

#[cfg(test)]
pub(crate) mod tests;
