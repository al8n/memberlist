//! QUIC crypto/config construction for the coordinator.
//!
//! # Cluster authentication on the QUIC reliable path
//!
//! The composed coordinator treats **QUIC's TLS 1.3 handshake as the
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

use core::{net::SocketAddr, time::Duration};
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
  /// Global ceiling on the number of QUIC connections the coordinator will
  /// track at once (handshaking + established + still-draining), or `None` for
  /// no bound. An unauthenticated inbound Initial is refused before it commits
  /// connection-table state once this many connections already exist. See
  /// [`Self::max_quic_connections`] for the default and rationale.
  max_quic_connections: Option<usize>,
  /// Ceiling on the number of concurrent *pending* (handshaking) QUIC
  /// connections a single source address may hold, or `None` for no bound. An
  /// unauthenticated inbound Initial from a source already at this many
  /// in-flight handshakes is refused. See
  /// [`Self::max_pending_connections_per_source`] for the default and rationale.
  max_pending_connections_per_source: Option<usize>,
  /// Coordinator-wide ceiling on concurrently accepted INBOUND reliable-stream
  /// exchanges (bridges) across all QUIC connections, or `None` for no bound. An
  /// inbound bidi stream accepted beyond this many live inbound bridges is
  /// refused (both halves reset) instead of minting a bridge. Distinct from
  /// quinn's per-connection `max_concurrent_bidi_streams` — that bounds one
  /// connection; this bounds the inbound bridge population summed across all of
  /// them. See [`Self::max_inbound_streams`] for the default and rationale.
  max_inbound_streams: Option<usize>,
  /// Per-peer ceiling on OUTSTANDING (still-dialing) reliable user-message dial
  /// intents. Past this many parked intents to one peer,
  /// [`QuicEndpoint::start_user_message`](crate::quic::QuicEndpoint::start_user_message)
  /// refuses a further send with a `Busy`-class error instead of parking another
  /// intent — admission control on the node's own application load. Unlike the
  /// `Option<usize>` transport caps above there is no `None` (unbounded) form: an
  /// admission bound is always in effect, and a value of 0 is rejected by
  /// [`Self::validate`]. Push/pull and reliable-ping dials are exempt. See
  /// [`Self::max_pending_user_dials_per_peer`] for the default and rationale.
  max_pending_user_dials_per_peer: usize,
  /// Cadence of the coordinator's budget-deferred residue catch-up, and the width
  /// of the sub-interval window a near-deadline dial can miss. A smaller interval
  /// drains a deferred residue faster and shrinks the residual window; a larger one
  /// lowers the idle scheduled-tick rate at the cost of a wider residual. Governs the
  /// ready-bridge / ready-dial catch-up anchor cadence, the chained dial
  /// service-wake spacing, and the urgent front-deposit horizon. See
  /// [`Self::catchup_interval`] for the default and validation bounds.
  catchup_interval: Duration,
  /// How far before a parked dial's deadline the coordinator schedules that dial's
  /// pre-deadline SERVICE wake, so a budget-deferred but still-creditable dial is
  /// attempted while `now < deadline` — and minted — rather than retired at the
  /// deadline. See [`Self::dial_service_margin`] for the default and the safety
  /// floor.
  dial_service_margin: Duration,
  /// Maximum liveness-critical reliable-ping fallback dials the coordinator attempts
  /// PAST a per-pass dial-attempt budget, per peer bucket per pass. See
  /// [`Self::max_reliable_ping_exempt_pops_per_pass`] for the default and the safety
  /// floor.
  max_reliable_ping_exempt_pops_per_pass: usize,
}

/// Default global QUIC connection ceiling installed by [`QuicOptions::new`].
///
/// The coordinator pools one QUIC connection per peer, idle-evicted by quinn's
/// `max_idle_timeout`, so a node's steady-state connection set is bounded by its
/// gossip/probe fan-out within one idle window — in practice far below the
/// cluster size. `4096` leaves generous headroom for even a large LAN cluster's
/// working set while bounding the connection-table slab (each entry owns a
/// `quinn_proto::Connection`) to a survivable footprint against an Initial
/// flood. Operators with an exceptional working set raise it (or pass `None`)
/// via [`QuicOptions::with_max_quic_connections`].
pub const DEFAULT_MAX_QUIC_CONNECTIONS: usize = 4096;

/// Default per-source pending-handshake ceiling installed by
/// [`QuicOptions::new`].
///
/// A legitimate peer holds a single pooled connection per direction; transient
/// duplicates arise only from the closed-before-drained redial window and
/// simultaneous bidirectional dial (a small constant, and *pending* ones fewer
/// still — typically one at a time). `16` is an order of magnitude above that
/// legitimate maximum while capping the half-open handshake state one source
/// address (including a spoofed one) can pin, so a single source cannot consume
/// the global budget with half-open connections. Tunable via
/// [`QuicOptions::with_max_pending_connections_per_source`].
pub const DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE: usize = 16;

/// Default coordinator-wide inbound reliable-stream ceiling installed by
/// [`QuicOptions::new`].
///
/// Each accepted inbound bidi stream mints a bridge that transiently pins up to
/// ~3x `max_stream_frame_size` of reassembly buffer, so the coordinator bounds
/// the inbound bridge population ACROSS all connections — quinn's per-connection
/// `max_concurrent_bidi_streams` bounds only one connection, so without this a
/// flood of connections (up to [`DEFAULT_MAX_QUIC_CONNECTIONS`]) could each open
/// their full bidi allowance and mint an unbounded number of bridges.
///
/// Legitimate inbound reliable concurrency is the gossip/push-pull fan-in — in
/// the low tens even for a large LAN cluster — so `1024` is one to two orders of
/// magnitude of headroom while keeping the population hard-bounded. It sits below
/// [`DEFAULT_MAX_QUIC_CONNECTIONS`] (a per-tracked-connection factor well under
/// one), so a connection flood cannot multiply into an unbounded bridge
/// population: the coordinator admits at most this many inbound bridges
/// regardless of how many connections are open. With the default KB-scale
/// push/pull frames the footprint is modest; an operator who raises
/// `max_stream_frame_size` toward its ceiling should lower this correspondingly
/// (the product is the worst-case transient reassembly footprint). `None` opts
/// out of the bound; tune via [`QuicOptions::with_max_inbound_streams`].
pub const DEFAULT_MAX_QUIC_INBOUND_STREAMS: usize = 1024;

// A default of 0 would reject every inbound stream (the accept gate refuses when
// `inbound_live >= max`), silently disabling inbound reliable exchanges. Keep the
// default a usable nonzero bound.
const _: () = assert!(
  DEFAULT_MAX_QUIC_INBOUND_STREAMS > 0,
  "the default QUIC inbound-stream ceiling must be nonzero"
);

/// Default per-peer reliable user-message dial-backlog ceiling installed by
/// [`QuicOptions::new`].
///
/// Bounds the number of OUTSTANDING (still-dialing) reliable user-message
/// intents the coordinator will hold for a single peer before
/// [`QuicEndpoint::start_user_message`](crate::quic::QuicEndpoint::start_user_message)
/// refuses a further send with visible backpressure. An application that queues
/// reliable messages to one peer faster than that peer establishes or grants
/// stream credit would otherwise park an unbounded number of intents (each
/// toward its dial deadline), learning of the overload only via delayed deadline
/// failures. The bound makes the overload visible at the call site instead.
///
/// A legitimate application sends a handful of concurrent reliable messages to
/// any one peer; a pooled connection that is answering mints each immediately, so
/// a healthy peer never accumulates backlog at all (only a peer that cannot yet
/// open a stream parks). `32` is an order of magnitude above the legitimate
/// working set while keeping the per-peer parked-intent footprint hard-bounded.
/// Push/pull and reliable-ping dials are exempt (protocol-paced,
/// liveness-critical) and never counted. Raise it via
/// [`QuicOptions::with_max_pending_user_dials_per_peer`] for a bursty reliable
/// workload.
pub const DEFAULT_MAX_PENDING_USER_DIALS_PER_PEER: usize = 32;

// A ceiling of 0 would refuse EVERY reliable user-message dial (the admission
// gate refuses when `backlog >= limit`, and an empty backlog is already `>= 0`),
// silently disabling reliable user messages. Keep the default a usable nonzero
// bound; `QuicOptions::validate` rejects an operator-supplied 0 at construction.
const _: () = assert!(
  DEFAULT_MAX_PENDING_USER_DIALS_PER_PEER >= 1,
  "the default per-peer reliable user-message dial-backlog ceiling must be >= 1"
);

/// Default catch-up servicing cadence installed by [`QuicOptions::new`] — the
/// interval the coordinator paces its budget-deferred residue drain at.
///
/// The residue-drain and near-deadline-dial machinery is a fixed, table-independent
/// rate: each catch-up interval the coordinator pumps one budget-sized chunk of
/// deferred bridges and services one budget-sized chunk of budget-deferred parked
/// dials, so the per-interval work stays constant regardless of the residue size.
/// `10ms` is far inside any reliable-exchange deadline (seconds) while keeping the
/// idle scheduled-tick rate low. Tune via [`QuicOptions::with_catchup_interval`].
pub const DEFAULT_CATCHUP_INTERVAL: Duration = super::CATCHUP_INTERVAL;

/// Upper bound [`QuicOptions::validate`] accepts for
/// [`QuicOptions::catchup_interval`].
///
/// A catch-up interval beyond this would pace the residue drain so slowly that a
/// budget-deferred bridge or parked dial could strand past its exchange deadline
/// before the next catch-up pass ran. `1s` is already an order of magnitude above
/// the `10ms` default, leaving generous headroom for a deliberately slack cadence
/// while keeping the drain bounded well inside a reliable-exchange deadline.
pub const MAX_CATCHUP_INTERVAL: Duration = Duration::from_secs(1);

/// Default dial service margin installed by [`QuicOptions::new`] — how far before a
/// parked dial's deadline the coordinator schedules its pre-deadline service wake.
///
/// Set to two [`DEFAULT_CATCHUP_INTERVAL`]s: one interval of driver-timer jitter
/// allowance plus one chained mid-window service tick, so a budget-deferred but
/// still-creditable dial is attempted (and minted) strictly before its deadline
/// rather than retired at it. Tune via [`QuicOptions::with_dial_service_margin`].
pub const DEFAULT_DIAL_SERVICE_MARGIN: Duration = super::DIAL_SERVICE_MARGIN;

/// Default per-pass reliable-ping exempt-pop cap installed by [`QuicOptions::new`].
///
/// The pass-budget twin of the reliable-ping outbound-cap exemption: a
/// liveness-critical reliable-ping fallback at the front of a peer's parked bucket is
/// attempted even when a pass's shared dial-attempt budget is spent, so it is never
/// deferred to its cumulative probe deadline (a false-positive Dead) behind a
/// user-message flood. `8` equals the membership `awareness_max_multiplier` default —
/// the bound on how many same-peer reliable-ping fallbacks can stack at once — so an
/// honest fallback is never deferred at the default configuration. See
/// [`QuicOptions::max_reliable_ping_exempt_pops_per_pass`] for the safety floor.
pub const DEFAULT_MAX_RELIABLE_PING_EXEMPT_POPS_PER_PASS: usize = 8;

// A zero exempt-pop cap would defer EVERY reliable-ping fallback that arrives on a
// budget-spent pass to its cumulative deadline — a false Suspect of a live peer.
// Keep the default a usable nonzero cap; `QuicOptions::validate` rejects a supplied 0.
const _: () = assert!(
  DEFAULT_MAX_RELIABLE_PING_EXEMPT_POPS_PER_PASS >= 1,
  "the default reliable-ping exempt-pop cap must be >= 1"
);

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
  /// Configuration note on `receive_window`: the coordinator caps the outbound
  /// reliable-stream population it opens per connection at a local constant
  /// (`C_OUT`), so the stream set quinn re-enumerates on every connection-window
  /// MAX_DATA is bounded by a config constant rather than by the peer's advertised
  /// `MAX_STREAMS`. With the default (effectively unbounded) `receive_window` the
  /// connection-level write limit reaches zero only under ACK starvation (a
  /// crashed or partitioned peer), so that blocked set is touched only under peer
  /// failure and self-frees when the connection is retired. Supplying a FINITE
  /// `receive_window` on this `transport` makes ordinary connection-window
  /// backpressure drive the same re-enumeration: it stays bounded by the same
  /// config constant per blocked-episode, but is now exercised in normal operation
  /// rather than only under a stalled peer. Lowering `receive_window` trades peak
  /// per-connection memory for that per-episode re-enumeration cost.
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
      max_quic_connections: Some(DEFAULT_MAX_QUIC_CONNECTIONS),
      max_pending_connections_per_source: Some(DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE),
      max_inbound_streams: Some(DEFAULT_MAX_QUIC_INBOUND_STREAMS),
      max_pending_user_dials_per_peer: DEFAULT_MAX_PENDING_USER_DIALS_PER_PEER,
      catchup_interval: DEFAULT_CATCHUP_INTERVAL,
      dial_service_margin: DEFAULT_DIAL_SERVICE_MARGIN,
      max_reliable_ping_exempt_pops_per_pass: DEFAULT_MAX_RELIABLE_PING_EXEMPT_POPS_PER_PASS,
    }
  }

  /// Override the global QUIC connection ceiling (handshaking + established +
  /// still-draining). `None` removes the bound. Enforced against an
  /// unauthenticated inbound Initial before any connection-table state is
  /// committed. Defaults to [`Some`]`(`[`DEFAULT_MAX_QUIC_CONNECTIONS`]`)`.
  #[must_use]
  #[inline(always)]
  pub fn with_max_quic_connections(mut self, max: Option<usize>) -> Self {
    self.max_quic_connections = max;
    self
  }

  /// Override the per-source pending-handshake ceiling. `None` removes the
  /// bound. Enforced against an unauthenticated inbound Initial before any
  /// connection-table state is committed. Defaults to [`Some`]`(`[`DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE`]`)`.
  #[must_use]
  #[inline(always)]
  pub fn with_max_pending_connections_per_source(mut self, max: Option<usize>) -> Self {
    self.max_pending_connections_per_source = max;
    self
  }

  /// Override the coordinator-wide inbound reliable-stream ceiling. `None`
  /// removes the bound. Bounds the inbound bridge population across all QUIC
  /// connections (an inbound bidi stream accepted beyond the ceiling is refused
  /// instead of minting a bridge). Defaults to
  /// [`Some`]`(`[`DEFAULT_MAX_QUIC_INBOUND_STREAMS`]`)`.
  #[must_use]
  #[inline(always)]
  pub fn with_max_inbound_streams(mut self, max: Option<usize>) -> Self {
    self.max_inbound_streams = max;
    self
  }

  /// Override the per-peer reliable user-message dial-backlog ceiling. A value of
  /// `0` disables reliable user messages entirely and is rejected by
  /// [`Self::validate`] at construction. Bounds the OUTSTANDING (still-dialing)
  /// reliable user-message intents to any one peer; a further send past the
  /// ceiling is refused with backpressure. Defaults to
  /// [`DEFAULT_MAX_PENDING_USER_DIALS_PER_PEER`].
  #[must_use]
  #[inline(always)]
  pub const fn with_max_pending_user_dials_per_peer(mut self, max: usize) -> Self {
    self.max_pending_user_dials_per_peer = max;
    self
  }

  /// Override the catch-up servicing cadence (residue-drain interval, chained
  /// service-wake spacing, and urgent front-deposit horizon). Must be `> 0` and
  /// `<=` [`MAX_CATCHUP_INTERVAL`], enforced by [`Self::validate`]. Defaults to
  /// [`DEFAULT_CATCHUP_INTERVAL`].
  #[must_use]
  #[inline(always)]
  pub const fn with_catchup_interval(mut self, interval: Duration) -> Self {
    self.catchup_interval = interval;
    self
  }

  /// Override the dial service margin — how far before a parked dial's deadline the
  /// coordinator schedules its pre-deadline service wake. Must be `>=`
  /// [`Self::catchup_interval`], enforced by [`Self::validate`] (a smaller margin
  /// could let the deadline fire before any pre-deadline service tick lands).
  /// Defaults to [`DEFAULT_DIAL_SERVICE_MARGIN`].
  #[must_use]
  #[inline(always)]
  pub const fn with_dial_service_margin(mut self, margin: Duration) -> Self {
    self.dial_service_margin = margin;
    self
  }

  /// Override the per-pass reliable-ping exempt-pop cap. A value of `0` disables the
  /// exemption and is rejected by [`Self::validate`]. It MUST stay `>=` the
  /// membership `awareness_max_multiplier` or honest reliable-ping fallbacks defer to
  /// a false Suspect — see [`Self::max_reliable_ping_exempt_pops_per_pass`]. Defaults
  /// to [`DEFAULT_MAX_RELIABLE_PING_EXEMPT_POPS_PER_PASS`].
  #[must_use]
  #[inline(always)]
  pub const fn with_max_reliable_ping_exempt_pops_per_pass(mut self, max: usize) -> Self {
    self.max_reliable_ping_exempt_pops_per_pass = max;
    self
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

  /// The global QUIC connection ceiling the coordinator enforces against
  /// unauthenticated inbound Initials, or `None` for no bound. Defaults to
  /// [`Some`]`(`[`DEFAULT_MAX_QUIC_CONNECTIONS`]`)`; see that constant for the
  /// rationale.
  #[inline(always)]
  pub const fn max_quic_connections(&self) -> Option<usize> {
    self.max_quic_connections
  }

  /// The per-source pending-handshake ceiling the coordinator enforces against
  /// unauthenticated inbound Initials, or `None` for no bound. Defaults to
  /// [`Some`]`(`[`DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE`]`)`; see that
  /// constant for the rationale.
  #[inline(always)]
  pub const fn max_pending_connections_per_source(&self) -> Option<usize> {
    self.max_pending_connections_per_source
  }

  /// The coordinator-wide inbound reliable-stream ceiling the QUIC accept loop
  /// enforces before minting a bridge, or `None` for no bound. Defaults to
  /// [`Some`]`(`[`DEFAULT_MAX_QUIC_INBOUND_STREAMS`]`)`; see that constant for
  /// the rationale.
  #[inline(always)]
  pub const fn max_inbound_streams(&self) -> Option<usize> {
    self.max_inbound_streams
  }

  /// The per-peer reliable user-message dial-backlog ceiling the coordinator
  /// enforces in
  /// [`QuicEndpoint::start_user_message`](crate::quic::QuicEndpoint::start_user_message).
  /// Defaults to [`DEFAULT_MAX_PENDING_USER_DIALS_PER_PEER`]; see that constant
  /// for the rationale.
  #[inline(always)]
  pub const fn max_pending_user_dials_per_peer(&self) -> usize {
    self.max_pending_user_dials_per_peer
  }

  /// The catch-up servicing cadence the coordinator paces its budget-deferred
  /// residue drain at, and the width of the sub-interval window a near-deadline dial
  /// can miss. Governs the ready-bridge / ready-dial catch-up anchor, the chained
  /// dial service-wake spacing, and the urgent front-deposit horizon. Defaults to
  /// [`DEFAULT_CATCHUP_INTERVAL`]; [`Self::validate`] rejects a zero (which would
  /// break the residual-window anchor) or a value above [`MAX_CATCHUP_INTERVAL`]
  /// (which would strand residue past its deadline).
  #[inline(always)]
  pub const fn catchup_interval(&self) -> Duration {
    self.catchup_interval
  }

  /// The dial service margin — how far before a parked dial's deadline the
  /// coordinator schedules that dial's pre-deadline SERVICE wake, so a
  /// budget-deferred but still-creditable dial is attempted (and minted) while
  /// `now < deadline` rather than retired at the deadline. Defaults to
  /// [`DEFAULT_DIAL_SERVICE_MARGIN`] (two catch-up intervals). [`Self::validate`]
  /// rejects a value below [`Self::catchup_interval`]: a smaller margin could let the
  /// deadline fire before any pre-deadline service tick landed in the window,
  /// silently reopening the spurious-retire-under-capacity failure this margin
  /// closes.
  #[inline(always)]
  pub const fn dial_service_margin(&self) -> Duration {
    self.dial_service_margin
  }

  /// The per-pass reliable-ping exempt-pop cap: the maximum liveness-critical
  /// reliable-ping fallback dials the coordinator attempts past a peer bucket's
  /// per-pass dial-attempt budget. A reliable-ping fallback deferred to its
  /// cumulative probe deadline yields a false-positive Dead, so this exemption keeps
  /// an honest fallback from stranding behind a user-message flood. Defaults to
  /// [`DEFAULT_MAX_RELIABLE_PING_EXEMPT_POPS_PER_PASS`].
  ///
  /// SAFETY FLOOR: keep this `>=` the membership `awareness_max_multiplier` (config
  /// default 8), which bounds how many same-peer reliable-ping fallbacks can stack at
  /// once. A smaller cap would defer honest fallbacks past the budget and risk a
  /// false Suspect. The coordinator cannot cheaply read the membership awareness
  /// bound at runtime (the composed endpoint exposes no accessor for it), so
  /// [`Self::validate`] only rejects a zero cap; the default matches the awareness
  /// default so the floor holds unless an operator lowers one without the other.
  #[inline(always)]
  pub const fn max_reliable_ping_exempt_pops_per_pass(&self) -> usize {
    self.max_reliable_ping_exempt_pops_per_pass
  }

  /// Validate operator-supplied option values, rejecting a configuration the
  /// coordinator cannot service. Reject rather than panic or silently ship a
  /// footgun (mirrors [`Endpoint::try_new_at`](crate::endpoint::Endpoint::try_new_at)'s
  /// operator-misconfiguration guards).
  ///
  /// The guards, each rejecting a value that would break a coordinator invariant:
  ///
  /// * `max_pending_user_dials_per_peer` must be `>= 1` — a `0` ceiling refuses every
  ///   reliable user-message dial (the admission gate refuses when `backlog >=
  ///   limit`, and an empty backlog already satisfies `>= 0`).
  /// * `catchup_interval` must be `> 0` — a zero interval would collapse the
  ///   residual-window anchor and the chained service-wake spacing — and `<=`
  ///   [`MAX_CATCHUP_INTERVAL`] — a larger cadence could strand residue past its
  ///   exchange deadline.
  /// * `dial_service_margin` must be `>=` `catchup_interval` — a smaller margin could
  ///   let a parked dial's deadline fire before any pre-deadline service tick landed
  ///   in the window, silently reopening the spurious-retire-under-capacity failure
  ///   the margin exists to close. Cross-field: checked against `catchup_interval`.
  /// * `max_reliable_ping_exempt_pops_per_pass` must be `>= 1` — a `0` cap defers
  ///   every over-budget reliable-ping fallback to its cumulative deadline, a false
  ///   Suspect of a live peer.
  ///
  /// Callers that assemble a [`QuicOptions`] from operator input run this before
  /// handing it to the coordinator.
  pub const fn validate(&self) -> Result<(), QuicOptionsError> {
    if self.max_pending_user_dials_per_peer == 0 {
      return Err(QuicOptionsError::MaxPendingUserDialsPerPeerZero);
    }
    if self.catchup_interval.is_zero() {
      return Err(QuicOptionsError::CatchupIntervalZero);
    }
    if self.catchup_interval.as_nanos() > MAX_CATCHUP_INTERVAL.as_nanos() {
      return Err(QuicOptionsError::CatchupIntervalTooLarge);
    }
    if self.dial_service_margin.as_nanos() < self.catchup_interval.as_nanos() {
      return Err(QuicOptionsError::DialServiceMarginBelowCatchupInterval);
    }
    if self.max_reliable_ping_exempt_pops_per_pass == 0 {
      return Err(QuicOptionsError::MaxReliablePingExemptPopsPerPassZero);
    }
    Ok(())
  }
}

/// Error returned by [`QuicOptions::validate`] when an operator-supplied option
/// value is unusable. `#[non_exhaustive]`: further option guards may be added
/// without a breaking change.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum QuicOptionsError {
  /// `max_pending_user_dials_per_peer` was set to `0`, which would refuse every
  /// reliable user-message dial (the admission gate refuses when
  /// `backlog >= limit`). Set it to `>= 1`.
  #[error("max_pending_user_dials_per_peer must be >= 1")]
  MaxPendingUserDialsPerPeerZero,
  /// `catchup_interval` was set to `0`, which would collapse the residue-drain
  /// cadence and the chained dial service-wake spacing. Set it to `> 0`.
  #[error("catchup_interval must be > 0")]
  CatchupIntervalZero,
  /// `catchup_interval` exceeds [`MAX_CATCHUP_INTERVAL`], a cadence so slack a
  /// budget-deferred residue could strand past its exchange deadline. Set it to
  /// `<= 1s`.
  #[error("catchup_interval must be <= 1s")]
  CatchupIntervalTooLarge,
  /// `dial_service_margin` is smaller than `catchup_interval`, so a parked dial's
  /// deadline could fire before any pre-deadline service tick landed in the window —
  /// reopening the spurious-retire-under-capacity failure. Set it to `>=
  /// catchup_interval`.
  #[error("dial_service_margin must be >= catchup_interval")]
  DialServiceMarginBelowCatchupInterval,
  /// `max_reliable_ping_exempt_pops_per_pass` was set to `0`, which would defer every
  /// over-budget reliable-ping fallback to its cumulative deadline (a false Suspect).
  /// Set it to `>= 1` (and `>=` the membership awareness max).
  #[error("max_reliable_ping_exempt_pops_per_pass must be >= 1")]
  MaxReliablePingExemptPopsPerPassZero,
}

#[cfg(test)]
pub(crate) mod tests;
