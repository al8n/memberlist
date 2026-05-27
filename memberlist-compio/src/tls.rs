//! TLS-backed memberlist driver.
//!
//! [`TlsMemberlist::new`] pins the generic [`crate::Memberlist`] handle to
//! the concrete `I = SmolStr`, `A = SocketAddr`, `R = TlsRecords`
//! instantiation, binds the UDP gossip socket, constructs the composed
//! [`StreamEndpoint`] super-state-machine with the rustls record layer, and
//! spawns the driver task on the compio runtime.
//!
//! ## Server name
//!
//! TLS requires a server name to verify the peer's certificate against. The
//! driver's SNI provider closure is called per dial with the peer's membership
//! address; it must return `Some(name)` matching the peer cert's SAN/CN.
//! Returning `None` causes the dial to fail with `"tls bridge returned None
//! for server_name"`. The bundled smoke tests use `"localhost"` to match the
//! self-signed localhost-SAN certs; production operators return the peer's
//! actual DNS name or SAN.

#![cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]

use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
  },
};

use arc_swap::ArcSwap;
use compio::net::{TcpListener, UdpSocket};
use memberlist_machine::{
  TlsOptions, TlsRecords, config::EndpointConfig, endpoint::Endpoint, streams::StreamEndpoint,
};
use memberlist_wire::Node;
use smol_str::SmolStr;

use crate::{
  Memberlist, MemberlistError, MemberlistSnapshot, Result, StreamDriverOptions, driver::driver_loop,
};

/// TLS-backed [`Memberlist`] alias — pins `I = SmolStr`, `A = SocketAddr`,
/// `R = TlsRecords`.
pub type TlsMemberlist = Memberlist<SmolStr, SocketAddr, TlsRecords>;

impl TlsMemberlist {
  /// Construct a TLS memberlist, bind the UDP gossip socket, and spawn the
  /// driver task on the current compio runtime.
  ///
  /// `config` carries the membership-level settings (local id, advertise
  /// address, SWIM timing, gossip MTU, etc.); `tls_opts` carries the
  /// caller-built `rustls::ServerConfig` / `ClientConfig` bundle that
  /// governs the TLS handshake on the reliable path. The UDP socket binds
  /// to `config.advertise_addr_ref()`; per-exchange TLS-over-TCP connections
  /// are dialed/accepted on demand inside the driver task.
  ///
  /// On return the driver task is already running. Reads (`snapshot`,
  /// `local_node`, `alive_count`, `member_count`) are served lock-free from
  /// the initial snapshot until the driver publishes its first refresh.
  ///
  /// # Errors
  ///
  /// Returns [`MemberlistError::Io`] if binding the UDP gossip socket fails
  /// (most commonly `EADDRINUSE` on a port collision).
  pub async fn new(
    config: EndpointConfig<SmolStr, SocketAddr>,
    tls_opts: TlsOptions,
  ) -> Result<Self> {
    Self::new_with_options(config, tls_opts, StreamDriverOptions::default()).await
  }

  /// Construct a TLS-backed memberlist with explicit per-driver tuning
  /// knobs. Most callers want [`Self::new`] (defaults); reach for this
  /// when a specific knob — `join_deadline`, `iter_drain_cap`,
  /// `event_queue_cap`, etc. — must deviate from
  /// [`StreamDriverOptions::default()`].
  ///
  /// See [`Self::new`] for the full error surface.
  pub async fn new_with_options(
    config: EndpointConfig<SmolStr, SocketAddr>,
    tls_opts: TlsOptions,
    driver_opts: StreamDriverOptions,
  ) -> Result<Self> {
    // 1. Bind UDP gossip socket + TCP reliable listener on the configured
    //    advertise address. Both share the same socket-address tuple.
    //    Both TCP and TLS are carried over raw TcpStream — the TLS
    //    handshake bytes flow through the same byte path as application
    //    bytes; the rustls record-layer codec inside `StreamEndpoint`
    //    distinguishes them internally.
    let local_addr: SocketAddr = *config.advertise_addr_ref();
    let local_id: SmolStr = config.local_id_ref().clone();
    let gossip_socket = UdpSocket::bind(local_addr)
      .await
      .map_err(MemberlistError::Io)?;
    let listener = TcpListener::bind(local_addr)
      .await
      .map_err(MemberlistError::Io)?;

    // 2. Build the composed super-state-machine. `Endpoint::new` inserts the
    //    local node as Alive at incarnation 1; `StreamEndpoint` wraps that with
    //    the `TlsRecords` record layer plug and the supplied `TlsOptions`. The
    //    `sni_provider` closure resolves the per-peer SNI hint `TlsRecords::
    //    dial_context` consumes.
    let endpoint: StreamEndpoint<SmolStr, SocketAddr, TlsRecords> = StreamEndpoint::new(
      Endpoint::new(config),
      tls_opts,
      Box::new(|_addr: &SocketAddr| Some("localhost".to_string())),
      Box::new(|addr: &SocketAddr| *addr),
    );

    // 3. Channels (see TcpMemberlist::new for the per-channel
    //    bounding rationale):
    //    - commands     : unbounded (every command acks via one-shot).
    //    - bridge_ready : unbounded (driver-internal producers).
    //    - events       : bounded by EVENT_QUEUE_CAP so a slow
    //                     subscriber cannot accumulate events without
    //                     limit; try_send drops the newest event on
    //                     full and subscribers observe a gap.
    let (commands_tx, commands_rx) = flume::unbounded();
    let (events_tx, events_rx) = flume::bounded(driver_opts.event_queue_cap());
    let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded();
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let events_dropped = Arc::new(AtomicU64::new(0));

    // 4. Initial snapshot (just the local node, no peers yet). The driver
    //    publishes a fresh snapshot immediately on entry to its loop (the
    //    `refresh_snapshot` call before the `select_biased`), so the initial
    //    values are only observable in the narrow window between `new`
    //    returning and the spawned task being polled.
    let local = Node::new(local_id, local_addr);
    let snapshot = Arc::new(ArcSwap::from_pointee(MemberlistSnapshot::new(
      vec![local.clone()],
      local,
      1, // alive_count: the local node is Alive at incarnation 1.
      1, // member_count: only the local node is known.
    )));

    // 5. Spawn the driver task. The driver owns BOTH the gossip socket
    //    and the TCP listener directly so they both drop the instant the
    //    driver loop exits — the listener's port is released before
    //    `Memberlist::shutdown` returns. TLS handshake bytes flow on the
    //    same byte path as application data; `TlsRecords` inside
    //    `StreamEndpoint::handle_transport_data` decodes them.
    //    `socket_to_peer` is the identity for this transport (membership
    //    address IS the `SocketAddr`).
    let socket_to_peer: Arc<dyn Fn(SocketAddr) -> SocketAddr + Send + Sync> =
      Arc::new(|addr: SocketAddr| addr);
    let driver_handle = compio::runtime::spawn(driver_loop::<SmolStr, SocketAddr, TlsRecords>(
      endpoint,
      gossip_socket,
      listener,
      commands_rx,
      events_tx,
      events_dropped.clone(),
      snapshot.clone(),
      bridge_ready_rx,
      bridge_ready_tx,
      shutdown_flag.clone(),
      driver_opts,
      socket_to_peer,
    ));

    // 6. Build the handle from the wired parts.
    Ok(Self::from_parts(
      commands_tx,
      snapshot,
      events_rx,
      events_dropped,
      driver_handle,
      shutdown_flag,
      driver_opts.join_deadline(),
    ))
  }
}
