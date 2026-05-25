//! TCP-backed memberlist driver — the first end-to-end usable surface.
//!
//! [`TcpMemberlist::new`] pins the generic [`crate::Memberlist`] handle to
//! the concrete `I = SmolStr`, `A = SocketAddr`, `R = RawRecords`
//! instantiation, binds the UDP gossip socket, constructs the composed
//! [`StreamEndpoint`] super-state-machine, creates the command / event
//! channels, publishes an initial snapshot, and spawns the driver task on
//! the compio runtime. The returned handle is cheaply clonable and shares
//! the same driver task with every clone.
//!
//! ## Address bridge
//!
//! Plain TCP needs no per-dial verification identity (no certificate
//! chain to verify against a hostname), so [`IdentityBridge`] passes
//! `SocketAddr` through verbatim and returns `None` from `server_name`.
//! This is symmetric with `streams::test_support::IdentityBridge` (the
//! sim harness identity bridge for `A = SocketAddr`) modulo the
//! `server_name`: the sim bridge returns `Some("localhost")` to match
//! the TLS localhost-SAN test certs; here we return `None` because the
//! plain-TCP `RawRecords::dial_context` does not consult it
//! (`memberlist-machine/src/tcp/mod.rs`).

#![cfg(feature = "tcp")]

use std::{
  marker::PhantomData,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
  },
};

use arc_swap::ArcSwap;
use compio::net::{TcpListener, UdpSocket};
use memberlist_machine::{
  AddrBridge, RawRecords, TcpOptions, config::EndpointConfig, endpoint::Endpoint,
  streams::StreamEndpoint,
};
use memberlist_wire::Node;
use smol_str::SmolStr;

use crate::{
  DriverOptions, Memberlist, MemberlistError, MemberlistSnapshot, Result, driver::driver_loop,
};

/// Identity [`AddrBridge`] for `A = SocketAddr`.
///
/// Translation is the identity (`to_socket` returns the address verbatim,
/// `from_socket` returns the socket verbatim) and there is no verification
/// identity to supply (`server_name` returns `None`): the plain-TCP record
/// layer never consults `server_name` (see
/// `memberlist-machine::tcp::RawRecords::dial_context`, which returns
/// `Ok(())` unconditionally), so a `None` is correct rather than a
/// hostname placeholder.
///
/// Unit struct (no fields) — every method on [`AddrBridge`] is static, so
/// the bridge is a zero-sized type parameter on [`StreamEndpoint`].
pub struct IdentityBridge;

impl AddrBridge<SocketAddr> for IdentityBridge {
  type ServerName = str;

  fn to_socket(addr: &SocketAddr) -> SocketAddr {
    *addr
  }

  fn from_socket(socket: SocketAddr) -> SocketAddr {
    socket
  }

  fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
    None
  }
}

/// TCP-backed [`Memberlist`] alias — pins `I = SmolStr`, `A = SocketAddr`,
/// `R = RawRecords`.
pub type TcpMemberlist = Memberlist<SmolStr, SocketAddr, RawRecords>;

impl TcpMemberlist {
  /// Construct a TCP memberlist, bind the UDP gossip socket, and spawn the
  /// driver task on the current compio runtime.
  ///
  /// `config` carries the membership-level settings (local id, advertise
  /// address, SWIM timing, gossip MTU, etc.); `tcp_opts` carries the
  /// plain-TCP record-layer options (cluster label + inbound-label-check
  /// policy). The UDP gossip socket and the TCP reliable listener both
  /// bind to `config.advertise_addr_ref()`; outbound TCP exchanges are
  /// dialed on demand from the driver loop's `StreamAction::Connect` arm.
  ///
  /// On return the driver and listener tasks are already running. Reads
  /// (`snapshot`, `local_node`, `alive_count`, `member_count`) are served
  /// lock-free from the initial snapshot until the driver publishes its
  /// first refresh on entry into the loop.
  ///
  /// # Errors
  ///
  /// Returns [`MemberlistError::Io`] if binding the UDP gossip socket or
  /// the TCP reliable listener fails (most commonly `EADDRINUSE` on a port
  /// collision).
  pub async fn new(
    config: EndpointConfig<SmolStr, SocketAddr>,
    tcp_opts: TcpOptions,
  ) -> Result<Self> {
    Self::new_with_options(config, tcp_opts, DriverOptions::default()).await
  }

  /// Construct a TCP-backed memberlist with explicit per-driver tuning
  /// knobs. Most callers want [`Self::new`] (defaults); reach for this
  /// when a specific knob — `join_deadline`, `iter_drain_cap`,
  /// `event_queue_cap`, etc. — must deviate from
  /// [`DriverOptions::default()`].
  ///
  /// See [`Self::new`] for the full error surface.
  pub async fn new_with_options(
    config: EndpointConfig<SmolStr, SocketAddr>,
    tcp_opts: TcpOptions,
    driver_opts: DriverOptions,
  ) -> Result<Self> {
    // 1. Bind UDP gossip socket + TCP reliable listener on the configured
    //    advertise address. Both share the same socket-address tuple — UDP
    //    and TCP are independent layer-4 protocols and may coexist on the
    //    same port.
    let local_addr: SocketAddr = *config.advertise_addr_ref();
    let local_id: SmolStr = config.local_id_ref().clone();
    let gossip_socket = UdpSocket::bind(local_addr)
      .await
      .map_err(MemberlistError::Io)?;
    let listener = TcpListener::bind(local_addr)
      .await
      .map_err(MemberlistError::Io)?;

    // 2. Build the composed super-state-machine. The membership
    //    `Endpoint::new` runs `config.clone()`-equivalent work
    //    (`local_id_ref().cheap_clone()`) and inserts the local node as
    //    Alive at incarnation 1; the `StreamEndpoint` wraps that with the
    //    `RawRecords` record layer plug and the supplied `TcpOptions`.
    let endpoint: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(Endpoint::new(config), tcp_opts);

    // 3. Channels:
    //    - commands  (handle → driver): unbounded; a slow driver still
    //      acks every command via its one-shot reply, so a bounded
    //      channel could back-pressure user calls under load.
    //    - bridge_ready (listener/dial → driver): unbounded; producers
    //      are driver-internal and cannot run hot enough to OOM.
    //    - events    (driver → subscribers): BOUNDED at
    //      `EVENT_QUEUE_CAP` so a slow / dropped subscriber cannot
    //      accumulate events without limit. `try_send` on a full
    //      channel drops the newest event; subscribers observe a
    //      gap, which is the standard contract for bounded
    //      broadcast-style streams.
    //    The events channel is MPMC (flume) so multiple `EventStream`s
    //    round-robin per the documented `events()` contract.
    let (commands_tx, commands_rx) = flume::unbounded();
    let (events_tx, events_rx) = flume::bounded(driver_opts.event_queue_cap());
    let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded();
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let events_dropped = Arc::new(AtomicU64::new(0));

    // 4. Initial snapshot (just the local node, no peers yet). The driver
    //    publishes a fresh snapshot immediately on entry to its loop (the
    //    `refresh_snapshot` call before the `select_biased`), so the
    //    initial values are only observable in the narrow window between
    //    `new` returning and the spawned task being polled.
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
    //    `Memberlist::shutdown` returns and an immediate rebind on the
    //    same address succeeds. The `PhantomData<fn(B)>` argument fixes
    //    the address-bridge type parameter at the spawn site, since
    //    the driver_loop's generic `B` is otherwise unconstrained by
    //    its other arguments. The driver keeps its own
    //    `bridge_ready_tx` clone so it can spawn outbound dial tasks
    //    that report back.
    let driver_handle =
      compio::runtime::spawn(
        driver_loop::<SmolStr, SocketAddr, IdentityBridge, RawRecords>(
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
          PhantomData,
        ),
      );

    // 6. Build the handle from the wired parts.
    Ok(Self::from_parts(
      commands_tx,
      snapshot,
      events_rx,
      events_dropped,
      driver_handle,
      shutdown_flag,
      driver_opts,
    ))
  }
}
