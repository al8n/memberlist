//! QUIC-backed memberlist driver — public seam.
//!
//! [`QuicMemberlist::new`] pins the
//! generic [`crate::Memberlist`] handle to the concrete `I = SmolStr`,
//! `A = SocketAddr`, `R = Quic` instantiation, binds the UDP socket,
//! constructs the composed `QuicEndpoint` super-state-machine,
//! creates the command / event channels, publishes an initial
//! snapshot, and spawns the QUIC driver task on the compio runtime.
//! The returned handle is cheaply clonable and shares the same driver
//! task with every clone.
//!
//! ## Address bridge
//!
//! QUIC's TLS 1.3 handshake requires a server name to verify the
//! peer's certificate against. The supplied [`IdentityBridge`]
//! returns `Some("localhost")` so the default rustls verifier
//! accepts the self-signed localhost-SAN cert used in test setups.
//! For production deployments where `A = SocketAddr` maps to a real
//! hostname, implement a custom
//! [`memberlist_machine::AddrBridge`] that returns the peer's actual
//! SAN/CN. A mismatch causes the TLS handshake to fail and the join
//! reaps with `MemberlistError::JoinAllFailed`.

#![cfg(feature = "quic")]

use core::marker::PhantomData;
use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
  },
};

use arc_swap::ArcSwap;
use compio::net::UdpSocket;
use memberlist_machine::{AddrBridge, QuicEndpoint, config::EndpointConfig, endpoint::Endpoint};
use memberlist_wire::Node;
use smol_str::SmolStr;

use crate::{
  Memberlist, MemberlistError, MemberlistSnapshot, QuicDriverOptions, Result,
  quic_driver::driver_loop,
};

/// Phantom type tag identifying the QUIC backend.
///
/// `Quic` does not implement
/// [`memberlist_machine::streams::StreamTransport`] — QUIC carries no
/// stream-transport record layer; its security is intrinsic to the
/// QUIC handshake. The tag exists only so
/// `QuicMemberlist = Memberlist<SmolStr, SocketAddr, Quic>` is a
/// type distinct from `TcpMemberlist` and `TlsMemberlist`, permitting
/// `impl QuicMemberlist { … }` without conflicting impls.
pub struct Quic;

/// Identity [`AddrBridge`] for `A = SocketAddr` with TLS server-name
/// routing for the QUIC handshake.
///
/// `to_socket` / `from_socket` pass `SocketAddr` through verbatim.
/// `server_name` returns `Some("localhost")` to match the self-signed
/// localhost-SAN certs used in test setups; production callers
/// implement a custom bridge that returns the peer's actual SAN/CN.
///
/// Unit struct (no fields) — every method on [`AddrBridge`] is
/// static, so the bridge is a zero-sized type parameter on
/// `QuicEndpoint`.
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
    Some("localhost")
  }
}

/// QUIC config bundle handed to [`QuicMemberlist::new`]. Re-exported
/// from `memberlist-machine` so callers don't need a direct dep.
pub use memberlist_machine::QuicConfig;

/// QUIC-backed [`Memberlist`] alias — pins `I = SmolStr`,
/// `A = SocketAddr`, `R = Quic`.
pub type QuicMemberlist = Memberlist<SmolStr, SocketAddr, Quic>;

impl QuicMemberlist {
  /// Construct a QUIC memberlist, bind the UDP socket, and spawn
  /// the QUIC driver task on the current compio runtime.
  ///
  /// `config` carries the membership-level settings (local id,
  /// advertise address, SWIM timing, gossip MTU, etc.);
  /// `quic_config` carries the caller-built `quinn_proto`
  /// `EndpointConfig` / `ServerConfig` / `ClientConfig` /
  /// `TransportConfig` bundle that governs the QUIC handshake on
  /// the reliable path.
  ///
  /// On return the driver task is already running. Reads
  /// (`snapshot`, `local_node`, `alive_count`, `member_count`) are
  /// served lock-free from the initial snapshot until the driver
  /// publishes its first refresh.
  ///
  /// # Errors
  ///
  /// Returns [`MemberlistError::Io`] if binding the UDP socket fails
  /// (most commonly `EADDRINUSE` on a port collision).
  pub async fn new(
    config: EndpointConfig<SmolStr, SocketAddr>,
    quic_config: QuicConfig,
  ) -> Result<Self> {
    Self::new_with_options(config, quic_config, QuicDriverOptions::default()).await
  }

  /// Construct a QUIC-backed memberlist with explicit per-driver
  /// tuning knobs. Most callers want [`Self::new`] (defaults).
  ///
  /// See [`Self::new`] for the full error surface.
  pub async fn new_with_options(
    config: EndpointConfig<SmolStr, SocketAddr>,
    quic_config: QuicConfig,
    driver_opts: QuicDriverOptions,
  ) -> Result<Self> {
    // 1. Bind the UDP socket on the configured advertise address.
    //    QUIC multiplexes both handshake and reliable-stream
    //    traffic over this single UDP socket; the memberlist
    //    unreliable path shares the same socket via the inbound
    //    first-byte demux inside `QuicEndpoint`.
    let local_addr: SocketAddr = *config.advertise_addr_ref();
    let local_id: SmolStr = config.local_id_ref().clone();
    let udp_socket = UdpSocket::bind(local_addr)
      .await
      .map_err(MemberlistError::Io)?;

    // 2. Build the composed super-state-machine. `Endpoint::new`
    //    inserts the local node as Alive at incarnation 1;
    //    `QuicEndpoint` wraps that with the caller-supplied
    //    `QuicConfig`.
    let endpoint: QuicEndpoint<SmolStr, SocketAddr, IdentityBridge> =
      QuicEndpoint::new(Endpoint::new(config), quic_config);

    // 3. Channels (mirroring the stream-transport adapter's
    //    bounding rationale):
    //    - commands : unbounded (every command acks via one-shot).
    //    - events   : bounded by `event_queue_cap` so a slow
    //                 subscriber cannot accumulate events without
    //                 limit; try_send drops the newest event on
    //                 full and subscribers observe a gap.
    let (commands_tx, commands_rx) = flume::unbounded();
    let (events_tx, events_rx) = flume::bounded(driver_opts.event_queue_cap());
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let events_dropped = Arc::new(AtomicU64::new(0));

    // 4. Initial snapshot (just the local node, no peers yet). The
    //    driver publishes a fresh snapshot on entry to its loop, so
    //    the initial values are only observable in the narrow
    //    window between `new` returning and the spawned task being
    //    polled.
    let local = Node::new(local_id, local_addr);
    let snapshot = Arc::new(ArcSwap::from_pointee(MemberlistSnapshot::new(
      vec![local.clone()],
      local,
      1, // alive_count
      1, // member_count
    )));

    // 5. Spawn the driver task. The driver owns the UDP socket
    //    directly so it drops the instant the driver loop exits and
    //    the bound port is released before `Memberlist::shutdown`
    //    returns. `PhantomData<fn(IdentityBridge)>` fixes the
    //    address-bridge type parameter at the spawn site.
    let driver_handle = compio::runtime::spawn(driver_loop::<SmolStr, SocketAddr, IdentityBridge>(
      endpoint,
      udp_socket,
      commands_rx,
      events_tx,
      events_dropped.clone(),
      snapshot.clone(),
      shutdown_flag.clone(),
      driver_opts,
      PhantomData,
    ));

    // 6. Build the handle from the wired parts. The handle caches
    //    the join deadline scalar (the only `DriverOptions` field
    //    `join_with` reads on the hot-path); the full
    //    `QuicDriverOptions` value moved into the driver task above.
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
