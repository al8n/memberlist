//! Memberlist handle: construction, accessors, and the caller-owned poll loop.

use core::net::SocketAddr;

// Under `no_std + alloc` the prelude does not bring `Box` / `VecDeque` into
// scope; import them explicitly from the aliased `std` (which is `alloc` in
// that build).
#[cfg(feature = "std")]
use std::collections::VecDeque;
#[cfg(not(feature = "std"))]
use std::{boxed::Box, collections::VecDeque};

use memberlist_proto::{
  AliveDelegate, Endpoint, EndpointConfig, Instant, Node, PushPullKind, RawRecords, StreamId,
  event::{PingId, Transmit},
  streams::{ExchangeId, StreamAction, StreamEndpoint},
  typed::{Alive, NodeState, State},
};
use smoltcp::{
  iface::{Config as IfConfig, Interface, SocketHandle, SocketSet},
  phy::Device,
  socket::{tcp, udp},
};

use crate::{
  Config, InitError, InterfaceConfig, TransformOptions,
  addr::{from_endpoint, from_smoltcp_instant, to_endpoint, to_smoltcp_instant},
  error::{GossipMtuTooLarge, MediumMismatch},
  interface::{HardwareAddress, Medium},
};

/// The maximum UDP payload (`u16` length minus the 8-byte UDP header), the
/// hard ceiling for an on-wire gossip datagram. Matches the async drivers.
const UDP_PAYLOAD_MAX: usize = 65507;

/// The largest per-socket TCP receive-buffer smoltcp accepts: 1 GiB.
///
/// smoltcp's `tcp::Socket::new` `panic!`s when the receive-buffer capacity
/// exceeds this (`if rx_capacity > (1 << 30)`, socket/tcp.rs), derived from the
/// RFC 1323 window-scale ceiling of 2^30. A caller-supplied
/// [`Config::tcp_socket_rx_bytes`](crate::Config::tcp_socket_rx_bytes) past it
/// would panic inside the fallible constructor, so `try_new` rejects it first.
/// The transmit buffer has no such limit and is not capped.
const TCP_RX_BUFFER_MAX: usize = 1 << 30;

/// Size the inbound-gossip receive scratch from the effective gossip MTU.
///
/// The machine caps an outbound gossip datagram's PLAINTEXT at the configured
/// [`EndpointConfig::gossip_mtu`]; the on-wire datagram can then exceed that by
/// up to [`memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD`] (30 B of wrapper
/// header, nonce, and AEAD tag) when encryption is enabled. The buffer must
/// hold the largest such on-wire datagram, so it is sized to
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`, floored at 1500 (the common
/// Ethernet payload) so the default ([`memberlist_proto::DEFAULT_GOSSIP_MTU`],
/// 1400) keeps a little headroom and any sub-1500 MTU never under-sizes it.
///
/// smoltcp's `udp::Socket::recv_slice` POPS the datagram before checking the
/// caller's slice length, so a datagram larger than this buffer is consumed and
/// lost (returned as `RecvError::Truncated`). Sizing the buffer from the same
/// knob the machine uses to bound outbound gossip means a correctly-configured
/// cluster never truncates an in-budget datagram.
fn gossip_recv_buf_size(gossip_mtu: usize) -> usize {
  (gossip_mtu + memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD).max(1500)
}

/// Whether `addr` is a destination the smoltcp stack can actually use.
///
/// The machine is address-agnostic (`A = SocketAddr`): a peer can gossip an
/// `Alive` advertising a non-routable address (the unspecified address, a
/// multicast/broadcast IP, or port 0), the machine stores it as a member, and a
/// later gossip / probe / push-pull selects it as a DESTINATION. Two things then
/// go wrong if the driver does not screen it:
///
/// - smoltcp's socket layer rejects an unspecified destination or port 0 outright
///   (`udp::Socket::send_slice` / `tcp::Socket::connect` return `Unaddressable`),
///   so every gossip datagram to such an address is silently dropped and every
///   dial to it fails — wasted exchanges and churn rather than a clean drop.
/// - smoltcp's route table (`iface::route::lookup`) and neighbor cache
///   (`iface::neighbor::lookup`) both `assert!(addr.is_unicast())` on the resolved
///   egress address — release-mode assertions. The socket-layer rejection above
///   and smoltcp's multicast/broadcast hardware-address derivation keep the
///   current (0.13) version from driving the listed addresses into those
///   assertions, but they are the contract smoltcp documents for a routable
///   address, so screening to the SAME predicate keeps the driver correct if a
///   future smoltcp routes a non-unicast destination differently.
///
/// This is the single validity predicate applied at every boundary that can feed
/// such an address toward smoltcp. `to_endpoint(*addr).addr.is_unicast()` calls
/// smoltcp's OWN `IpAddress::is_unicast` — the exact function those assertions
/// use — so the driver and smoltcp agree byte-for-byte on what "routable" means.
/// The `port() != 0` term additionally rejects port 0, which no socket can
/// connect to or send a meaningful datagram to.
pub(crate) fn endpoint_is_routable(addr: &SocketAddr) -> bool {
  to_endpoint(*addr).addr.is_unicast() && addr.port() != 0
}

/// An [`AliveDelegate`] that admits a peer only when its advertised address is a
/// routable destination ([`endpoint_is_routable`]).
///
/// The machine calls `notify_alive` inline for EVERY admitted Alive — gossip and
/// join push/pull alike (`Endpoint::process_alive`) — so this one filter drops a
/// non-routable address at admission on both planes. The bad address is never
/// stored as a member and so is never re-gossiped, stopping cluster-wide
/// propagation of a member address that no node could ever send a useful packet
/// to. It is the propagation-prevention layer; the egress chokepoints remain the
/// last-line drop for any address that reaches the driver by any other path.
struct RoutableAddrFilter;

impl<I> AliveDelegate<I, SocketAddr> for RoutableAddrFilter
where
  I: memberlist_proto::Id,
{
  fn notify_alive(&self, peer: &NodeState<I, SocketAddr>) -> bool {
    endpoint_is_routable(peer.address_ref())
  }
}

/// Derive the medium a [`HardwareAddress`] selects, or `None` for a medium this
/// driver does not support.
///
/// smoltcp's own `HardwareAddress::medium` is `pub(crate)`, so the driver
/// matches the variant itself to validate the configured medium against the
/// bound device before `Interface::new` (whose internal assertion would
/// otherwise panic on a mismatch). This crate enables only smoltcp's
/// `medium-ip` and `medium-ethernet`, so those are the variants that map to a
/// `Some` medium; any other variant — `Ieee802154`, which exists only when a
/// downstream crate unifies smoltcp's `medium-ieee802154` feature on — returns
/// `None` so the caller can surface a typed [`InitError::UnsupportedMedium`]
/// instead of reaching an `unreachable!()`.
fn hardware_address_medium(addr: &HardwareAddress) -> Option<Medium> {
  match addr {
    HardwareAddress::Ip => Some(Medium::Ip),
    HardwareAddress::Ethernet(_) => Some(Medium::Ethernet),
    #[allow(unreachable_patterns)]
    _ => None,
  }
}

/// An executor-free memberlist node that composes the memberlist SWIM machine
/// with a smoltcp TCP/IP stack.
///
/// The caller owns the device (`D`) and drives the node by calling `poll` in a
/// super-loop. Construction (`new`) binds the gossip UDP socket and wires up
/// the `StreamEndpoint`; no I/O occurs here.
///
/// `I` is the node identifier type (e.g. `SmolStr`). `D` is the smoltcp
/// [`Device`] (e.g. `smoltcp::phy::Loopback` for tests, an ethernet driver in
/// production). `A` is pinned to `core::net::SocketAddr`.
pub struct Memberlist<I, D: Device>
where
  I: memberlist_proto::Id,
{
  iface: Interface,
  /// The seed handed to smoltcp's interface RNG at construction (TCP ISN /
  /// ephemeral port selection). Retained because smoltcp does not expose it;
  /// surfaced via [`Memberlist::interface_random_seed`] so a test can witness
  /// that an unpinned seed was a nonzero entropy draw and a pinned seed was
  /// applied verbatim.
  iface_random_seed: u64,
  sockets: SocketSet<'static>,
  /// Handle into `sockets` for the gossip UDP socket.
  udp: SocketHandle,
  endpoint: StreamEndpoint<I, SocketAddr, RawRecords>,
  /// Driver sizing configuration; retained for the reliable-plane paths.
  cfg: Config,
  /// Pooled TCP sockets and the exchange-to-socket map for the reliable plane.
  plane: crate::reliable::ReliablePlane,
  /// Heap scratch for one inbound gossip datagram, sized once at construction
  /// from the configured gossip MTU (see [`gossip_recv_buf_size`]) and reused
  /// every poll. Heap-resident (not a per-poll stack array) so a large MTU does
  /// not blow a constrained stack and the allocation happens exactly once.
  gossip_recv: std::vec::Vec<u8>,
  /// Seed addresses queued by `join` that have not yet been handed to the
  /// machine. Drained in the machine-pump phase of each `poll` tick: one
  /// `start_push_pull(seed, Join, now)` per entry, which queues a
  /// `DialRequested` the machine immediately services into a `Connect` action
  /// consumed later that same tick. Keeping the queue on the driver (rather
  /// than the reliable plane) because join intent is a driver-level policy —
  /// the machine drives the actual exchange state, while this queue records
  /// which seeds are still waiting for a first contact attempt.
  pending_seeds: VecDeque<SocketAddr>,
  // `D` is not stored — it is passed in at construction time and then to
  // each `poll` call. `PhantomData` is required so the struct is generic
  // over `D` without actually holding it.
  _device: core::marker::PhantomData<D>,
}

impl<I, D: Device> Memberlist<I, D>
where
  I: memberlist_proto::Id,
{
  /// Construct a node, panicking on a misconfiguration or entropy failure.
  ///
  /// This is the convenience wrapper over [`try_new`](Self::try_new); it has the
  /// same parameters and behaviour but unwraps the result. Use it only when the
  /// [`InterfaceConfig`] is a static constant known to be valid and the build
  /// targets a host whose entropy source cannot fail.
  ///
  /// # Panics
  ///
  /// Panics if [`try_new`](Self::try_new) returns an [`InitError`] — e.g. on an
  /// unsupported or mismatched medium, a non-unicast hardware or IP address, a
  /// missing/over-capacity IP address or route, an entropy failure, or a
  /// machine-endpoint init failure. Call [`try_new`](Self::try_new) to handle
  /// those.
  pub fn new(
    cfg: Config,
    iface: InterfaceConfig,
    transform: TransformOptions,
    ep_cfg: EndpointConfig<I, SocketAddr>,
    device: &mut D,
    now: Instant,
  ) -> Self {
    Self::try_new(cfg, iface, transform, ep_cfg, device, now).expect(
      "Memberlist::new: invalid interface configuration or entropy failure; use try_new to handle",
    )
  }

  /// Fallibly construct a node.
  ///
  /// Builds the smoltcp `Interface` from `iface`, allocates the gossip UDP
  /// socket, and wires up the `StreamEndpoint` over the machine's `Endpoint`.
  /// No I/O occurs here.
  ///
  /// # Parameters
  ///
  /// - `cfg`: driver sizing / port configuration.
  /// - `iface`: the smoltcp interface configuration — hardware address (which
  ///   selects the medium), IP addresses, routes, and RNG seed. The hardware
  ///   address must select a supported medium (IP or Ethernet) and, if Ethernet,
  ///   be unicast; every IP address must be unicast or unspecified. All of these
  ///   are validated (see below) rather than panicking inside smoltcp.
  /// - `transform`: cross-transport gossip + reliable-plane compression and
  ///   encryption, plus the reliable-plane (TCP) cluster label. A configured
  ///   encryption keyring is probed here (see Errors); the default is fully
  ///   disabled and unlabelled.
  /// - `ep_cfg`: machine identity (`id`, `advertise`, timing knobs, …).
  /// - `device`: the smoltcp [`Device`] the interface is bound to. Its medium
  ///   must match the one implied by `iface.hardware_addr`.
  /// - `now`: the driver's clock reading at construction (passed to the
  ///   `Interface` and the `Endpoint` so timers start from a consistent
  ///   origin; see `addr::to_smoltcp_instant` / `Endpoint::try_new_at`).
  ///
  /// # Errors
  ///
  /// Returns [`InitError`] instead of panicking when the configuration is
  /// invalid for the bound device:
  ///
  /// - [`InitError::UnsupportedMedium`] — `iface.hardware_addr` selects a medium
  ///   this driver does not support (smoltcp's `Ieee802154`), so its medium
  ///   cannot be derived (checked here so the derivation never hits an
  ///   `unreachable!()`).
  /// - [`InitError::MediumMismatch`] — `iface.hardware_addr`'s medium differs
  ///   from `device.capabilities().medium` (checked here so smoltcp's internal
  ///   assertion can never fire).
  /// - [`InitError::NonUnicastHardwareAddress`] — `iface.hardware_addr` is an
  ///   Ethernet MAC that is not unicast (checked here because smoltcp's
  ///   `Interface::new` stores it without validating it).
  /// - [`InitError::NonUnicastIpAddress`] — an address in `iface.ip_addrs` is
  ///   neither unicast nor unspecified (checked here so smoltcp's
  ///   `check_ip_addrs` can never `panic!`).
  /// - [`InitError::MissingIpAddress`] — `iface.ip_addrs` is empty.
  /// - [`InitError::TooManyIpAddresses`] / [`InitError::TooManyRoutes`] — more
  ///   addresses or routes than smoltcp's interface can hold.
  /// - [`InitError::Entropy`] — `iface.random_seed` was `None` and the system
  ///   entropy source failed.
  /// - [`InitError::Endpoint`] — the machine endpoint failed to initialize.
  /// - [`InitError::Encryption`] — `transform.encryption` carries a keyring with
  ///   a key whose AEAD backend was not compiled into this binary (probed by
  ///   encrypting an empty frame with each key), so encrypted gossip would
  ///   otherwise be silently dropped at runtime.
  pub fn try_new(
    cfg: Config,
    iface: InterfaceConfig,
    transform: TransformOptions,
    ep_cfg: EndpointConfig<I, SocketAddr>,
    device: &mut D,
    now: Instant,
  ) -> Result<Self, InitError> {
    // 1. Validate the medium up front: smoltcp's `Interface::new` asserts the
    //    hardware address's medium equals the device's, and a mismatch panics
    //    deep inside it. Derive the configured medium ourselves (smoltcp's
    //    `HardwareAddress::medium` is `pub(crate)`); a hardware address whose
    //    medium this driver does not support (Ieee802154) yields `None` and is
    //    rejected here rather than reaching an `unreachable!()`. A medium that is
    //    supported but differs from the device's is a `MediumMismatch`.
    let expected =
      hardware_address_medium(&iface.hardware_addr).ok_or(InitError::UnsupportedMedium)?;
    let actual = device.capabilities().medium;
    if expected != actual {
      return Err(InitError::MediumMismatch(MediumMismatch {
        expected,
        actual,
      }));
    }

    // 2. Validate the Ethernet hardware address is unicast. smoltcp's
    //    `Interface::new` stores the configured hardware address WITHOUT calling
    //    `check_hardware_addr` (that runs only on the `set_hardware_addr` path the
    //    driver never uses), so a broadcast/multicast MAC would not panic but
    //    would install an invalid L2 source/acceptance identity. Match the
    //    `Ethernet` variant directly and test the inner MAC with
    //    `EthernetAddress::is_unicast` (`!(is_broadcast() || is_multicast())`) —
    //    NOT `HardwareAddress::is_unicast`, which is `unreachable!()` for the `Ip`
    //    variant and would itself panic. The `Ip` variant has no L2 address and
    //    is always acceptable.
    if let HardwareAddress::Ethernet(mac) = &iface.hardware_addr {
      if !mac.is_unicast() {
        return Err(InitError::NonUnicastHardwareAddress(iface.hardware_addr));
      }
    }

    // 3. An interface with no address silently drops every packet, so reject it.
    if iface.ip_addrs.is_empty() {
      return Err(InitError::MissingIpAddress);
    }

    // 4. Validate every configured IP address is acceptable to smoltcp. Its
    //    `update_ip_addrs` calls `check_ip_addrs`, which `panic!`s on any address
    //    that is neither unicast nor unspecified. Mirror that EXACT condition here
    //    (permitting the unspecified address, which smoltcp itself permits) so a
    //    multicast/broadcast CIDR is a typed error instead of a panic. Done before
    //    `update_ip_addrs` (step 7) so the capacity-overflow check there still runs
    //    afterward over already-unicast-validated addresses.
    for cidr in &iface.ip_addrs {
      if !cidr.address().is_unicast() && !cidr.address().is_unspecified() {
        return Err(InitError::NonUnicastIpAddress(*cidr));
      }
    }

    // Validate every configured route's gateway. smoltcp resolves an off-link
    // next hop through its neighbor cache on Ethernet egress, and that lookup
    // asserts the protocol address is unicast (a release `assert!`); a route
    // whose `via_router` family differs from its prefix can never resolve a next
    // hop. Both would surface only at first egress — a panic / dead route on an
    // already-constructed node — so reject a malformed route here as a typed
    // construction error instead.
    for route in &iface.routes {
      if !route.via_router.is_unicast() {
        return Err(InitError::NonUnicastRouteGateway(*route));
      }
      if route.cidr.address().version() != route.via_router.version() {
        return Err(InitError::RouteFamilyMismatch(*route));
      }
    }

    // Reject a zero port up front. smoltcp's `udp::Socket::bind` and
    // `tcp::Socket::listen` reject port 0 (`Unaddressable`); the bind/listen
    // calls below would otherwise panic inside this fallible constructor on a
    // runtime-supplied zero port. Validate before allocating any sockets.
    if cfg.port == 0 {
      return Err(InitError::ZeroPort);
    }

    // Reject a gossip MTU whose on-wire datagram cannot fit a UDP packet. The
    // UDP arenas and the receive scratch are sized from
    // `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD` (the largest on-wire datagram the
    // machine can emit); an over-ceiling `gossip_mtu` would overflow that
    // addition — a panic in a checked build, a wrap to an undersized arena in
    // release that then silently truncates in-budget gossip. Bounding it here
    // makes every downstream `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD` safe and
    // mirrors the async drivers' reject-not-clamp doctrine. Done before any UDP
    // allocation.
    let gossip_mtu_ceiling = UDP_PAYLOAD_MAX - memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD;
    if ep_cfg.gossip_mtu() > gossip_mtu_ceiling {
      return Err(InitError::GossipMtuTooLarge(GossipMtuTooLarge {
        gossip_mtu: ep_cfg.gossip_mtu(),
        ceiling: gossip_mtu_ceiling,
      }));
    }

    // Reject a sub-2 TCP pool. Construction dedicates one pooled socket to the
    // listener and uses the rest for dials/accepts: 0 sockets is no listener and
    // no reliable plane at all, and 1 leaves the listener holding the only socket
    // with none free to dial — so the node could never dial a seed to join. The
    // functional minimum is a listener plus one dial/accept socket. Checked
    // before allocating the pool below.
    if cfg.tcp_pool_size < 2 {
      return Err(InitError::TcpPoolTooSmall);
    }

    // Reject a zero-length TCP socket buffer. smoltcp's `RingBuffer::new` does
    // not panic on empty storage — it builds a permanently-empty ring — so a
    // 0-byte rx (or tx) buffer would yield a socket that can never receive (or
    // send): a silently-dead reliable plane rather than a construction error.
    // Checked before allocating the per-socket rings below.
    if cfg.tcp_socket_rx_bytes == 0 || cfg.tcp_socket_tx_bytes == 0 {
      return Err(InitError::ZeroTcpSocketBuffer);
    }

    // Reject a TCP receive buffer over smoltcp's 1 GiB cap. smoltcp's
    // `tcp::Socket::new` `panic!`s when the receive-buffer capacity exceeds
    // `TCP_RX_BUFFER_MAX` (`> 1 << 30`); a larger `tcp_socket_rx_bytes` would
    // panic inside this fallible constructor. The transmit buffer has no such
    // limit, so it is not capped. Checked before allocating the rings below.
    if cfg.tcp_socket_rx_bytes > TCP_RX_BUFFER_MAX {
      return Err(InitError::TcpRxBufferTooLarge);
    }

    // Reject zero UDP packet-metadata slots. The gossip `udp::PacketBuffer` is
    // built with `udp_*_packets` metadata slots; zero slots is a ring that can
    // never enqueue or dequeue a datagram, so gossip could never be received (or
    // sent): a silently-dead gossip plane. Checked before allocating the UDP
    // packet buffers below.
    if cfg.udp_rx_packets == 0 || cfg.udp_tx_packets == 0 {
      return Err(InitError::ZeroUdpPackets);
    }

    // Reject a zero graceful-close timeout. `close_timeout` bounds the reliable
    // graceful-close drain: a connection still `Closing` past `now +
    // close_timeout` is force-aborted. Zero sets that deadline to `now`, so every
    // graceful close is force-aborted immediately — the drain never runs and an
    // in-flight push/pull response is truncated. A pure-`Config` check, so it is
    // validated up front with the others.
    if cfg.close_timeout.is_zero() {
      return Err(InitError::ZeroCloseTimeout);
    }

    // 5. Resolve the interface RNG seed: pinned value, or a fresh system-entropy
    //    draw. A nonzero seed is what keeps smoltcp's TCP ISN and ephemeral port
    //    selection from repeating across reboots.
    let random_seed = match iface.random_seed {
      Some(s) => s,
      None => {
        let mut b = [0u8; 8];
        getrandom::fill(&mut b).map_err(|_| InitError::Entropy)?;
        u64::from_le_bytes(b)
      }
    };

    // 6. Build the interface. The medium is validated (step 1) and the seed is
    //    set, so `Interface::new` cannot panic on the medium assertion. It stores
    //    the hardware address without re-checking it, but steps 1–2 already
    //    rejected any non-unicast Ethernet MAC, so the stored address is a valid
    //    unicast L2 identity (the `Ip` variant carries no L2 address).
    let mut ic = IfConfig::new(iface.hardware_addr);
    ic.random_seed = random_seed;
    let mut iface_obj = Interface::new(ic, device, to_smoltcp_instant(now));

    // 7. Apply the configured IP addresses. Every address was validated unicast or
    //    unspecified in step 4, so `update_ip_addrs`'s internal `check_ip_addrs`
    //    cannot panic. smoltcp's address store is a bounded `heapless::Vec`; a push
    //    past `IFACE_MAX_ADDR_COUNT` returns the item. Capture that overflow out of
    //    the closure and surface it rather than silently dropping addresses.
    let mut overflow = false;
    iface_obj.update_ip_addrs(|addrs| {
      for cidr in &iface.ip_addrs {
        if addrs.push(*cidr).is_err() {
          overflow = true;
          break;
        }
      }
    });
    if overflow {
      return Err(InitError::TooManyIpAddresses);
    }

    // 8. Apply the configured routes. smoltcp's route table is likewise bounded;
    //    `push` past `IFACE_MAX_ROUTE_COUNT` returns the route.
    let mut route_overflow = false;
    iface_obj.routes_mut().update(|table| {
      for route in &iface.routes {
        if table.push(*route).is_err() {
          route_overflow = true;
          break;
        }
      }
    });
    if route_overflow {
      return Err(InitError::TooManyRoutes);
    }

    let iface = iface_obj;

    // Allocate a gossip UDP socket with per-packet metadata rings and a flat
    // payload arena.  `SocketSet::new(Vec::new())` creates an alloc-backed,
    // growable socket storage — smoltcp accepts any `Into<ManagedSlice>`.
    let mut sockets = SocketSet::new(std::vec::Vec::new());

    // Floor each UDP payload arena at "configured datagram slots × max on-wire
    // datagram". The machine caps an outbound gossip datagram's PLAINTEXT at
    // `gossip_mtu`; the on-wire datagram can exceed that by up to
    // `ENCRYPTED_WRAPPER_OVERHEAD` when encryption is enabled. smoltcp's
    // `udp::Socket::send_slice` fails (and silently drops, since gossip is
    // best-effort) when the payload arena cannot hold the datagram, so an arena
    // smaller than `udp_*_packets * max_datagram` could reject in-budget gossip
    // the machine legitimately emitted. Flooring here keeps the arenas in
    // lockstep with the configured gossip MTU — raising `gossip_mtu`
    // auto-scales them — while still honoring a larger explicitly-configured
    // arena. The metadata slot counts (`udp_*_packets`) stay as configured.
    // `gossip_mtu` is bounded above (see the ceiling check), so this addition
    // cannot overflow. The `packets * max_datagram` products still can on a
    // 32-bit target (e.g. `usize::MAX / 65507 ≈ 65541` packet slots), so use
    // `checked_mul` and reject an overflowing arena rather than wrapping to an
    // undersized one.
    let max_datagram = ep_cfg.gossip_mtu() + memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD;
    let udp_rx_arena = cfg.udp_rx_payload_bytes.max(
      cfg
        .udp_rx_packets
        .checked_mul(max_datagram)
        .ok_or(InitError::UdpArenaTooLarge)?,
    );
    let udp_tx_arena = cfg.udp_tx_payload_bytes.max(
      cfg
        .udp_tx_packets
        .checked_mul(max_datagram)
        .ok_or(InitError::UdpArenaTooLarge)?,
    );

    let udp_rx = udp::PacketBuffer::new(
      vec![udp::PacketMetadata::EMPTY; cfg.udp_rx_packets],
      vec![0u8; udp_rx_arena],
    );
    let udp_tx = udp::PacketBuffer::new(
      vec![udp::PacketMetadata::EMPTY; cfg.udp_tx_packets],
      vec![0u8; udp_tx_arena],
    );
    let mut udp_sock = udp::Socket::new(udp_rx, udp_tx);
    // Bind the gossip UDP socket. `bind` fails only on port 0 (rejected above) or
    // an already-open socket (this one is fresh), so it never errors in practice;
    // propagate rather than `expect` so no panic can escape this fallible
    // constructor even if that invariant ever changes.
    udp_sock.bind(cfg.port).map_err(|_| InitError::ZeroPort)?;
    let udp = sockets.add(udp_sock);

    // Allocate pooled TCP sockets for the reliable plane. Each socket gets
    // independent rx/tx ring buffers sized by the config. One socket is
    // immediately moved into listen state; the rest stay free for outbound
    // dials and accepted inbound connections.
    let mut plane = crate::reliable::ReliablePlane::new();
    for _ in 0..cfg.tcp_pool_size {
      let rx = tcp::SocketBuffer::new(vec![0u8; cfg.tcp_socket_rx_bytes]);
      let tx = tcp::SocketBuffer::new(vec![0u8; cfg.tcp_socket_tx_bytes]);
      plane.pool.push(sockets.add(tcp::Socket::new(rx, tx)));
    }
    // Dedicate one pooled socket to listening for inbound reliable connections.
    if let Some(h) = plane.pool.take() {
      // `listen` fails only on port 0 (rejected above) or an already-open
      // socket (this one is fresh); propagate rather than `expect` so no panic
      // can escape this fallible constructor even if that invariant changes.
      sockets
        .get_mut::<tcp::Socket>(h)
        .listen(cfg.port)
        .map_err(|_| InitError::ZeroPort)?;
      plane.listener = Some(h);
    }

    // Size the inbound-gossip scratch from the configured gossip MTU BEFORE
    // `ep_cfg` is moved into `Endpoint::new_at`. The buffer must hold the
    // largest on-wire gossip datagram the machine will emit; reading the knob
    // here keeps the driver's ingress in lockstep with the machine's egress
    // bound (see `gossip_recv_buf_size`).
    let gossip_recv = std::vec![0u8; gossip_recv_buf_size(ep_cfg.gossip_mtu())];

    // Validate the encryption configuration before any endpoint exists, so an
    // unusable keyring is a typed construction error rather than a silent
    // runtime drop of every encrypted gossip datagram. Probe each configured
    // key (primary then secondaries) by encrypting an empty frame: a key whose
    // AEAD backend was not compiled into this binary fails here with
    // `EncryptionError::UnsupportedAlgorithm`.
    if let Some(keyring) = transform.encryption.keyring() {
      for key in core::iter::once(keyring.primary_ref()).chain(keyring.secondaries()) {
        memberlist_proto::encode_encrypted_frame(key.algorithm(), key, b"")
          .map_err(InitError::Encryption)?;
      }
    }

    // Reject a non-routable advertise address before the endpoint exists. A node
    // must advertise an address its peers can route a reply to; an
    // unspecified/multicast/broadcast IP or port 0 would be gossiped cluster-wide
    // and then be useless to every peer that selected it as an egress destination
    // (smoltcp's socket layer rejects it as `Unaddressable` / its route lookup
    // asserts unicast). Fail fast here rather than admit a self-description no
    // peer can use.
    if !endpoint_is_routable(ep_cfg.advertise_addr_ref()) {
      return Err(InitError::NonRoutableAdvertiseAddr(
        *ep_cfg.advertise_addr_ref(),
      ));
    }

    // The machine advertises ONE SocketAddr that peers use for BOTH gossip (UDP)
    // and reliable (TCP) — the single-port memberlist model (one `cfg.port` binds
    // both). The advertised port must match it; otherwise a peer reaches neither
    // plane (its gossip and its push/pull dial both route to a port nothing is
    // listening on) and join/state-sync silently partitions. A direct smoltcp
    // interface has no NAT, so the advertised port is the bound port.
    if ep_cfg.advertise_addr_ref().port() != cfg.port {
      return Err(InitError::AdvertisePortMismatch);
    }

    // The advertised IP must be one the interface actually holds. smoltcp's
    // ingress drops any packet whose destination is not an assigned address, so
    // a node advertising an IP the interface lacks is unreachable on both planes
    // — peers gossip and dial an address its own interface discards. Check with
    // smoltcp's own `has_ip_addr`, the exact predicate that gates ingress in
    // `process_ipv4` / `process_ipv6`.
    let advertised_ip = to_endpoint(*ep_cfg.advertise_addr_ref()).addr;
    if !iface.has_ip_addr(advertised_ip) {
      return Err(InitError::AdvertiseAddrNotLocal(
        *ep_cfg.advertise_addr_ref(),
      ));
    }

    // Wire up the machine's stream endpoint. `peer_to_socket` is identity
    // because `A = SocketAddr`; `sni_provider` returns `None` (no TLS / no SNI).
    // `try_new_at` (not `new_at`) so a machine entropy failure becomes
    // `InitError::Endpoint` rather than a panic. The reliable-plane label and
    // the cross-transport compression/encryption come from `transform`; with a
    // default `TransformOptions` all three are disabled, reproducing the plain
    // no-label endpoint.
    let mut ep = Endpoint::try_new_at(ep_cfg, now).map_err(InitError::Endpoint)?;
    // Install the routable-address admission filter on the raw `Endpoint` BEFORE
    // it is moved into the `StreamEndpoint`. The machine consults it inline for
    // every inbound Alive (gossip AND join push/pull), so a peer advertising a
    // non-routable address is dropped at admission — never stored, never
    // re-gossiped — preventing cluster-wide propagation of an address no node
    // could send a useful packet to.
    ep.set_alive_delegate(RoutableAddrFilter);
    let endpoint = StreamEndpoint::with_compression(
      ep,
      transform.tcp,
      Box::new(|_: &SocketAddr| -> Option<std::string::String> { None }),
      Box::new(|addr: &SocketAddr| *addr),
      transform.compression,
    )
    .with_encryption(transform.encryption);

    Ok(Self {
      iface,
      iface_random_seed: random_seed,
      sockets,
      udp,
      endpoint,
      cfg,
      plane,
      gossip_recv,
      pending_seeds: VecDeque::new(),
      _device: core::marker::PhantomData,
    })
  }

  /// The seed handed to smoltcp's interface RNG at construction.
  ///
  /// A diagnostic for the interface-seed contract: smoltcp seeds its TCP
  /// initial-sequence-number and ephemeral-port RNG from this value but does not
  /// expose it, so a test uses this to witness that an unpinned
  /// [`InterfaceConfig`] drew a nonzero seed from system entropy and a pinned one
  /// was applied verbatim.
  #[doc(hidden)]
  #[inline]
  pub fn interface_random_seed(&self) -> u64 {
    self.iface_random_seed
  }

  /// Number of inbound reliable connections this node has accepted on its TCP
  /// listener since construction.
  ///
  /// A diagnostic for the reliable plane's listener self-healing: after the
  /// socket pool is momentarily exhausted the listener slot can be left empty,
  /// and the driver must re-establish it from a freed socket so subsequent
  /// inbound connections are still accepted. Membership and `poll_event` cannot
  /// witness that invariant — gossip can converge a peer with no TCP accept —
  /// so this counter exposes the accept directly for tests and operators.
  #[doc(hidden)]
  #[inline]
  pub fn accepted_inbound_count(&self) -> u64 {
    self.plane.accepted_inbound
  }

  /// Number of pooled TCP sockets currently free (neither assigned to an active
  /// exchange, the listener, nor parked gracefully closing).
  ///
  /// A diagnostic for the reliable plane's close bounding: a peer that vanishes
  /// mid-FIN would otherwise keep its socket parked in `closing` forever
  /// (smoltcp has no default TCP timeout), permanently shrinking the free-list.
  /// The driver force-aborts a closing socket past `close_timeout` and returns
  /// it here, so a test can witness the free count recover after the timeout.
  #[doc(hidden)]
  #[inline]
  pub fn pool_free_count(&self) -> usize {
    self.plane.pool.free_len()
  }

  /// Number of TCP sockets currently parked mid-close (our FIN sent, the peer's
  /// not yet completed), detached from any connection and awaiting reap.
  ///
  /// A diagnostic for the close-bounding invariant: a socket enters this set when
  /// the machine's `StreamAction::Close` finds it still `is_open()` after the
  /// exchange already half-closed, and leaves it when it reaches `Closed` (the
  /// peer's FIN completed the close) or is force-aborted at `close_timeout`.
  /// Tests use it to witness a socket parked closing before driving past the
  /// timeout.
  #[doc(hidden)]
  #[inline]
  pub fn closing_count(&self) -> usize {
    self.plane.closing.len()
  }

  /// Number of reliable exchanges currently half-closed: their graceful
  /// write-half FIN has been emitted but the exchange is STILL mapped, awaiting
  /// the peer's reply and/or FIN.
  ///
  /// A diagnostic for the half-close lifecycle: `StreamAction::Shutdown` closes
  /// only the local SEND half (the TCP FIN); the exchange stays live so the
  /// peer's later reply + FIN still pump inbound, and the socket is reclaimed
  /// only by the eventual `StreamAction::Close`. An exchange leaves this set when
  /// that `Close` tears it down. Tests use it to witness the moment the local FIN
  /// went out — e.g. to pause the peer and force a delayed reply that must still
  /// be processed across the half-close, or to start a vanished-peer close clock.
  #[doc(hidden)]
  #[inline]
  pub fn half_closed_count(&self) -> usize {
    self.plane.half_closed_count()
  }

  /// Whether a passive-open listener socket is currently installed.
  ///
  /// A diagnostic for the listener-first invariant: an accept consumes the
  /// listener into the new exchange, and the same `poll` must replenish it from
  /// the pool BEFORE any deferred outbound dial can take that socket. A test uses
  /// this to witness that an accept-ready inbound did not leave the node without
  /// a listener because a pending dial stole the only free socket first.
  #[doc(hidden)]
  #[inline]
  pub fn listener_present(&self) -> bool {
    self.plane.listener.is_some()
  }

  /// Number of reliable exchanges still in `PendingDial`: a dial was requested
  /// but the pool was exhausted, so no socket is assigned yet.
  ///
  /// A diagnostic for the listener-first invariant: when exactly one socket is
  /// free and an inbound is accept-ready, the listener claims that socket and any
  /// pending dial must keep waiting. A test uses this to witness that the dial
  /// did NOT steal the listener's socket (it is still counted here after the
  /// contended poll).
  #[doc(hidden)]
  #[inline]
  pub fn pending_dial_count(&self) -> usize {
    self
      .plane
      .connections
      .values()
      .filter(|c| c.state == crate::reliable::ConnState::PendingDial)
      .count()
  }

  /// Number of known members, including the local node itself.
  ///
  /// A freshly constructed node has exactly one member (itself). Peers join
  /// after push/pull exchanges or gossip convergence during the poll loop.
  #[inline]
  pub fn num_members(&self) -> usize {
    self.endpoint.endpoint_ref().num_members()
  }

  /// Drain one application-visible membership or lifecycle event, if any.
  ///
  /// Returns events emitted by the machine during the last `poll` tick.
  /// Returns `None` when the event queue is empty; call again after the
  /// next `poll` tick.
  #[inline]
  pub fn poll_event(&mut self) -> Option<memberlist_proto::event::Event<I, SocketAddr>> {
    self.endpoint.poll_event()
  }

  /// Arm the SWIM scheduler at `now`. Call once before the first `poll`.
  ///
  /// Forwards to `StreamEndpoint::start_scheduling`, which arms the probe,
  /// gossip, and push-pull periodic timers so `poll_timeout` returns a
  /// finite deadline on the very next call.
  pub fn start(&mut self, now: Instant) {
    self.endpoint.start_scheduling(now);
  }

  /// Seed a statically-known peer as Alive, bootstrapping membership without
  /// the TCP push-pull join path.
  ///
  /// Builds a synthetic `Alive` message for `id` at `peer` (incarnation 1)
  /// and feeds it into the machine via `handle_alive`, exactly as if the node
  /// had been learned through gossip. Useful for static embedded clusters and
  /// for tests that skip the join phase.
  ///
  /// A non-routable `peer` (unspecified/multicast/broadcast IP or port 0) is
  /// dropped: it could only be stored as a member no node can send a useful
  /// packet to. The machine's admission filter would reject it too (the same
  /// routable-address delegate runs inline on `handle_alive`), but rejecting at
  /// this public entry makes the contract explicit and avoids building the
  /// synthetic `Alive` at all.
  pub fn inject_alive(&mut self, id: I, peer: SocketAddr, now: Instant) {
    if !endpoint_is_routable(&peer) {
      return;
    }
    let alive = Alive::new(1, Node::new(id.cheap_clone(), peer));
    self.endpoint.handle_alive(peer, alive, now);
  }

  /// Whether `id` is currently Alive from this node's perspective.
  ///
  /// Returns `false` for unknown ids or ids in any non-Alive state.
  #[inline]
  pub fn is_alive(&self, id: &I) -> bool {
    self
      .endpoint
      .endpoint_ref()
      .member_liveness(id)
      .map(|s| s == State::Alive)
      .unwrap_or(false)
  }

  /// Whether `id` is currently Dead from this node's perspective.
  ///
  /// Returns `false` for unknown ids or ids in any non-Dead state.
  #[inline]
  pub fn is_dead(&self, id: &I) -> bool {
    self
      .endpoint
      .endpoint_ref()
      .member_liveness(id)
      .map(|s| s == State::Dead)
      .unwrap_or(false)
  }

  // ── Query accessors ────────────────────────────────────────────────────────
  //
  // These are thin `&self` reads over the live `Endpoint` inside the
  // `StreamEndpoint`. Unlike the async drivers (compio, reactor) there is no
  // `ArcSwap` snapshot: reads go directly to the machine state, so they always
  // reflect the result of the last `poll` tick with no snapshot lag.
  //
  // FSM liveness: the `NodeState.state()` wire field is frozen at the last
  // `Alive` broadcast. The real, gossip-tracked liveness is
  // `endpoint.member_liveness(id)`. Every method that returns a `NodeState`
  // stamps it with the FSM liveness via `ns.as_ref().clone().with_state(fsm)` so
  // that `online_members()` and `is_alive()` agree on the same ground truth.

  /// Return the `NodeState` for `id`, stamped with the current FSM liveness.
  ///
  /// Returns `None` when `id` is unknown to this node. The `NodeState.state()`
  /// field reflects the live gossip-FSM state (`Alive` / `Suspect` / `Dead` /
  /// `Unknown`), not the frozen wire-format value.
  #[inline]
  pub fn by_id(&self, id: &I) -> Option<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    let ns = ep.member(id)?;
    let fsm = ep.member_liveness(id).unwrap_or(State::Unknown(0));
    Some(std::sync::Arc::new(ns.as_ref().clone().with_state(fsm)))
  }

  /// All members currently in the `Alive` FSM state.
  ///
  /// Each returned `NodeState` is stamped with the FSM liveness, so
  /// `online_members()[i].state() == State::Alive` always holds. Consistent
  /// with `is_alive`: if `is_alive(id)` is `true`, `id` appears here.
  #[inline]
  pub fn online_members(&self) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter_map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        if fsm == State::Alive {
          Some(std::sync::Arc::new(ns.as_ref().clone().with_state(fsm)))
        } else {
          None
        }
      })
      .collect()
  }

  /// Count of members currently in the `Alive` FSM state.
  ///
  /// Equivalent to `online_members().len()` but avoids allocating a `Vec`.
  #[inline]
  pub fn num_online_members(&self) -> usize {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter(|ns| {
        ep.member_liveness(ns.id_ref())
          .map(|s| s == State::Alive)
          .unwrap_or(false)
      })
      .count()
  }

  /// All known members (Alive + Suspect + Dead/Left), each stamped with the
  /// current FSM liveness.
  ///
  /// Mirrors the legacy `Memberlist::members` name.
  #[inline]
  pub fn members(&self) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        std::sync::Arc::new(ns.as_ref().clone().with_state(fsm))
      })
      .collect()
  }

  /// Members matching `pred`, each stamped with the current FSM liveness.
  #[inline]
  pub fn members_by(
    &self,
    mut pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool,
  ) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter_map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        let stamped = ns.as_ref().clone().with_state(fsm);
        if pred(&stamped) {
          Some(std::sync::Arc::new(stamped))
        } else {
          None
        }
      })
      .collect()
  }

  /// Count of members matching `pred`.
  #[inline]
  pub fn num_members_by(&self, mut pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool) -> usize {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        let stamped = ns.as_ref().clone().with_state(fsm);
        pred(&stamped)
      })
      .count()
  }

  /// Map-filter members, collecting all `Some` results into a `Vec`.
  ///
  /// Each `NodeState` passed to `f` is stamped with the current FSM liveness.
  #[inline]
  pub fn members_map_by<O>(
    &self,
    mut f: impl FnMut(&NodeState<I, SocketAddr>) -> Option<O>,
  ) -> std::vec::Vec<O> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter_map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        let stamped = ns.as_ref().clone().with_state(fsm);
        f(&stamped)
      })
      .collect()
  }

  /// The local node's Lifeguard health score (`0` = fully healthy; higher = worse).
  ///
  /// Read directly from the live machine endpoint — no snapshot lag.
  #[inline]
  pub fn health_score(&self) -> usize {
    self.endpoint.endpoint_ref().health_score()
  }

  /// The local node's id, cheap-cloned from the machine endpoint.
  #[inline]
  pub fn local_id(&self) -> I {
    self.endpoint.endpoint_ref().local_id_ref().cheap_clone()
  }

  /// The local node's advertised `SocketAddr`.
  #[inline]
  pub fn advertise_address(&self) -> SocketAddr {
    *self.endpoint.endpoint_ref().advertise_ref()
  }

  /// The local node's `NodeState`, stamped with the current FSM liveness.
  #[inline]
  pub fn local_state(&self) -> std::sync::Arc<NodeState<I, SocketAddr>> {
    let ep = self.endpoint.endpoint_ref();
    let local_id = ep.local_id_ref();
    let ns = ep
      .member(local_id)
      .expect("local node is always in the membership map");
    // Alive is the correct fallback for the LOCAL node specifically: from its own
    // perspective the node is always alive, and before start() is called
    // member_liveness may return None.  Unknown(0) would be wrong here because
    // it implies the node's health is uncertain, which it never is locally.
    let fsm = ep.member_liveness(local_id).unwrap_or(State::Alive);
    std::sync::Arc::new(ns.as_ref().clone().with_state(fsm))
  }

  // ── Directed I/O ──────────────────────────────────────────────────────────
  //
  // These are `&mut self` thin forwarders. The poll loop drives all actual
  // I/O; the caller correlates completion by draining `poll_event()` after
  // subsequent `poll` ticks.

  /// Send a direct UDP ping to `node`.
  ///
  /// Returns a [`PingId`] token. The caller should drain `poll_event()` after
  /// subsequent `poll` ticks to observe the terminal event:
  ///
  /// - `Event::PingCompleted { ping_id, .. }` — the peer replied within
  ///   `probe_timeout`.
  /// - `Event::PingFailed { ping_id, .. }` — no reply within `probe_timeout`.
  ///
  /// Unlike a SWIM failure-detection probe, an application ping is direct-only:
  /// it does not fan out to indirect peers, request a reliable fallback, or
  /// mark the target as suspect on timeout.
  #[inline]
  pub fn ping(&mut self, node: Node<I, SocketAddr>, now: Instant) -> PingId {
    self.endpoint.ping(node, now)
  }

  /// Enqueue a directed unreliable UDP user-data packet to `to`.
  ///
  /// The payload is encoded as a `UserData` gossip message and emitted on the
  /// next gossip drain in `poll`. The peer observes it as `Event::UserPacket`
  /// via `poll_event()`. Delivery is best-effort (UDP); callers that need
  /// guaranteed delivery should use [`Self::send_reliable`].
  ///
  /// Returns `Err` when the framed payload exceeds the configured gossip MTU.
  #[inline]
  pub fn send(
    &mut self,
    to: SocketAddr,
    payload: bytes::Bytes,
  ) -> Result<(), memberlist_proto::Error> {
    self.endpoint.send_user_packet(to, payload)
  }

  /// Enqueue multiple directed unreliable UDP user-data packets to `to`.
  ///
  /// When `payloads` contains two or more entries they are compounded into a
  /// single gossip datagram if they fit together within the configured gossip
  /// MTU. The peer observes each payload separately as `Event::UserPacket` via
  /// `poll_event()`.
  ///
  /// Returns `Err` when the compound frame exceeds the gossip MTU.
  #[inline]
  pub fn send_many(
    &mut self,
    to: SocketAddr,
    payloads: &[bytes::Bytes],
  ) -> Result<(), memberlist_proto::Error> {
    self.endpoint.send_user_packets(to, payloads)
  }

  /// Initiate a reliable TCP user-message delivery to `to`.
  ///
  /// The payload is encoded and sent over a dedicated TCP stream. Returns a
  /// [`StreamId`] token. Completion surfaces as
  /// `Event::ExchangeCompleted { kind: ExchangeKind::UserMessage, .. }` via
  /// `poll_event()` after subsequent `poll` ticks.
  ///
  /// The smoltcp poll loop services the resulting `DialRequested` generically —
  /// the same `Connect` path used for join push/pull — so no additional driver
  /// changes are needed to carry user messages over TCP.
  ///
  /// Callers that want non-blocking fire-and-forget should retain the
  /// `StreamId` for debugging only; the poll loop and machine handle the full
  /// dial → handshake → send → FIN → teardown lifecycle.
  ///
  /// **Reliable exchanges share a single listener, so a peer accepts inbound
  /// reliable streams one at a time.** To send multiple reliable messages to
  /// the same peer, issue them sequentially: call `send_reliable`, drive `poll`
  /// until the matching `Event::ExchangeCompleted { kind: UserMessage }` arrives
  /// via `poll_event`, then send the next. Concurrent reliable streams to one
  /// peer would collide at the listener (the second SYN is RST'd during the
  /// first's handshake).
  #[inline]
  pub fn send_reliable(&mut self, to: SocketAddr, payload: bytes::Bytes, now: Instant) -> StreamId {
    self.endpoint.start_user_message(to, payload, now)
  }

  // ── Join ──────────────────────────────────────────────────────────────────

  /// Record intent to join the cluster via these seed addresses.
  ///
  /// Returns immediately; the poll loop initiates a push/pull state exchange
  /// to each seed on the next tick. The caller should watch `is_joined()` or
  /// drain `poll_event()` for `Event::PushPullReplyReceived` / membership
  /// changes, and enforce its own join deadline — this method performs no I/O
  /// and imposes no timeout.
  pub fn join(&mut self, seeds: &[SocketAddr]) {
    for s in seeds {
      // Drop a non-routable seed (unspecified/multicast/broadcast IP or port 0):
      // it could only produce a doomed dial — smoltcp's `connect` rejects the
      // unspecified address and port 0, and the rest are addresses no dial can
      // usefully reach. Queue only seeds a dial can actually complete.
      if endpoint_is_routable(s) {
        self.pending_seeds.push_back(*s);
      }
    }
  }

  /// Queue an application user-data payload for piggyback gossip to peers.
  ///
  /// The payload rides the next gossip rounds as a `UserData` message and
  /// surfaces on each receiving node as `Event::UserPacket` via `poll_event()`.
  /// A payload whose lone framed datagram would exceed the configured gossip
  /// MTU is rejected with `Error::UserBroadcastExceedsMtu` and not stored
  /// (it could never be gossiped even alone). Delivery is best-effort, like all
  /// gossip.
  pub fn queue_user_broadcast(
    &mut self,
    data: bytes::Bytes,
  ) -> Result<(), memberlist_proto::Error> {
    self.endpoint.queue_user_broadcast(data)
  }

  /// Begin leaving the cluster.
  ///
  /// Forwards to the machine's graceful-leave path, which gossips the
  /// departure and ultimately emits `Event::LeftCluster` via `poll_event()`.
  /// The caller enforces its own leave timeout, then stops polling.
  ///
  /// Returns an error if the node is not in a running state (e.g. already left
  /// or never started); the caller may choose to ignore this when tearing down
  /// unconditionally.
  pub fn leave(&mut self, now: Instant) -> Result<(), memberlist_proto::Error> {
    self.endpoint.leave(now)
  }

  /// Whether this node has learned at least one peer.
  ///
  /// `num_members() > 1` means a join push/pull has synced remote state, or a
  /// peer was injected via `inject_alive`. A coarse readiness signal; the
  /// caller owns the real join deadline.
  #[inline]
  pub fn is_joined(&self) -> bool {
    self.num_members() > 1
  }

  /// Advance both the smoltcp stack and the memberlist state machine once.
  /// Returns the next wakeup deadline: the minimum of the smoltcp stack's next
  /// scheduled event, the machine's next timer, AND any driver-owned deadline
  /// (currently the soonest gracefully-closing socket's force-abort instant).
  ///
  /// The caller owns the super-loop and is responsible for sleeping until
  /// the returned deadline, advancing the clock, and calling `poll` again.
  /// Because every deadline the driver enforces on a tick is folded into this
  /// instant, a caller that sleeps exactly to it always wakes in time to honor
  /// them — including reclaiming a closing socket by `Config::close_timeout`.
  ///
  /// # Eight-step order
  ///
  /// 1. **Stack tick** — `iface.poll` drains the device and services TCP/UDP.
  ///    Reap sub-steps then return gracefully-closed (or close-timed-out)
  ///    sockets to the pool, re-establish the listener if missing, and retry any
  ///    dials deferred while the pool was exhausted.
  /// 2. **Accept inbound** — if the listener socket completed a passive open,
  ///    hand it to the machine and replenish the listener from the pool.
  /// 3. **UDP ingress** — drain received datagrams into `handle_gossip`, then
  ///    decode buffered raw frames through the codec and feed typed messages
  ///    back via `handle_packet`.
  /// 4. **TCP ingress pump** — for each connection with an assigned socket,
  ///    drain the socket's rx buffer into `handle_transport_data`; deliver a
  ///    one-shot EOF to the machine when the peer's FIN has been received and the
  ///    receive buffer is fully drained (`RecvError::Finished`).
  /// 5. **Join-seed drain** — call `start_push_pull(seed, Join, now)` for
  ///    each address queued by `join()`. `start_push_pull` issues a
  ///    `DialRequested` internally serviced into a `Connect` action, so step
  ///    6's action drain opens the TCP dial this same tick.
  /// 6. **Machine tick** — `handle_timeout` fires due timers (probe /
  ///    gossip / push-pull schedulers).
  /// 7. **Stream actions + TCP/UDP egress** — drain `poll_action` to create
  ///    dialing connections, flag graceful half-closes, or tear down
  ///    connections; promote dialing connections whose handshake completed to
  ///    `Established`; append new transmits from `poll_transport_transmit` to each
  ///    connection's out queue and flush every queue to its socket; emit any
  ///    deferred graceful FIN whose connection is `Established` and has fully
  ///    drained its tx ring; re-run the listener/dial rebalance over every socket
  ///    the teardown / close paths just freed back to the pool this tick; drain
  ///    `poll_memberlist_transmit` to encode and send gossip datagrams.
  /// 8. **Deadline** — return `min(stack_next, machine_next, closing_next)`,
  ///    where `closing_next` is the soonest gracefully-closing socket's
  ///    force-abort deadline, so a caller sleeping to the returned instant wakes
  ///    in time to reap it by `close_timeout`.
  pub fn poll(&mut self, now: Instant, device: &mut D) -> Option<Instant> {
    let s_now = to_smoltcp_instant(now);

    // 1. Stack tick.
    // `PollResult` is ignored — the machine drives its own state via the
    // steps below; we do not gate machine work on stack idle/active.
    self.iface.poll(s_now, device, &mut self.sockets);

    // 1a. Reap gracefully-closing sockets. The stack tick above may have
    // advanced FIN exchanges to completion; reclaim any that are now fully
    // closed (or have exceeded `cfg.close_timeout`) so the freed handles back
    // new dials/accepts this same tick.
    self.reap_closing(now);

    // The accept/replenish/dial phase is ordered LISTENER-FIRST: the inbound
    // listener gets first claim on a free socket and a deferred outbound dial
    // takes only what remains. The whole reliable plane can be driven from a
    // single spare socket, so an inbound peer must never be starved of a socket
    // by outbound intent. The two steps below run in this exact order:
    //
    //   1b. `check_listener`  — consume an accept-ready listener + replenish
    //   1c. `rebalance_pool`  — self-heal a missing listener, then deferred dials
    //                           take any remaining sockets (listener-first)
    //
    // 1b. Accept an inbound connection completed on the listener and replenish
    // the listener from the pool.
    //
    // The step-1 stack tick promotes a listener whose three-way handshake just
    // finished from Listen to Established; `check_listener` sees that here (it
    // needs only that prior `iface.poll`, nothing later in the phase), hands the
    // connection to the machine, registers the exchange↔socket mapping, then
    // immediately re-`listen`s a fresh listener from the pool so the next inbound
    // SYN has a socket ready. Running this BEFORE the dial rebalance is what
    // enforces listener priority: with one spare socket and an outbound backlog,
    // draining dials first would steal that socket and leave the listener `None`
    // and unreplenishable — starving inbound until some later exchange frees a
    // socket. Accept-and-replenish first claims it for the listener instead.
    self.check_listener(now);

    // 1c. Self-heal a still-missing listener, then assign any remaining free
    // sockets to deferred dials (listener-first). This runs the SAME rebalance
    // as the late call after the in-tick frees below (step 7), here over whatever
    // `reap_closing` freed plus the spare pool. Running it BEFORE the machine
    // tick (step 6) is required: a `PendingDial` deferred on a PRIOR tick must be
    // assigned a freed socket and dialed before step 6's `handle_timeout` could
    // elapse its bridge and tear it down — so the early site cannot move later.
    self.rebalance_pool(now);

    // 2a. Drain inbound UDP datagrams into the machine's raw ingress buffer.
    {
      // Disjoint field borrows: the UDP socket, the reusable receive scratch,
      // and the machine endpoint are three separate fields, so all three can be
      // borrowed across the drain loop at once.
      let sock = self.sockets.get_mut::<udp::Socket>(self.udp);
      let buf = self.gossip_recv.as_mut_slice();
      let endpoint = &mut self.endpoint;
      while sock.can_recv() {
        match sock.recv_slice(buf) {
          Ok((n, meta)) => {
            let src = from_endpoint(meta.endpoint);
            endpoint.handle_gossip(src, &buf[..n], now);
          }
          // The ring is empty — nothing more to drain this tick.
          Err(udp::RecvError::Exhausted) => break,
          // The datagram exceeded `buf` and was already POPPED by `recv_slice`
          // (smoltcp dequeues before the length check), so it is consumed and
          // gone. This is an over-budget peer datagram — larger than the
          // configured gossip MTU plus encryption overhead the buffer is sized
          // for. Skip it and CONTINUE draining the rest of the rx ring rather
          // than breaking, so one oversized datagram cannot stall delivery of
          // the in-budget datagrams queued behind it.
          Err(udp::RecvError::Truncated) => continue,
        }
      }
    }

    // 2b. Unwrap the encryption/compression transforms, then decode each raw
    // gossip frame and feed typed messages back.
    while let Some((src, raw)) = self.endpoint.poll_memberlist_ingress() {
      // Strip the Encrypted-then-Compressed wrapper stack FIRST (each layer is
      // identity when its wrapper is absent, and the whole call is identity
      // when no keyring is configured, so the plaintext path is preserved
      // exactly). On an encrypted cluster the strict-mode entry check rejects a
      // non-Encrypted datagram here; a corrupt frame, an unknown algorithm, or
      // an oversized wrapper is likewise an Err. Drop on Err — a plaintext
      // datagram on an encrypted node, or a corrupt frame, must not reach the
      // decoder. Gossip is lossy and self-healing.
      // Strip the Encrypted-then-Compressed wrapper stack FIRST (each layer is
      // identity when its wrapper is absent, and the whole call is identity
      // when no keyring is configured, so the plaintext path is preserved
      // exactly). On an encrypted cluster the strict-mode entry check rejects a
      // non-Encrypted datagram here; a corrupt frame, an unknown algorithm, or
      // an oversized wrapper is likewise an Err. Drop on Err — a plaintext
      // datagram on an encrypted node, or a corrupt frame, must not reach the
      // decoder. Gossip is lossy and self-healing.
      let decrypted = match self.endpoint.decrypt_gossip(&raw) {
        Ok(p) => bytes::Bytes::from(p),
        Err(_) => continue,
      };
      let opts = memberlist_proto::codec::DecodeOptions::new(None);
      // Drop malformed inbound datagrams silently — bad network input must not
      // panic the node; SWIM is self-healing. (`decode_incoming` Err = label
      // mismatch / framing error; `parse_messages` Err = malformed frame.)
      if let Ok(plain) = memberlist_proto::codec::decode_incoming(decrypted, &opts) {
        if let Ok(msgs) = memberlist_proto::codec::parse_messages::<I, SocketAddr>(plain) {
          for msg in msgs {
            self.endpoint.handle_packet(src, msg, now);
          }
        }
      }
    }

    // 3. TCP ingress pump: drain each active exchange's socket rx buffer into
    // the machine. Must run before the machine tick so the machine sees fresh
    // inbound bytes (including the peer-FIN EOF) when firing timers.
    self.pump_inbound_reliable(now);

    // 5. Drain join seeds: each seed queued by `join()` gets a push/pull
    // exchange initiated now. `start_push_pull` internally calls `service_dials`
    // + `flush_outbound`, queuing a `Connect` action that step 7a below will
    // consume this same tick, so the first TCP dial bytes are emitted without
    // requiring an additional poll.
    while let Some(seed) = self.pending_seeds.pop_front() {
      // StreamId is the machine's correlation token for this exchange. The dial
      // is correlated via the ExchangeId carried in the resulting Connect action,
      // not by the StreamId; the driver does not need to retain it.
      let _sid = self.endpoint.start_push_pull(seed, PushPullKind::Join, now);
    }

    // 6. Machine tick: fire due timers (probe / gossip / push-pull).
    self.endpoint.handle_timeout(now);

    // 7a. Drain stream actions: open dials, half-close, or tear down exchanges.
    //
    // Actions are drained BEFORE transport transmits because a Connect must
    // install the exchange↔socket mapping before this same tick's outbound
    // bytes for that exchange are written (see the stream-endpoint ordering
    // contract in memberlist-proto/src/streams/mod.rs).
    self.drain_stream_actions(now);

    // 7b. Promote dialing connections whose handshake completed this tick to
    // Established, so the egress pump and the deferred-FIN gate below see an
    // accurate lifecycle state.
    self.promote_established();

    // 7c. TCP egress pump: append new transmits to each connection's out queue,
    // then flush every connection's queue to its socket.
    self.pump_outbound_reliable();

    // 7d. Emit any deferred graceful write-half FINs whose connection is now
    // Established and whose outbound bytes have fully drained to the peer. The
    // connection stays mapped in `connections` (its inbound reply + FIN still
    // pump); the socket is reclaimed only later by the machine's `Close`.
    self.flush_pending_shutdowns();

    // 7d'. Complete the deferred terminal close of any connection draining in
    // `Closing`: a graceful `Close` whose send-capable socket still held
    // undelivered outbound bytes was kept mapped so 7c could keep flushing them.
    // Now that this tick's flush has run, emit the terminal FIN + detach for any
    // whose `out` and tx ring are fully drained, or force-abort one past its
    // close deadline. Runs AFTER 7c so a connection that finished draining this
    // very tick FINs the same tick rather than waiting for the next.
    self.flush_closing(now);

    // 7d''. Re-run the listener/dial rebalance over every socket the machine tick
    // and teardown just freed back to the pool IN THIS TICK. The early 1c
    // rebalance ran before step 6 and saw only what `reap_closing` had freed; the
    // socket-freeing close paths run LATER (after the machine tick fires the
    // `Close` actions), so a socket freed here would otherwise sit idle until the
    // next poll — stranding a deferred `PendingDial` or a missing listener until
    // some unrelated timer happened to wake the driver, possibly past the waiting
    // exchange's own bridge deadline (which would then kill it before it ever got
    // the socket). Servicing the frees in-tick lets a freed socket immediately
    // back the oldest waiting dial — its SYN egress is then driven by the stack
    // deadline, which `poll_at` reports as ~now below — or restore the listener,
    // so the returned wakeup is naturally correct with no `pending_dial` deadline
    // term needed.
    //
    // This MUST stay positioned after EVERY late `pool.give()` path so it
    // dominates all of them. Today those are exactly three, all upstream here:
    //   - 7a `drain_stream_actions` → `teardown`: the `Closed | TimeWait` branch
    //     and the abrupt `abort()` branch both `pool.give(h)`.
    //   - 7d' `flush_closing`: the deadline-`Abort` branch `pool.give(h)`.
    // (`teardown`'s and `flush_closing`'s graceful-FIN branches `closing.insert`
    // instead of `give`; those handles are reaped to the pool by a LATER tick's
    // 1a `reap_closing`, whose freed sockets the next tick's 1c rebalance claims —
    // so they need no in-tick rebalance here.) If a new late free path is ever
    // added, it must precede this call or the end-of-tick invariant below regresses.
    self.rebalance_pool(now);

    // Invariant held at end-of-tick: if the reliable pool is non-empty then a
    // listener is present AND no connection remains in `PendingDial`. The late
    // rebalance above is the last pool-touching reliable phase — `drain_gossip_transmits`
    // touches only the UDP gossip socket, never the reliable pool — so the
    // invariant cannot be disturbed before the deadline is computed below.
    debug_assert!(
      self.plane.pool.free_len() == 0
        || (self.plane.listener.is_some() && self.pending_dial_count() == 0),
      "end-of-tick: a free reliable socket left a listener missing or a PendingDial unserviced"
    );

    // 7e. Egress: drain outbound gossip transmits, encode, and send.
    self.drain_gossip_transmits();

    // 8. Next deadline = min(stack, machine, closing).
    //
    // The returned instant is the wake contract: a caller that sleeps until it
    // must wake in time to enforce every deadline the driver owns. Three classes
    // contribute:
    //
    // - `stack` — smoltcp's next scheduled event (retransmit, delayed-ACK, …).
    // - `machine` — the SWIM machine's next timer (probe / gossip / push-pull /
    //   bridge handshake-and-dial deadlines).
    // - `closing` — the soonest force-abort deadline among sockets parked
    //   mid-close. Two driver-owned deadline sources feed this class, both
    //   enforced only on a tick that actually runs: the `plane.closing` map (a
    //   detached handle whose FIN is in flight, reaped by `reap_closing`) and the
    //   `close_deadline` of any connection still draining in `Closing` before its
    //   terminal FIN (the backstop in `flush_closing`). If either were omitted, a
    //   peer that vanished mid-close would keep its socket `is_open()` forever
    //   (smoltcp sets no TCP timeout), and a deadline-driven caller — sleeping
    //   only to `min(stack, machine)` — could sleep arbitrarily past
    //   `close_timeout`, leaving the socket reclaimed only by whatever unrelated
    //   timer next woke the driver. Folding the soonest of BOTH in guarantees the
    //   caller wakes by `close_timeout` to reap it, so pool / listener recovery is
    //   bounded by `Config::close_timeout` as documented.
    //
    // `pending_dial` deliberately contributes NO deadline of its own: a buffered
    // dial is serviced when a socket frees, never on a clock of its own, and every
    // free either is handled THIS tick or already feeds one of the terms above.
    // A socket freed straight to the pool by a teardown / close this tick (7a / 7d')
    // is spent on the oldest waiting dial by the late 7d'' rebalance immediately,
    // so its SYN is queued before this deadline is computed and surfaces as a
    // `stack` term (`poll_at` ~now). A socket whose graceful FIN is still in flight
    // is reaped to the pool only on a LATER tick by `reap_closing`, but that tick
    // is itself guaranteed by the `closing` deadline folded in above; the next
    // tick's early rebalance then dials the waiting connection. Either way the
    // unblocking event is already covered, so a `pending_dial` term would be
    // redundant.
    let stack = self
      .iface
      .poll_at(s_now, &self.sockets)
      .map(from_smoltcp_instant);
    let machine = self.endpoint.poll_timeout();
    // Soonest force-abort deadline across BOTH close backstops: detached handles
    // parked in `plane.closing`, and connections still draining in `Closing`
    // before their terminal FIN (their `close_deadline`).
    let closing = self
      .plane
      .closing
      .values()
      .chain(
        self
          .plane
          .connections
          .values()
          .filter_map(|c| c.close_deadline.as_ref()),
      )
      .min()
      .copied();
    min_opt(min_opt(stack, machine), closing)
  }

  /// Reclaim gracefully-closing sockets that have finished closing or whose
  /// close has exceeded `cfg.close_timeout`.
  ///
  /// A graceful `teardown` issues a FIN and parks the handle in `plane.closing`
  /// with a deadline (see `teardown`). The socket then works through the TCP FIN
  /// states (FinWait / Closing / LastAck / TimeWait) before becoming reusable.
  /// This pass returns to the pool every parked handle that has reached a
  /// reusable state and leaves the rest parked for a later tick.
  ///
  /// "Reusable" is `!is_open()`. Per `smoltcp/src/socket/tcp.rs`, `is_open()`
  /// is false only in the `Closed` and `TimeWait` states, and those are exactly
  /// the states in which the socket's next consumer — `connect()` (dial) or
  /// `listen()` (replenish) — is accepted; both reject an open socket with
  /// `InvalidState` and both `reset()` the socket on reuse, discarding any
  /// `TimeWait` 2MSL remainder. A socket still flushing its FIN
  /// (FinWait1/2, Closing, LastAck) is still `is_open()` and would normally stay
  /// parked, so a socket is never reclaimed before its FIN completes.
  ///
  /// The deadline bounds that wait: smoltcp applies no TCP timeout by default,
  /// so a peer that vanishes mid-FIN leaves the socket `is_open()` forever and
  /// the handle would leak. When `now >= deadline`, this pass `abort()`s the
  /// socket (forcing it straight to `Closed`) before returning it, so a stuck
  /// graceful close cannot permanently shrink the pool. A healthy close reaches
  /// `Closed` long before the deadline and is reclaimed on the `!is_open()` path.
  fn reap_closing(&mut self, now: Instant) {
    // Borrow split: `closing` lives on `plane`, the sockets on `self.sockets`.
    // Retain the still-closing handles; give the finished (or timed-out) ones
    // back to the pool.
    let sockets = &mut self.sockets;
    let pool = &mut self.plane.pool;
    self.plane.closing.retain(|&h, &mut deadline| {
      if !sockets.get::<tcp::Socket>(h).is_open() {
        // Clean close completed (Closed / TimeWait): reclaim.
        pool.give(h);
        return false;
      }
      if now >= deadline {
        // Peer vanished mid-FIN: force the socket to Closed and reclaim so the
        // pool (and the listener replenished from it) recover.
        sockets.get_mut::<tcp::Socket>(h).abort();
        pool.give(h);
        return false;
      }
      // Still flushing its FIN within the deadline — keep parked.
      true
    });
  }

  /// Check whether the listener socket accepted an inbound connection.
  ///
  /// After `iface.poll`, a passive open is *complete* only once the listener
  /// socket reaches Established — i.e. the remote's final ACK of the three-way
  /// handshake has arrived. `remote_endpoint()` is then populated and the
  /// connection can carry the push/pull byte exchange. We swap the
  /// now-connected socket out of `plane.listener` and replenish a fresh listener
  /// from the pool if one is available.
  fn check_listener(&mut self, now: Instant) {
    let h = match self.plane.listener {
      Some(h) => h,
      None => return,
    };

    // Check whether the passive open COMPLETED this tick.
    let accepted = {
      let sock = self.sockets.get_mut::<tcp::Socket>(h);
      // The gate is `may_send()`, which per smoltcp is true only in Established
      // and CloseWait. Both mean the handshake settled and our send half is
      // open (CloseWait additionally covers a peer that already half-closed
      // after sending its push/pull half — still a completed, accept-worthy
      // connection).
      //
      // `is_active()` MUST NOT be used here: it is also true in SynReceived
      // (the half-open state after we send the SYN-ACK but before the remote's
      // final ACK). Accepting in SynReceived would move a not-yet-established
      // socket out of the listener slot while it still carries the listen
      // endpoint; a retransmit/RST during the unfinished handshake then flips
      // that socket back to Listen (smoltcp `tcp.rs`: an RST in SynReceived with
      // a non-zero listen_endpoint reverts to Listen), silently turning the
      // exchange's socket into a second listener and wedging the join. Gating on
      // `may_send()` accepts strictly at/after Established, where an RST closes
      // the socket cleanly instead of reverting it.
      sock.may_send()
    };

    if !accepted {
      return;
    }

    // Extract the remote address before calling into the machine (split borrow:
    // we need `sockets` for the address and `endpoint` for accept_connection).
    let remote_ep = self
      .sockets
      .get_mut::<tcp::Socket>(h)
      .remote_endpoint()
      .map(from_endpoint);

    if let Some(peer) = remote_ep {
      // Register the inbound exchange with the machine; it returns the
      // ExchangeId the driver uses to route subsequent inbound bytes. The
      // listener socket already completed its handshake (we gated on
      // `may_send()`), so the Connection starts Established.
      let eid = self.endpoint.accept_connection(peer, now);
      self
        .plane
        .connections
        .insert(eid, crate::reliable::Connection::accepted(peer, h));
      self.plane.accepted_inbound += 1;
    } else {
      // The socket claimed to be active but has no remote endpoint —
      // reachable only if the TCP state is inconsistent. Abort and return
      // the socket to the pool so it can be reused.
      self.sockets.get_mut::<tcp::Socket>(h).abort();
      self.plane.pool.give(h);
    }

    // The listener socket is now the exchange (or was aborted back to the pool);
    // either way the listener slot is empty.
    self.plane.listener = None;

    // Replenish immediately if a socket is free. This is the SAME path the
    // poll-phase `ensure_listener` self-heal uses, so there is exactly one
    // listener-establishment routine and the two cannot drift. Replenishing here
    // — before `drain_pending_dials` runs later this tick — is what gives the
    // listener first claim on a free socket over any deferred outbound dial. When
    // the pool is empty here, the slot stays empty this tick and a later poll
    // re-establishes it once a socket is freed (this exchange completing, or a
    // concurrent one reaped), preserving the self-healing invariant.
    self.ensure_listener();
  }

  /// Re-establish the passive-open listener if it is missing and the pool can
  /// supply a socket.
  ///
  /// The accept path (`check_listener`) moves the listener socket into the
  /// accepted exchange and replenishes the listener only from whatever is free
  /// at that instant; a momentarily exhausted pool therefore leaves the listener
  /// slot empty with no other path to restore it. Calling this at the top of
  /// each `poll` (after the reap pass) closes that gap: as soon as a socket is
  /// free again, the listener is rebuilt, so a transient full pool never
  /// permanently stops the node from accepting inbound reliable connections.
  ///
  /// A no-op when a listener already exists or the pool is empty.
  fn ensure_listener(&mut self) {
    if self.plane.listener.is_some() {
      return;
    }
    if let Some(h) = self.plane.pool.take() {
      // `listen()` only fails on port 0 or an already-open socket. Neither
      // applies: a pooled socket is Closed (freshly created, or `reset()` on
      // reuse out of TimeWait/Closed) and `cfg.port` is the user-supplied
      // non-zero port.
      // Ignoring Err: the two failure modes above are both unreachable here.
      let _ = self.sockets.get_mut::<tcp::Socket>(h).listen(self.cfg.port);
      self.plane.listener = Some(h);
    }
  }

  /// Open a TCP dial for the `Dialing` connection `eid` on its assigned socket
  /// `h`.
  ///
  /// The caller has already created/transitioned the [`crate::reliable::Connection`]
  /// to [`crate::reliable::ConnState::Dialing`] with `socket: Some(h)` (so this
  /// same tick's parked/outbound push-pull bytes and later inbound bytes route to
  /// `h`). This issues the smoltcp `connect()`. On a connect rejection (e.g. the
  /// local address is unresolvable for this peer) — or a peer that is not a
  /// routable destination, screened up front before `connect` is ever called —
  /// the socket is aborted and returned to the pool — never leaked — the
  /// `Connection` is removed, and an EOF is latched on the bridge as a best-effort
  /// cancel. (For a `Handshaking` bridge that EOF is consumed only post-promotion,
  /// so the bridge is ultimately retired by its own dial/handshake deadline; the
  /// driver has no prompt dial-cancel path.) Shared by the live `Connect` drain
  /// and the deferred `drain_pending_dials` retry so both dial identically.
  fn dial(&mut self, eid: ExchangeId, peer: SocketAddr, h: SocketHandle, now: Instant) {
    // Screen a non-routable peer BEFORE `connect`. A non-routable peer can never
    // be a useful TCP destination: smoltcp's `connect` rejects the unspecified
    // address and port 0 with `Unaddressable`, and a multicast/broadcast remote
    // resolves only to a derived L2 multicast/broadcast MAC. Screen here on
    // smoltcp's own `is_unicast` predicate so no doomed connect is started, and
    // reclaim cleanly exactly as the connect-rejection path does: the
    // freshly-assigned socket is still Closed, so `abort()` is a no-op that
    // returns it reusable (never leaked), and latch the best-effort EOF so the
    // bridge tears down by its own deadline.
    if !endpoint_is_routable(&peer) {
      self.sockets.get_mut::<tcp::Socket>(h).abort();
      self.plane.pool.give(h);
      self.plane.connections.remove(&eid);
      self.endpoint.handle_transport_data(eid, &[], true, now);
      return;
    }

    // Derive an ephemeral local port from the ExchangeId so each dial uses a
    // distinct port within the IANA ephemeral range (49152–65535, 16 384 ports).
    // The ExchangeId is a monotonically increasing u64 per endpoint, making this
    // a cheap, collision-resistant scheme without an explicit port allocator.
    let local_port = 49152u16 + (eid.get() as u16 % 16384);
    let remote_ep = to_endpoint(peer);
    let cx = self.iface.context();
    if self
      .sockets
      .get_mut::<tcp::Socket>(h)
      .connect(cx, remote_ep, local_port)
      .is_err()
    {
      // The connect was rejected before any SYN: abort, reclaim the socket, drop
      // the Connection (with all its parked state), and latch a best-effort EOF.
      self.sockets.get_mut::<tcp::Socket>(h).abort();
      self.plane.pool.give(h);
      self.plane.connections.remove(&eid);
      self.endpoint.handle_transport_data(eid, &[], true, now);
    }
  }

  /// Assign a freed socket to each connection still waiting in
  /// [`crate::reliable::ConnState::PendingDial`], one per freed socket, and dial
  /// it.
  ///
  /// Services the waiting connections oldest-first — by ascending `ExchangeId`,
  /// which is the machine's monotonically increasing per-endpoint correlation
  /// token, so the oldest deferred dial is dialed first. Stops the moment the
  /// pool empties again so the rest stay parked for a later tick. Called each
  /// `poll` LAST in the accept/replenish/dial phase — after `reap_closing`,
  /// `check_listener`, and the `ensure_listener` self-heal — so the inbound
  /// listener has already taken its socket and a deferred dial claims only what
  /// remains, yet a socket freed by a timed-out dead-seed bridge is still
  /// promptly spent on the oldest waiting viable dial rather than the intent
  /// being lost.
  ///
  /// A connection already retired (its bridge timed out and issued a `Close`) was
  /// removed from `connections` by `teardown`, so this never dials a dead
  /// exchange. Each assigned connection's parked `out` bytes and `fin_pending`
  /// FIN survive the transition and flush/fire once the socket is Established.
  fn drain_pending_dials(&mut self, now: Instant) {
    // Collect the waiting dials oldest-first. The borrow of `connections` is
    // released before the dial loop mutates the plane.
    let mut waiting: std::vec::Vec<(ExchangeId, SocketAddr)> = self
      .plane
      .connections
      .iter()
      .filter(|(_, c)| c.state == crate::reliable::ConnState::PendingDial)
      .map(|(&eid, c)| (eid, c.peer))
      .collect();
    if waiting.is_empty() {
      return;
    }
    waiting.sort_by_key(|(eid, _)| eid.get());

    for (eid, peer) in waiting {
      let Some(h) = self.plane.pool.take() else {
        // Pool exhausted again — leave the rest parked for a later tick.
        break;
      };
      // Assign the freed socket and transition PendingDial → Dialing, then issue
      // the connect. `assign_socket` retains any parked `out` / `fin_pending`.
      // The connection is still present (a racing Close would have removed it,
      // but `waiting` was just collected from `connections` this same tick with
      // no intervening machine call), so the assignment always finds it.
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.assign_socket(h);
      }
      self.dial(eid, peer, h, now);
    }
  }

  /// Give the listener and any deferred dials first claim on whatever is
  /// currently in the pool: self-heal a missing listener, then assign the rest to
  /// `PendingDial` connections oldest-first.
  ///
  /// Idempotent and LISTENER-FIRST — `ensure_listener` claims at most one socket
  /// before `drain_pending_dials` touches the pool, so an inbound listener is
  /// never starved by deferred outbound intent — and a no-op when the pool is
  /// empty or there is no unmet demand. Run at two points each `poll`: early
  /// (1c), over the sockets `reap_closing` freed plus the spare pool, BEFORE the
  /// machine tick so a prior-tick `PendingDial` is dialed before its bridge can
  /// time out; and late (7d''), over every socket the machine's teardown / close
  /// paths freed THIS tick, so an in-tick free immediately backs a waiting dial or
  /// restores the listener rather than sitting idle until the next poll.
  fn rebalance_pool(&mut self, now: Instant) {
    self.ensure_listener();
    self.drain_pending_dials(now);
  }

  /// Drain all `StreamAction`s emitted by the machine this tick.
  ///
  /// `Connect` — take a pooled socket, create a `Dialing`
  /// [`crate::reliable::Connection`] (socket assigned), and `dial` it. When the
  /// pool is exhausted a `PendingDial` connection (no socket) is recorded instead
  /// and `drain_pending_dials` assigns a socket once one frees, so the intent is
  /// never lost. If the connect call itself errors the socket is returned to the
  /// pool, the connection is removed, and the bridge is retired by its own
  /// dial/handshake deadline, since the driver has no prompt dial-cancel path.
  ///
  /// `Shutdown` — the SEND-half close signal: the local side finished sending
  /// but the bridge is still awaiting the peer's reply and/or FIN. The graceful
  /// write-half FIN (`tcp::Socket::close`) is DEFERRED by setting the connection's
  /// `fin_pending` flag; `flush_pending_shutdowns` emits it once the connection is
  /// `Established` and the tx ring has drained, then transitions it to
  /// `HalfClosed` (still mapped) so the peer's reply still pumps inbound. The
  /// socket is reclaimed only later, by this exchange's `Close`.
  ///
  /// `Close` — the exchange completed GRACEFULLY (the bridge reached
  /// `BothClosed`): tear down via `teardown`, which removes the connection
  /// (dropping all its per-exchange state) and reclaims its socket by TCP state —
  /// directly to the pool if both FINs already completed, parked in `closing` if
  /// our FIN is in flight (we half-closed, or an acceptor graceful-closes its
  /// CloseWait socket to flush its reply + a clean EOF), or — when buffered bytes
  /// remain — parked in `Closing` to finish flushing them before the terminal
  /// FIN. A graceful close never discards undelivered bytes.
  ///
  /// `Abort` — the exchange FAILED (dial failure, label/encryption rejection, or
  /// an elapsed exchange deadline): tear down via `abort_exchange`, which removes
  /// the connection (DISCARDING its buffered `out` bytes), hard-`abort()`s the
  /// socket (RST), and returns it straight to the pool. No `Closing` drain, no
  /// graceful FIN — the bytes are stale and the peer is given up on.
  fn drain_stream_actions(&mut self, now: Instant) {
    // The machine guarantees all Connects surface before any Shutdown/Close/Abort
    // (see StreamEndpoint::poll_action ordering contract). Draining fully in
    // one pass is therefore safe: no teardown can precede its Connect.
    while let Some(action) = self.endpoint.poll_action() {
      match action {
        StreamAction::Connect(info) => {
          let eid = info.id();
          let peer = info.peer();

          match self.plane.pool.take() {
            Some(h) => {
              // A socket is free: create the Dialing connection (socket assigned)
              // and issue the connect this same tick.
              self
                .plane
                .connections
                .insert(eid, crate::reliable::Connection::dialing(peer, h));
              self.dial(eid, peer, h, now);
            }
            None => {
              // Pool exhausted: no socket free to back this dial right now. The
              // Connect action was consumed by poll_action and is never
              // re-emitted, so dropping it would LOSE the dial intent. Record a
              // PendingDial connection (no socket yet) instead; this same tick's
              // request bytes and a same-tick Shutdown accumulate on it, and
              // `drain_pending_dials` assigns a socket once `reap_closing` frees
              // one (e.g. when a dead seed's bridge times out). This is what lets
              // a multi-seed `join()` reach a viable later seed even when earlier
              // dead seeds momentarily hold every socket.
              self
                .plane
                .connections
                .insert(eid, crate::reliable::Connection::pending_dial(peer));
            }
          }
        }

        StreamAction::Shutdown(r) => {
          // Graceful write-side half-close (TCP FIN), DEFERRED. Issuing
          // smoltcp's `close()` now would destroy a pre-Established socket and
          // its buffered push/pull bytes (see the SynSent/Listen → Closed jump
          // in smoltcp `tcp.rs::close`). Set the connection's `fin_pending` flag;
          // `flush_pending_shutdowns` emits the FIN once the socket is Established
          // and its tx ring has fully drained, then transitions the connection to
          // HalfClosed (still mapped) so the reply still pumps inbound. A Shutdown
          // for a PendingDial connection (socket not yet assigned) is honored too:
          // the flag rides the connection until the socket is assigned and drains.
          if let Some(conn) = self.plane.connections.get_mut(&r.id()) {
            conn.fin_pending = true;
          }
        }

        StreamAction::Close(r) => {
          // The exchange is done — tear it down and reclaim its socket. `teardown`
          // removes the Connection and reclaims by socket state (clean both-FIN
          // done → pool; our FIN already sent but peer's pending → park in
          // `closing`; nothing left to deliver → graceful FIN then park; abrupt /
          // never-established → abort). The one case it does NOT remove on the
          // spot is a graceful close whose send-capable socket still holds
          // undelivered bytes: it parks the Connection in `Closing` so the egress
          // pump can finish flushing them, and `flush_closing` removes it and FINs
          // once they are delivered — draining the reply rather than truncating it.
          self.teardown(r.id(), now);
        }

        StreamAction::Abort(r) => {
          // The exchange FAILED (dial failure, label/encryption rejection, or an
          // elapsed exchange deadline): its buffered `out` bytes are stale and
          // MUST be discarded, not drained. Hard-reset the socket (RST) and
          // reclaim it immediately — no `Closing` drain, no graceful FIN. Unlike
          // `Close`, `abort_exchange` never parks the connection: a failed
          // exchange owes nothing to the peer, so its socket returns straight to
          // the pool.
          self.abort_exchange(r.id());
        }
      }
    }
  }

  /// Abort the exchange for the machine's `StreamAction::Abort`, discarding any
  /// buffered outbound bytes and reclaiming the TCP socket immediately.
  ///
  /// `Abort` is the machine's FAILED-terminal signal: the bridge reached a
  /// failed phase (dial failure, label/encryption rejection, or an elapsed
  /// exchange deadline). The buffered `out` bytes belong to an exchange the
  /// coordinator has given up on — flushing them would leak membership state
  /// from a failed push/pull (or hold the socket open until `close_timeout`
  /// draining bytes the peer will never act on), so they are discarded with the
  /// whole `Connection`.
  ///
  /// This is the unconditional analog of `teardown`'s abrupt `else` branch: the
  /// socket (if one was assigned) is `abort()`ed — a TCP RST that moves it to
  /// `Closed` in one step — and returned straight to the pool, regardless of the
  /// connection's prior state (`Dialing`, `Established`, `HalfClosed`, or
  /// `Closing`). A `PendingDial` connection (pool was exhausted, no socket
  /// assigned) has nothing to reset or reclaim: removing it is the whole abort,
  /// so a failed exchange is never later dialed.
  fn abort_exchange(&mut self, eid: memberlist_proto::streams::ExchangeId) {
    let Some(conn) = self.plane.connections.remove(&eid) else {
      return;
    };
    // Removing the `Connection` already dropped its parked `out` bytes, the
    // deferred FIN flag, and the EOF-delivered flag. Reclaim the socket (if any)
    // with a hard reset so a half-delivered frame is not flushed.
    if let Some(h) = conn.socket {
      self.sockets.get_mut::<tcp::Socket>(h).abort();
      self.plane.pool.give(h);
    }
  }

  /// Tear down the exchange for the machine's `StreamAction::Close`, draining any
  /// undelivered outbound bytes before the terminal FIN and reclaiming the TCP
  /// socket.
  ///
  /// `Close` is the machine's GRACEFUL terminal signal: the bridge reached
  /// `BothClosed` (peer replied + FIN'd). The failed-terminal case (an elapsed
  /// exchange deadline, a dial/label/encryption failure) is signalled separately
  /// by `StreamAction::Abort` → `abort_exchange`, which discards rather than
  /// drains. The connection is removed (dropping ALL of its
  /// per-exchange state at once — the parked `out` bytes, the deferred
  /// `fin_pending` FIN, and the `eof_delivered` flag) ONLY on a path that
  /// completes the teardown this tick; a graceful close with bytes still to
  /// deliver is instead deferred (see the drain branch below). The socket (if one
  /// was assigned) is handled by its TCP state:
  ///
  /// - `!is_open()` (Closed / TimeWait) — both FINs already exchanged (the clean
  ///   `BothClosed` case where our graceful FIN went out in
  ///   `flush_pending_shutdowns` and the peer's FIN was pumped in): remove the
  ///   connection and return the handle straight to the pool, no close handshake
  ///   left to wait on.
  /// - `is_open()` after a graceful half-close (the connection was in
  ///   [`crate::reliable::ConnState::HalfClosed`]): our FIN is already in flight,
  ///   so `out` can no longer be flushed (the tx half is closed). Remove the
  ///   connection and park the handle in `closing` with a `now + close_timeout`
  ///   deadline; the reap pass reclaims it once it reaches Closed, or
  ///   force-`abort()`s it at the deadline so a vanished peer cannot leak the
  ///   socket.
  /// - send-capable (`Established` / `CloseWait`) with outbound bytes still
  ///   undelivered (`!out.is_empty()` OR `send_queue() != 0`) — a push/pull reply
  ///   (or request) larger than the tx ring whose remainder is parked in `out`
  ///   from partial-write backpressure, or still in the tx ring awaiting ACK.
  ///   Issuing the FIN now would truncate it: `close()` only FINs after the bytes
  ///   already IN the tx ring, never the remainder still in `out`, and an
  ///   `abort()` would RST over a partial frame — collapsing the reliable
  ///   push/pull to a gossip-only sync (or losing the reply entirely). Instead
  ///   transition the connection to [`crate::reliable::ConnState::Closing`] with a
  ///   `now + close_timeout` deadline and KEEP it mapped, so `pump_outbound_reliable`
  ///   keeps flushing `out` into the tx ring; `flush_closing` emits the terminal
  ///   FIN and detaches the socket only once `out` is empty and the tx ring is
  ///   fully acknowledged, or force-aborts at the deadline if the peer never
  ///   drains it.
  /// - send-capable with nothing left to deliver (`out` empty AND
  ///   `send_queue() == 0`) — an acceptor in `CloseWait` whose reply already
  ///   reached the wire, or an `Established` one-shot teardown with an empty tx
  ///   ring. There is nothing to drain: emit the graceful FIN immediately
  ///   (`close()` → LastAck / FinWait1, giving the peer a clean EOF so its
  ///   initiator commits the response) and park the handle in `closing` for the
  ///   reap pass.
  /// - any other `is_open()` state with no prior half-close (an abrupt `Close` —
  ///   a failed dial, an admission-rejected exchange, a never-promoted bridge in
  ///   SynSent): `abort()` (TCP RST) sets the state to Closed in one step, so the
  ///   handle returns to the pool at once with no close handshake and the failed
  ///   exchange's stale tx bytes are discarded rather than flushed — there is
  ///   nothing to deliver over a connection the peer never established.
  ///
  /// A connection still in `PendingDial` (its bridge timed out before a socket
  /// freed) has no socket: removing it is the whole teardown, so a retired
  /// exchange is never later dialed.
  fn teardown(&mut self, eid: memberlist_proto::streams::ExchangeId, now: Instant) {
    // Inspect the connection WITHOUT removing it: a graceful close that still has
    // bytes to deliver must stay mapped (transition to `Closing`) so the egress
    // pump can finish flushing them. Only the paths that complete the teardown
    // this tick remove the connection, and each drops every per-exchange entry
    // for `eid` — parked `out` bytes, the deferred FIN flag, the EOF-delivered
    // flag — in one mutation, so no exchange state outlives a completed `Close`.
    let Some(conn) = self.plane.connections.get(&eid) else {
      return;
    };

    let Some(h) = conn.socket else {
      // PendingDial: no socket was ever assigned, so there is nothing to reclaim
      // and nothing to deliver. Removing the connection is the whole teardown.
      self.plane.connections.remove(&eid);
      return;
    };

    // Whether the connection already half-closed (its graceful FIN was emitted by
    // `flush_pending_shutdowns`), so a still-open socket is parked for the close
    // backstop rather than reset with a RST — and its `out` can no longer be
    // flushed (the tx half is closed).
    let was_half_closed = conn.state == crate::reliable::ConnState::HalfClosed;
    let out_pending = !conn.out_is_empty();

    // Read the TCP state and tx-ring depth once, then drop the socket borrow
    // before touching the connection / pool / closing maps.
    let (state, tx_unacked, may_send) = {
      let sock = self.sockets.get::<tcp::Socket>(h);
      (sock.state(), sock.send_queue(), sock.may_send())
    };

    if matches!(state, tcp::State::Closed | tcp::State::TimeWait) {
      // `!is_open()` is exactly `Closed | TimeWait` per smoltcp `tcp.rs`: both
      // FINs already exchanged (or the socket was already aborted). Reclaim
      // directly, no close handshake left to wait on.
      self.plane.connections.remove(&eid);
      self.plane.pool.give(h);
    } else if was_half_closed {
      // Our graceful FIN is in flight but the peer has not finished the close.
      // Do NOT close()/abort() now: the FIN was already sent and the tx half is
      // closed, so any `out` remainder is undeliverable. Park the handle in
      // `closing` with a `now + close_timeout` deadline so the reap pass reclaims
      // it once it reaches Closed, or force-aborts it at the deadline if the peer
      // vanished mid-FIN.
      self.plane.connections.remove(&eid);
      self.plane.closing.insert(h, now + self.cfg.close_timeout);
    } else if may_send && (out_pending || tx_unacked != 0) {
      // Send-capable (Established / CloseWait) with outbound bytes the peer has
      // NOT yet received — parked in `out` (partial-write backpressure) and/or
      // still unacknowledged in the tx ring. FIN-ing now truncates the reply:
      // `close()` flushes only what is already in the tx ring, never the `out`
      // remainder, and `abort()` would RST over a partial frame. Defer the close:
      // transition to `Closing` (KEEP the connection mapped) with a deadline so
      // `pump_outbound_reliable` keeps draining `out` into the tx ring; the FIN +
      // socket detach happen in `flush_closing` once everything is delivered, or
      // the deadline force-aborts a permanently-backpressured / vanished peer.
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.state = crate::reliable::ConnState::Closing;
        conn.close_deadline = Some(now + self.cfg.close_timeout);
        // Seed the drain-progress mark with the current undelivered count (parked
        // `out` + unacked tx ring). `flush_closing` re-arms `close_deadline` each
        // tick the count shrinks, so `close_timeout` bounds a STALL, not the total
        // drain — a slow-but-progressing peer is never truncated.
        conn.close_drain_mark = conn.out_bytes() + tx_unacked;
        // A still-pending Shutdown FIN is subsumed by the Closing drain's own
        // terminal FIN; clear it so `flush_pending_shutdowns` does not also act.
        conn.fin_pending = false;
      }
    } else if may_send {
      // Send-capable with nothing left to deliver (`out` empty AND tx ring fully
      // acknowledged): an acceptor whose reply already reached the wire, or an
      // Established one-shot teardown with an empty ring. Emit the graceful FIN
      // immediately — `close()` (CloseWait → LastAck, Established → FinWait1)
      // sends our FIN, giving the peer a clean EOF so its initiator commits the
      // response — and park the handle in `closing` for the reap backstop.
      self.plane.connections.remove(&eid);
      self.sockets.get_mut::<tcp::Socket>(h).close();
      self.plane.closing.insert(h, now + self.cfg.close_timeout);
    } else {
      // Abrupt teardown: a graceful `Close` whose socket is not send-capable and
      // never half-closed (e.g. a connection still in SynSent / never promoted).
      // FAILED exchanges no longer reach here — they arrive via
      // `StreamAction::Abort` → `abort_exchange` — but a graceful `Close` over a
      // socket the peer never established is handled defensively the same way:
      // RST and reclaim at once. `abort()` sets the state to Closed immediately,
      // so reuse is safe without waiting for a close handshake, and any stale tx
      // bytes are discarded rather than flushed — there is nothing to deliver
      // over a connection the peer never established.
      self.plane.connections.remove(&eid);
      self.sockets.get_mut::<tcp::Socket>(h).abort();
      self.plane.pool.give(h);
    }
  }

  /// Complete the deferred terminal close of every connection draining in
  /// [`crate::reliable::ConnState::Closing`].
  ///
  /// A graceful `StreamAction::Close` whose send-capable socket still held
  /// undelivered outbound bytes does NOT FIN on the spot — that would truncate a
  /// push/pull reply (or request) larger than the tx ring, whose remainder is
  /// parked in the connection's `out`. `teardown` instead moves it to `Closing`
  /// and leaves it mapped so `pump_outbound_reliable` keeps flushing `out` into
  /// the tx ring across ticks. This pass, run each tick right after that pump,
  /// drives each `Closing` connection to completion:
  ///
  /// - **Drained** — `out` is empty AND the tx ring is fully acknowledged
  ///   (`send_queue() == 0`): every byte reached the peer. Emit the terminal FIN
  ///   (`close()`, closing only the transmit half — CloseWait → LastAck or
  ///   Established → FinWait1) so the peer reads a clean EOF and commits the full
  ///   response, remove the `Connection` (dropping its remaining per-exchange
  ///   state), and park the detached handle in `closing` with a fresh
  ///   `now + close_timeout` deadline for the reap pass to reclaim once the close
  ///   completes.
  /// - **Deadline elapsed** — `now >= close_deadline` while bytes are still
  ///   undelivered (the peer stopped draining the tx ring: permanent backpressure
  ///   or a vanished peer): force-`abort()` the socket (RST → Closed), remove the
  ///   `Connection`, and return the handle straight to the pool. The drain is
  ///   best-effort and bounded; it must never wedge a pooled socket.
  /// - Otherwise the connection is still draining within its deadline — leave it
  ///   mapped for a later tick.
  ///
  /// The `Closing` deadline is folded into `poll()`'s returned wakeup (alongside
  /// the `closing`-map deadlines) so a deadline-driven caller wakes in time to
  /// run this abort backstop by `close_timeout`.
  fn flush_closing(&mut self, now: Instant) {
    // Classify each Closing connection without holding the `connections` borrow
    // across the mutating socket / pool / closing-map calls below.
    enum Outcome {
      /// Drained: emit the FIN and park the handle in `closing`.
      Fin(SocketHandle),
      /// No drain progress for the full `close_timeout`: abort and reclaim.
      Abort(SocketHandle),
      /// The drain made progress this tick (the peer acked bytes): re-arm the
      /// idle deadline with the new undelivered mark.
      Progress(usize),
    }

    let mut actions: std::vec::Vec<(ExchangeId, Outcome)> = std::vec::Vec::new();
    for (&eid, conn) in self.plane.connections.iter() {
      if conn.state != crate::reliable::ConnState::Closing {
        continue;
      }
      // A Closing connection always has an assigned socket (it reached
      // Established / CloseWait before the close); skip defensively if not.
      let Some(h) = conn.socket else { continue };
      // Undelivered = bytes still parked in `out` plus bytes in the tx ring the
      // peer has not yet acked. It only ever shrinks during a close (no new bytes
      // are queued once Closing), and shrinks ONLY when the peer acks — so a
      // shrink is the peer-liveness signal. `close_timeout` therefore bounds the
      // time since the peer last acked (a stall), not the total drain duration:
      // a slow-but-progressing peer re-arms the deadline every tick it acks.
      let undelivered = conn.out_bytes() + self.sockets.get::<tcp::Socket>(h).send_queue();
      if undelivered == 0 {
        actions.push((eid, Outcome::Fin(h)));
      } else if undelivered < conn.close_drain_mark {
        actions.push((eid, Outcome::Progress(undelivered)));
      } else if conn.close_deadline.is_some_and(|d| now >= d) {
        // No progress for the full `close_timeout`: the peer stalled / vanished.
        // Give up on the remainder and reclaim the socket so the pool cannot wedge.
        actions.push((eid, Outcome::Abort(h)));
      }
    }

    for (eid, outcome) in actions {
      match outcome {
        Outcome::Fin(h) => {
          // Every byte was delivered: FIN the transmit half so the peer reads a
          // clean EOF, then park for the reap backstop.
          self.plane.connections.remove(&eid);
          self.sockets.get_mut::<tcp::Socket>(h).close();
          self.plane.closing.insert(h, now + self.cfg.close_timeout);
        }
        Outcome::Abort(h) => {
          // Idle deadline elapsed: RST and reclaim at once.
          self.plane.connections.remove(&eid);
          self.sockets.get_mut::<tcp::Socket>(h).abort();
          self.plane.pool.give(h);
        }
        Outcome::Progress(mark) => {
          // Re-arm the idle deadline from `now`; keep the connection mapped so the
          // egress pump keeps draining.
          if let Some(conn) = self.plane.connections.get_mut(&eid) {
            conn.close_drain_mark = mark;
            conn.close_deadline = Some(now + self.cfg.close_timeout);
          }
        }
      }
    }
  }

  /// Drain each active connection's socket rx buffer into the machine, and
  /// deliver a one-shot EOF once the peer's FIN has been received and drained.
  ///
  /// For every connection in `connections` that has an assigned socket, reads
  /// available bytes from the smoltcp TCP socket and feeds them to
  /// `handle_transport_data`. The peer's FIN is signalled by `recv_slice`
  /// returning `Err(tcp::RecvError::Finished)` once the FIN has been received AND
  /// the receive buffer is fully drained; smoltcp returns it from ANY post-FIN
  /// state — `CloseWait` for a connection whose own send half is still open (an
  /// acceptor finishing its reply), and `FinWait2` / `TimeWait` for one already
  /// in [`crate::reliable::ConnState::HalfClosed`] (a push/pull initiator that
  /// emitted its graceful FIN in `flush_pending_shutdowns` and stays mapped to
  /// receive the reply). The EOF is delivered exactly once per connection —
  /// `Connection::eof_delivered` gates it so the bridge FSM receives a single
  /// half-close signal.
  ///
  /// A connection still in `PendingDial` has no socket, so it is skipped (there
  /// is nothing to receive on a dial that has not even been issued).
  ///
  /// The drain loop calls `recv_slice` unconditionally rather than gating on
  /// `can_recv()`: an empty rx buffer is NOT proof there is nothing to deliver —
  /// the peer's drained FIN surfaces only as the `Finished` return, never as
  /// readable bytes, so a `can_recv()`-gated loop would skip `recv_slice` on an
  /// already-empty FinWait/CloseWait socket and never deliver the EOF. Letting
  /// `recv_slice` itself classify each call keeps the FIN authoritative:
  /// `Ok(n>0)` is data, `Ok(0)` an empty Established ring (nothing this tick),
  /// `Finished` the drained peer FIN (EOF), and `InvalidState` a not-yet-
  /// receivable socket (still handshaking: SynSent / SynReceived) — so no
  /// spurious EOF is delivered before the handshake completes.
  fn pump_inbound_reliable(&mut self, now: Instant) {
    // Scratch buffer for one read. 4 KiB matches the default socket rx ring
    // size; reads chunk that size at most, and the machine reassembles frames
    // across multiple calls to handle_transport_data.
    const READ_BUF: usize = 4096;
    let mut buf = [0u8; READ_BUF];

    // Collect the active (eid, handle) pairs first to avoid holding a
    // `&connections` borrow across the mutable `sockets` + `endpoint` calls.
    // PendingDial connections (no socket) contribute no pair.
    let pairs: std::vec::Vec<_> = self
      .plane
      .connections
      .iter()
      .filter_map(|(&eid, c)| c.socket.map(|h| (eid, h)))
      .collect();

    for (eid, h) in pairs {
      // Drain the socket: read all buffered bytes, then observe a drained peer
      // FIN as `Finished`. `recv_slice` is called unconditionally (not gated on
      // `can_recv()`) so the FIN-only case — empty buffer, peer FIN received — is
      // never skipped; see the method doc.
      loop {
        let sock = self.sockets.get_mut::<tcp::Socket>(h);
        match sock.recv_slice(&mut buf) {
          Ok(0) => {
            // Established with an empty ring (no data this tick) — and no FIN, or
            // `Finished` would have been returned instead. Nothing to drain.
            break;
          }
          Ok(n) => {
            self
              .endpoint
              .handle_transport_data(eid, &buf[..n], false, now);
          }
          Err(tcp::RecvError::Finished) => {
            // Peer FIN received and the receive buffer is fully drained (from any
            // post-FIN state: CloseWait, or FinWait2 / TimeWait for a connection
            // already in HalfClosed). Deliver exactly one EOF to the machine,
            // gated by the connection's `eof_delivered` flag.
            if let Some(conn) = self.plane.connections.get_mut(&eid) {
              if !conn.eof_delivered {
                conn.eof_delivered = true;
                self.endpoint.handle_transport_data(eid, &[], true, now);
              }
            }
            break;
          }
          Err(tcp::RecvError::InvalidState) => {
            // Socket is not in a receivable state (e.g. still handshaking
            // on a just-dialled connection). Nothing to deliver this tick.
            break;
          }
        }
      }
    }
  }

  /// Flush partially-written outbound TCP bytes and drain new transport
  /// transmits from the machine.
  ///
  /// Outbound bytes are written to the connection's socket tx ring via
  /// `send_slice`. Because the ring has finite capacity, `send_slice` may accept
  /// fewer bytes than offered (partial write); the unwritten remainder stays at
  /// the front of the connection's `out` queue (oldest-first). Each tick this
  /// method:
  ///
  /// 1. **Appends new transmits** — calls `poll_transport_transmit()` until
  ///    `None`, pushing each `(eid, _peer, bytes)` onto the matching
  ///    connection's `out` queue (preserving emission order). A connection in
  ///    `PendingDial` parks them too — the machine emits a push/pull's request
  ///    bytes the same tick as its dial, which for a pool-exhausted dial is
  ///    before any socket exists; the bytes flush once `drain_pending_dials`
  ///    assigns a socket. Bytes for an exchange with no connection (torn down)
  ///    are dropped.
  /// 2. **Flushes each connection's `out`** — for every connection whose socket
  ///    is past the handshake, drains its `out` front-to-back via `send_slice`,
  ///    stopping at the first partial write so the unsent tail stays at the front
  ///    and per-exchange byte order is preserved. This includes a connection in
  ///    [`crate::reliable::ConnState::Closing`]: a graceful close that still had
  ///    buffered bytes stays mapped specifically so this pass keeps flushing them
  ///    until they are all delivered, which is what `flush_closing` then waits on
  ///    before emitting the terminal FIN — the drain-before-close guarantee.
  ///
  /// Appending before flushing (rather than writing new bytes directly) keeps a
  /// single ordered queue per connection: new bytes can never overtake an older
  /// parked remainder, and they still reach the tx ring this same tick via the
  /// flush pass below.
  ///
  /// # Writing to a still-opening socket
  ///
  /// A freshly dialled socket is in `SynSent` until its handshake completes, and
  /// the machine commonly hands the push/pull initiator's first bytes in the
  /// SAME tick the dial opens — before the socket is writable. smoltcp's
  /// `send_slice` rejects writes with `InvalidState` until Established, so the
  /// flush pass skips a connection whose socket is still opening (or a
  /// `PendingDial` connection with no socket at all); doing the write and
  /// treating the rejection as "tx half closed" would silently drop the entire
  /// push/pull half and wedge the join. The bytes stay in `out` and retry each
  /// tick until the socket reaches Established. A genuine `InvalidState` on a
  /// closing socket (FinWait*/Closing/LastAck) drops the remainder — that tx half
  /// is really gone.
  fn pump_outbound_reliable(&mut self) {
    // --- Pass 1: append new transmits to their connection's out queue ---
    while let Some((eid, _peer, bytes)) = self.endpoint.poll_transport_transmit() {
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        // Park in order regardless of state: an Established connection's bytes
        // are written by the flush pass below this same tick; a Dialing /
        // PendingDial connection holds them until its socket is writable.
        conn.out.push_back(bytes);
      }
      // Otherwise no connection: the exchange was torn down before these bytes
      // arrived, so they are dropped — the exchange is dead.
    }

    // --- Pass 2: flush each connection's out queue to its socket ---
    //
    // Collect the active (eid, handle) pairs first so the `connections` borrow
    // is released before the mutable `sockets` access inside the loop.
    let pairs: std::vec::Vec<_> = self
      .plane
      .connections
      .iter()
      .filter_map(|(&eid, c)| {
        // A PendingDial connection (no socket) or one with an empty queue has
        // nothing to flush this tick.
        if c.out.is_empty() {
          return None;
        }
        c.socket.map(|h| (eid, h))
      })
      .collect();

    for (eid, h) in pairs {
      // Still handshaking (the common push/pull-initiator case: first bytes
      // arrive the same tick as the dial): leave the queue parked and retry once
      // the socket is Established.
      if socket_still_opening(self.sockets.get::<tcp::Socket>(h).state()) {
        continue;
      }
      // Drain front-to-back. Stop at the first partial write so the unsent tail
      // stays at the front of the queue and later entries are not reordered.
      while let Some(front) = self
        .plane
        .connections
        .get(&eid)
        .and_then(|c| c.out.front().cloned())
      {
        let sock = self.sockets.get_mut::<tcp::Socket>(h);
        match sock.send_slice(&front) {
          Ok(sent) if sent >= front.len() => {
            // Fully written — pop it and continue with the next buffer.
            if let Some(conn) = self.plane.connections.get_mut(&eid) {
              conn.out.pop_front();
            }
          }
          Ok(sent) => {
            // Partial write — replace the front with its unsent tail and stop
            // flushing this connection (the tx ring is full this tick).
            if let Some(conn) = self.plane.connections.get_mut(&eid) {
              if let Some(slot) = conn.out.front_mut() {
                *slot = front.slice(sent..);
              }
            }
            break;
          }
          Err(_) => {
            // SendError::InvalidState on a non-opening socket: the tx half is
            // closed (exchange is being torn down). Drop the whole queue.
            // Ignoring Err: the exchange is being torn down; bytes are no longer
            // deliverable.
            if let Some(conn) = self.plane.connections.get_mut(&eid) {
              conn.out.clear();
            }
            break;
          }
        }
      }
    }
  }

  /// Promote each `Dialing` connection whose TCP handshake has completed to
  /// `Established`.
  ///
  /// A connection is created `Dialing` (socket assigned, SynSent) and stays so
  /// while the three-way handshake is in flight. Once the socket can send
  /// (`may_send()` — Established, and also CloseWait if the peer FIN'd before we
  /// did), the connection is writable: its parked `out` flushes and a deferred
  /// FIN may fire. Recording that as `ConnState::Established` makes the FIN gate
  /// in `flush_pending_shutdowns` a precise `state == Established` check rather
  /// than re-deriving readiness from the socket, and keeps `ConnState` an honest
  /// reflection of the lifecycle. `PendingDial` connections (no socket) and ones
  /// already `Established`/`HalfClosed` are left as-is.
  fn promote_established(&mut self) {
    let promote: std::vec::Vec<ExchangeId> = self
      .plane
      .connections
      .iter()
      .filter(|(_, c)| c.state == crate::reliable::ConnState::Dialing)
      .filter_map(|(&eid, c)| c.socket.map(|h| (eid, h)))
      .filter(|&(_, h)| self.sockets.get::<tcp::Socket>(h).may_send())
      .map(|(eid, _)| eid)
      .collect();
    for eid in promote {
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.state = crate::reliable::ConnState::Established;
      }
    }
  }

  /// Emit deferred graceful write-half FINs for connections whose socket can now
  /// carry one losslessly — KEEPING the connection mapped so its inbound reply
  /// still pumps.
  ///
  /// `StreamAction::Shutdown` is the machine's SEND-half close signal: the local
  /// side finished sending (a push/pull initiator wrote its full request), but
  /// the bridge is STILL awaiting the peer's reply and/or FIN. It sets the
  /// connection's `fin_pending` flag rather than closing the socket on the spot
  /// (see the `Shutdown` arm of `drain_stream_actions`). This pass, run each tick
  /// after the outbound byte pump, promotes a parked FIN to an actual `close()`
  /// once ALL of:
  ///
  /// - the connection is `Established` (its handshake completed, so `close()`
  ///   issues a real FIN instead of the SynSent/Listen abort that would discard
  ///   buffered bytes); AND
  /// - the tx ring has fully drained and been acknowledged (`send_queue() == 0`)
  ///   and no remainder is parked in the connection's `out`, so every push/pull
  ///   byte has reached the peer before the FIN.
  ///
  /// When all hold, `close()` issues the FIN (closing only the TRANSMIT half) and
  /// the connection transitions `Established → HalfClosed` — but it STAYS in
  /// `connections`. This is the half-close correctness invariant: a FinWait
  /// socket still receives, so the connection must remain mapped for
  /// `pump_inbound_reliable` to drain the peer's later reply and FIN (EOF) into
  /// the bridge. Detaching here (removing the connection / parking the handle in
  /// `closing`) would strand the peer's reply — the peer normally ACKs the
  /// request in a separate segment BEFORE sending its reply, so the FIN fires
  /// first — and the exchange would time out at its bridge deadline despite a
  /// valid response. The socket is reclaimed only later, by the
  /// `StreamAction::Close` the machine emits once the bridge reaches `BothClosed`
  /// (peer replied + FIN) or its exchange deadline elapses (peer vanished); see
  /// `teardown`.
  ///
  /// Emitting exactly once is structural: the transition to `HalfClosed` clears
  /// the connection from the `Established` set this pass selects, and resets
  /// `fin_pending`. A connection still `Dialing`/`PendingDial` (socket not yet
  /// writable) keeps its `fin_pending` flag and fires on a later tick once it
  /// reaches `Established`; the machine-issued abrupt `Close` on the bridge's
  /// deadline bounds the wait.
  fn flush_pending_shutdowns(&mut self) {
    // Collect the connections ready to emit their FIN: `fin_pending` set, in
    // `Established`, socket fully drained and acknowledged, `out` empty. The
    // `connections` borrow is released before the mutation below.
    let ready: std::vec::Vec<_> = self
      .plane
      .connections
      .iter()
      // `fin_pending` requested, handshake complete (Established), and no
      // outbound bytes still parked in `out`.
      .filter(|(_, c)| {
        c.fin_pending && c.state == crate::reliable::ConnState::Established && c.out_is_empty()
      })
      .filter_map(|(&eid, c)| c.socket.map(|h| (eid, h)))
      // …and the socket's tx ring is fully drained and acknowledged, so every
      // push/pull byte reached the peer before the FIN.
      .filter(|&(_, h)| {
        let sock = self.sockets.get::<tcp::Socket>(h);
        sock.may_send() && sock.send_queue() == 0
      })
      .collect();

    for (eid, h) in ready {
      // Socket is Established with an empty, acknowledged tx ring: issue the
      // graceful FIN on the TRANSMIT half only. The connection stays in
      // `connections` so the peer's reply + FIN still pump inbound; the
      // transition to HalfClosed (and clearing `fin_pending`) records the FIN is
      // sent so a later flush tick does not `close()` twice, and the eventual
      // `StreamAction::Close` reclaims the socket.
      self.sockets.get_mut::<tcp::Socket>(h).close();
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.fin_pending = false;
        conn.state = crate::reliable::ConnState::HalfClosed;
      }
    }
  }

  /// Drain all outbound gossip transmits from the machine, encode each one
  /// with the shared no-std codec, and write it to the UDP socket.
  ///
  /// A single-message transmit (`Transmit::Packet`) is encoded as a plain
  /// frame; a multi-message batch (`Transmit::Compound`) is encoded as a
  /// compound frame. Encoding errors and a full tx ring both silently drop
  /// the datagram — gossip is best-effort and SWIM recovers on the next round.
  fn drain_gossip_transmits(&mut self) {
    let enc = memberlist_proto::codec::EncodeOptions::new(None);
    while let Some(transmit) = self.endpoint.poll_memberlist_transmit() {
      let (dest, bytes) = match encode_transmit::<I>(transmit, &enc) {
        Some(pair) => pair,
        None => continue,
      };
      // Apply the cross-transport transforms to the encoded frame before it
      // hits the wire: compress, then encrypt, so the on-wire byte order is
      // `[Encrypted[Compressed[frame]]]`. Both are identity when disabled, so a
      // default `TransformOptions` sends the same plaintext frame as before.
      // Staged into owned `Vec`s here so the `&self.endpoint` transform borrows
      // end before the disjoint `&mut self.sockets` send below.
      let compressed = self.endpoint.compress_gossip(&bytes);
      let on_wire = match self.endpoint.encrypt_gossip(&compressed) {
        Ok(b) => b,
        // Encryption is configured but the backend rejected the request (e.g. a
        // primary key whose AEAD algorithm was not built into this binary).
        // Drop: emitting the plaintext frame on an encrypted-cluster path would
        // silently bypass authentication. Gossip is lossy and self-healing.
        Err(_) => continue,
      };
      // Last-line egress drop: a non-routable destination (unspecified/
      // multicast/broadcast IP or port 0) is screened here so no bad address from
      // ANY source (gossip, push/pull, config) reaches the UDP socket. smoltcp's
      // `send_slice` would itself reject the unspecified address and port 0 with
      // `Unaddressable` (a silent per-datagram drop); skipping it up front is a
      // clean drop on the same predicate smoltcp's route / neighbor lookup
      // asserts, and gossip is lossy so SWIM recovers regardless.
      if !endpoint_is_routable(&dest) {
        continue;
      }
      let sock = self.sockets.get_mut::<udp::Socket>(self.udp);
      // Ignoring Err: gossip is best-effort — a full or errored UDP tx ring
      // drops this datagram and SWIM recovers on the next gossip round.
      let _ = sock.send_slice(&on_wire, to_endpoint(dest));
    }
  }
}

/// Whether a TCP socket is still completing its three-way handshake.
///
/// `SynSent` (we dialled, awaiting SYN-ACK) and `SynReceived` (we sent the
/// SYN-ACK, awaiting the final ACK) are the two states in which the connection
/// is opening: it has a populated 4-tuple but is not yet writable (`may_send()`
/// is false). smoltcp's `tcp::Socket::send_slice` rejects writes with
/// `SendError::InvalidState` until the socket reaches Established, so the
/// outbound pump must HOLD bytes for an opening socket (re-trying next tick)
/// rather than treating the rejection as an unrecoverable closed tx half. Every
/// other `!may_send()` state (FinWait*/Closing/LastAck/TimeWait/Closed) is a
/// genuinely closed/closing tx half whose bytes are undeliverable.
fn socket_still_opening(state: tcp::State) -> bool {
  matches!(state, tcp::State::SynSent | tcp::State::SynReceived)
}

/// Returns the earlier of two optional deadlines. If only one is `Some`, that
/// deadline wins; if both are `None` the result is `None`.
fn min_opt(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
  match (a, b) {
    (Some(x), Some(y)) => Some(core::cmp::min(x, y)),
    (x, y) => x.or(y),
  }
}

/// Encode one outbound gossip transmit using the shared no-std codec.
///
/// Returns `(dest, encoded_bytes)` on success, or `None` if encoding fails
/// (in which case the caller silently skips the datagram — gossip is lossy).
///
/// - `Transmit::Packet` → plain frame (single message).
/// - `Transmit::Compound` → compound frame (two or more messages piggybacked).
fn encode_transmit<I>(
  t: Transmit<I, SocketAddr>,
  enc: &memberlist_proto::codec::EncodeOptions,
) -> Option<(SocketAddr, bytes::Bytes)>
where
  I: memberlist_proto::Data,
{
  match t {
    Transmit::Packet(pkt) => {
      let (to, msg) = pkt.into_parts();
      let bytes = memberlist_proto::codec::encode_outgoing(&msg, enc).ok()?;
      Some((to, bytes))
    }
    Transmit::Compound(cmp) => {
      let (to, msgs) = cmp.into_parts();
      let bytes = memberlist_proto::codec::encode_outgoing_compound(&msgs, enc).ok()?;
      Some((to, bytes))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{HardwareAddress, InterfaceConfig, IpCidr};
  use core::net::{IpAddr, Ipv4Addr, SocketAddr};
  use smol_str::SmolStr;

  fn addr(p: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), p)
  }

  fn ip_iface() -> InterfaceConfig {
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(IpCidr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)).into(),
      24,
    ))
  }

  #[test]
  fn new_node_is_sole_member() {
    let cfg = crate::Config::new();
    let ep_cfg =
      memberlist_proto::EndpointConfig::new(SmolStr::new("a"), addr(7946)).with_rng_seed(1);
    let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
    let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_secs(1));
    let m: Memberlist<SmolStr, _> = Memberlist::new(
      cfg,
      ip_iface(),
      TransformOptions::default(),
      ep_cfg,
      &mut dev,
      now,
    );
    assert_eq!(m.num_members(), 1);
  }

  #[test]
  fn poll_emits_initial_gossip_and_a_deadline() {
    let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
    let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_secs(1));
    let ep_cfg =
      memberlist_proto::EndpointConfig::new(SmolStr::new("a"), addr(7946)).with_rng_seed(1);
    let mut m: Memberlist<SmolStr, _> = Memberlist::new(
      crate::Config::new(),
      ip_iface(),
      TransformOptions::default(),
      ep_cfg,
      &mut dev,
      now,
    );
    m.start(now);
    let next = m.poll(now, &mut dev);
    assert!(next.is_some(), "scheduler must arm a deadline");
  }

  #[test]
  fn endpoint_is_routable_matches_smoltcp_unicast() {
    use core::net::Ipv6Addr;

    // Unicast IPv4 with a non-zero port is the only routable case here.
    assert!(endpoint_is_routable(&addr(7946)));
    // Unicast IPv6 is routable too.
    assert!(endpoint_is_routable(&SocketAddr::new(
      IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
      7946
    )));

    // Unspecified (0.0.0.0 / ::) is not unicast.
    assert!(!endpoint_is_routable(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::UNSPECIFIED),
      7946
    )));
    assert!(!endpoint_is_routable(&SocketAddr::new(
      IpAddr::V6(Ipv6Addr::UNSPECIFIED),
      7946
    )));
    // Multicast (224.0.0.1) is not unicast.
    assert!(!endpoint_is_routable(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
      7946
    )));
    // Limited broadcast (255.255.255.255) is not unicast.
    assert!(!endpoint_is_routable(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
      7946
    )));
    // Port 0 is rejected even with a unicast IP: no socket can address it.
    assert!(!endpoint_is_routable(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
      0
    )));
  }
}
