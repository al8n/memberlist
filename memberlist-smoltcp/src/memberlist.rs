//! Memberlist handle: construction, accessors, and the caller-owned poll loop.

use core::{cell::RefCell, net::SocketAddr};

use memberlist_embedded::Engine;
use memberlist_proto::{EndpointConfig, Instant, Node, StreamId, event::PingId, typed::NodeState};
use smoltcp::{
  iface::{Config as IfConfig, Interface, SocketHandle, SocketSet},
  phy::Device,
  socket::{tcp, udp},
};

use crate::{
  Config, InitError, InterfaceConfig, TransformOptions,
  addr::{from_smoltcp_instant, to_endpoint, to_smoltcp_instant},
  error::{GossipMtuTooLarge, MediumMismatch},
  gossip_io::SmoltcpGossip,
  interface::{HardwareAddress, Medium},
  stream_io::SmoltcpStream,
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
/// This is the construction-time validity predicate for the advertise address (the
/// engine screens every egress address it touches with its own transport-neutral
/// `socket_addr_is_routable`, the same `!(broadcast || multicast || unspecified)`
/// plus `port != 0` test). `to_endpoint(*addr).addr.is_unicast()` calls smoltcp's
/// OWN `IpAddress::is_unicast` — the exact function those assertions use — so the
/// driver and smoltcp agree byte-for-byte on what "routable" means.
pub(crate) fn endpoint_is_routable(addr: &SocketAddr) -> bool {
  to_endpoint(*addr).addr.is_unicast() && addr.port() != 0
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
/// super-loop. Construction (`new`) binds the gossip UDP socket, allocates the
/// reliable-plane TCP socket pool, and wires up the transport-agnostic
/// [`Engine`]; no I/O occurs here. Each `poll` ticks the smoltcp stack, then
/// drives the engine over a [`SmoltcpGossip`] + [`SmoltcpStream`] view of the
/// just-ticked sockets — so all protocol work lives in the shared engine and this
/// driver supplies only the link layer.
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
  /// The transport-agnostic driving core: the SWIM machine, the reliable-plane
  /// connection state machine and its `SocketHandle` pool, the gossip codec
  /// pipeline, and the join-seed queue. The driver owns only the smoltcp sockets;
  /// the engine owns everything protocol-shaped and is driven each `poll` over a
  /// view of those sockets.
  engine: Engine<I, SocketHandle>,
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
  /// socket and the reliable-plane TCP socket pool, and wires up the
  /// transport-agnostic [`Engine`] over the machine's `Endpoint`. No I/O occurs
  /// here.
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
  ///   encryption keyring is probed by the engine (see Errors); the default is
  ///   fully disabled and unlabelled.
  /// - `ep_cfg`: machine identity (`id`, `advertise`, timing knobs, …).
  /// - `device`: the smoltcp [`Device`] the interface is bound to. Its medium
  ///   must match the one implied by `iface.hardware_addr`.
  /// - `now`: the driver's clock reading at construction (passed to the
  ///   `Interface` and the engine so timers start from a consistent origin; see
  ///   `addr::to_smoltcp_instant` / `Engine::try_new_at`).
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
  ///   a key whose AEAD backend was not compiled into this binary (probed by the
  ///   engine by encrypting an empty frame with each key), so encrypted gossip
  ///   would otherwise be silently dropped at runtime.
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
    // runtime-supplied zero port. (The engine also rejects a zero port, but the
    // driver binds its sockets before constructing the engine, so screen here
    // first.) Validate before allocating any sockets.
    if cfg.port == 0 {
      return Err(InitError::ZeroPort);
    }

    // Reject a gossip MTU whose on-wire datagram cannot fit a UDP packet. The
    // UDP arenas are sized from `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD` (the
    // largest on-wire datagram the machine can emit); an over-ceiling `gossip_mtu`
    // would overflow that addition — a panic in a checked build, a wrap to an
    // undersized arena in release that then silently truncates in-budget gossip.
    // Bounding it here makes every downstream `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`
    // safe and mirrors the async drivers' reject-not-clamp doctrine. (The engine
    // re-validates it too; the driver needs it before sizing the UDP arenas.) Done
    // before any UDP allocation.
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
    // in-flight push/pull response is truncated. (The engine also rejects it; the
    // driver screens it here with the rest of its pure-`Config` checks.)
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

    // Reject a non-routable advertise address BEFORE the not-local check below. A
    // node must advertise an address its peers can route a reply to; an
    // unspecified/multicast/broadcast IP or port 0 would be gossiped cluster-wide
    // and then be useless to every peer that selected it as an egress destination
    // (smoltcp's socket layer rejects it as `Unaddressable` / its route lookup
    // asserts unicast). The engine re-checks this (its `try_new_at` rejects a
    // non-routable advertise), but the driver's `has_ip_addr` not-local check below
    // runs before the engine is built and would otherwise mask the unspecified
    // address as `AdvertiseAddrNotLocal` (`0.0.0.0` is never an assigned address).
    // Screen on the same `is_unicast` predicate here so a non-routable advertise is
    // the precise `NonRoutableAdvertiseAddr` regardless of the interface's
    // addresses.
    if !endpoint_is_routable(ep_cfg.advertise_addr_ref()) {
      return Err(InitError::NonRoutableAdvertiseAddr(
        *ep_cfg.advertise_addr_ref(),
      ));
    }

    // The advertised IP must be one the interface actually holds. smoltcp's
    // ingress drops any packet whose destination is not an assigned address, so
    // a node advertising an IP the interface lacks is unreachable on both planes
    // — peers gossip and dial an address its own interface discards. Check with
    // smoltcp's own `has_ip_addr`, the exact predicate that gates ingress in
    // `process_ipv4` / `process_ipv6`. (This and the routable screen above are the
    // advertise-address checks that need the interface in scope; the port-match
    // check lives in the engine.)
    let advertised_ip = to_endpoint(*ep_cfg.advertise_addr_ref()).addr;
    if !iface.has_ip_addr(advertised_ip) {
      return Err(InitError::AdvertiseAddrNotLocal(
        *ep_cfg.advertise_addr_ref(),
      ));
    }

    // Build the engine from the driver's port / timeout config. NOTE the `Config`
    // name collision — the driver's `crate::Config` carries link-layer sizing
    // (socket buffers, UDP arenas, `tcp_pool_size`) that stays on the driver,
    // while `memberlist_embedded::Config` carries only the port and close timeout
    // the engine reads directly. `try_new_at` (not `new_at`) so a machine entropy
    // failure, an unusable encryption keyring, or a non-routable / port-mismatched
    // advertise address becomes a typed `InitError` rather than a panic. The
    // engine installs the routable-address admission filter and the
    // compression/encryption/label transforms internally.
    let embedded_cfg = memberlist_embedded::Config::new()
      .with_port(cfg.port)
      .with_close_timeout(cfg.close_timeout);
    let mut engine =
      Engine::try_new_at(embedded_cfg, transform, ep_cfg, now).map_err(InitError::from_embedded)?;

    // Allocate pooled TCP sockets for the reliable plane and register their
    // handles with the engine's reliable plane (the pool authority). Each socket
    // gets independent rx/tx ring buffers sized by the config. One socket is
    // immediately moved into listen state and installed as the engine's listener;
    // the rest stay free for outbound dials and accepted inbound connections.
    for _ in 0..cfg.tcp_pool_size {
      let rx = tcp::SocketBuffer::new(vec![0u8; cfg.tcp_socket_rx_bytes]);
      let tx = tcp::SocketBuffer::new(vec![0u8; cfg.tcp_socket_tx_bytes]);
      engine
        .plane_mut()
        .pool
        .push(sockets.add(tcp::Socket::new(rx, tx)));
    }
    // Dedicate one pooled socket to listening for inbound reliable connections.
    if let Some(h) = engine.plane_mut().pool.take() {
      // `listen` fails only on port 0 (rejected above) or an already-open
      // socket (this one is fresh); propagate rather than `expect` so no panic
      // can escape this fallible constructor even if that invariant changes.
      sockets
        .get_mut::<tcp::Socket>(h)
        .listen(cfg.port)
        .map_err(|_| InitError::ZeroPort)?;
      engine.set_listener(h);
    }

    Ok(Self {
      iface,
      iface_random_seed: random_seed,
      sockets,
      udp,
      engine,
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
    self.engine.accepted_inbound_count()
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
    self.engine.pool_free_count()
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
    self.engine.closing_count()
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
    self.engine.half_closed_count()
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
    self.engine.listener_present()
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
    self.engine.pending_dial_count()
  }

  /// Number of known members, including the local node itself.
  ///
  /// A freshly constructed node has exactly one member (itself). Peers join
  /// after push/pull exchanges or gossip convergence during the poll loop.
  #[inline]
  pub fn num_members(&self) -> usize {
    self.engine.num_members()
  }

  /// Drain one application-visible membership or lifecycle event, if any.
  ///
  /// Returns events emitted by the machine during the last `poll` tick.
  /// Returns `None` when the event queue is empty; call again after the
  /// next `poll` tick.
  #[inline]
  pub fn poll_event(&mut self) -> Option<memberlist_proto::event::Event<I, SocketAddr>> {
    self.engine.poll_event()
  }

  /// Arm the SWIM scheduler at `now`. Call once before the first `poll`.
  ///
  /// Forwards to the engine, which arms the probe, gossip, and push-pull periodic
  /// timers so `poll_timeout` returns a finite deadline on the very next call.
  pub fn start(&mut self, now: Instant) {
    self.engine.start(now);
  }

  /// Seed a statically-known peer as Alive, bootstrapping membership without
  /// the TCP push-pull join path.
  ///
  /// Builds a synthetic `Alive` message for `id` at `peer` (incarnation 1)
  /// and feeds it into the machine, exactly as if the node had been learned
  /// through gossip. Useful for static embedded clusters and for tests that skip
  /// the join phase.
  ///
  /// A non-routable `peer` (unspecified/multicast/broadcast IP or port 0) is
  /// dropped by the engine: it could only be stored as a member no node can send
  /// a useful packet to.
  pub fn inject_alive(&mut self, id: I, peer: SocketAddr, now: Instant) {
    self.engine.inject_alive(id, peer, now);
  }

  /// Whether `id` is currently Alive from this node's perspective.
  ///
  /// Returns `false` for unknown ids or ids in any non-Alive state.
  #[inline]
  pub fn is_alive(&self, id: &I) -> bool {
    self.engine.is_alive(id)
  }

  /// Whether `id` is currently Dead from this node's perspective.
  ///
  /// Returns `false` for unknown ids or ids in any non-Dead state.
  #[inline]
  pub fn is_dead(&self, id: &I) -> bool {
    self.engine.is_dead(id)
  }

  // ── Query accessors ────────────────────────────────────────────────────────
  //
  // Thin forwards to the engine's `&self` reads over the live machine endpoint.
  // Unlike the async drivers (compio, reactor) there is no `ArcSwap` snapshot:
  // reads go directly to the machine state, so they always reflect the result of
  // the last `poll` tick with no snapshot lag. Each `NodeState` the engine returns
  // is already stamped with the live FSM liveness.

  /// Return the `NodeState` for `id`, stamped with the current FSM liveness.
  ///
  /// Returns `None` when `id` is unknown to this node. The `NodeState.state()`
  /// field reflects the live gossip-FSM state (`Alive` / `Suspect` / `Dead` /
  /// `Unknown`), not the frozen wire-format value.
  #[inline]
  pub fn by_id(&self, id: &I) -> Option<std::sync::Arc<NodeState<I, SocketAddr>>> {
    self.engine.by_id(id)
  }

  /// All members currently in the `Alive` FSM state.
  ///
  /// Each returned `NodeState` is stamped with the FSM liveness, so
  /// `online_members()[i].state() == State::Alive` always holds. Consistent
  /// with `is_alive`: if `is_alive(id)` is `true`, `id` appears here.
  #[inline]
  pub fn online_members(&self) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    self.engine.online_members()
  }

  /// Count of members currently in the `Alive` FSM state.
  ///
  /// Equivalent to `online_members().len()` but avoids allocating a `Vec`.
  #[inline]
  pub fn num_online_members(&self) -> usize {
    self.engine.num_online_members()
  }

  /// All known members (Alive + Suspect + Dead/Left), each stamped with the
  /// current FSM liveness.
  ///
  /// Mirrors the legacy `Memberlist::members` name.
  #[inline]
  pub fn members(&self) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    self.engine.members()
  }

  /// Members matching `pred`, each stamped with the current FSM liveness.
  #[inline]
  pub fn members_by(
    &self,
    pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool,
  ) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    self.engine.members_by(pred)
  }

  /// Count of members matching `pred`.
  #[inline]
  pub fn num_members_by(&self, pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool) -> usize {
    self.engine.num_members_by(pred)
  }

  /// Map-filter members, collecting all `Some` results into a `Vec`.
  ///
  /// Each `NodeState` passed to `f` is stamped with the current FSM liveness.
  #[inline]
  pub fn members_map_by<O>(
    &self,
    f: impl FnMut(&NodeState<I, SocketAddr>) -> Option<O>,
  ) -> std::vec::Vec<O> {
    self.engine.members_map_by(f)
  }

  /// The local node's Lifeguard health score (`0` = fully healthy; higher = worse).
  ///
  /// Read directly from the live machine endpoint — no snapshot lag.
  #[inline]
  pub fn health_score(&self) -> usize {
    self.engine.health_score()
  }

  /// The local node's id, cheap-cloned from the machine endpoint.
  #[inline]
  pub fn local_id(&self) -> I {
    self.engine.local_id()
  }

  /// The local node's advertised `SocketAddr`.
  #[inline]
  pub fn advertise_address(&self) -> SocketAddr {
    self.engine.advertise_address()
  }

  /// The local node's `NodeState`, stamped with the current FSM liveness.
  #[inline]
  pub fn local_state(&self) -> std::sync::Arc<NodeState<I, SocketAddr>> {
    self.engine.local_state()
  }

  // ── Directed I/O ──────────────────────────────────────────────────────────
  //
  // Thin forwards to the engine. The poll loop drives all actual I/O; the caller
  // correlates completion by draining `poll_event()` after subsequent `poll`
  // ticks.

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
    self.engine.ping(node, now)
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
    self.engine.send(to, payload)
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
    self.engine.send_many(to, payloads)
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
    self.engine.send_reliable(to, payload, now)
  }

  // ── Join ──────────────────────────────────────────────────────────────────

  /// Record intent to join the cluster via these seed addresses.
  ///
  /// Returns immediately; the poll loop initiates a push/pull state exchange
  /// to each seed on the next tick. The caller should watch `is_joined()` or
  /// drain `poll_event()` for `Event::PushPullReplyReceived` / membership
  /// changes, and enforce its own join deadline — this method performs no I/O
  /// and imposes no timeout. A non-routable seed (unspecified/multicast/broadcast
  /// IP or port 0) is dropped by the engine: it could only produce a doomed dial.
  pub fn join(&mut self, seeds: &[SocketAddr]) {
    self.engine.join(seeds);
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
    self.engine.queue_user_broadcast(data)
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
    self.engine.leave(now)
  }

  /// Whether this node has learned at least one peer.
  ///
  /// `num_members() > 1` means a join push/pull has synced remote state, or a
  /// peer was injected via `inject_alive`. A coarse readiness signal; the
  /// caller owns the real join deadline.
  #[inline]
  pub fn is_joined(&self) -> bool {
    self.engine.is_joined()
  }

  /// Advance both the smoltcp stack and the memberlist state machine once.
  /// Returns the next wakeup deadline: the minimum of the smoltcp stack's next
  /// scheduled event, the machine's next timer, AND any engine-owned deadline
  /// (the soonest gracefully-closing socket's force-abort instant).
  ///
  /// The caller owns the super-loop and is responsible for sleeping until
  /// the returned deadline, advancing the clock, and calling `poll` again.
  /// Because every deadline enforced on a tick is folded into this instant, a
  /// caller that sleeps exactly to it always wakes in time to honor them —
  /// including reclaiming a closing socket by `Config::close_timeout`.
  ///
  /// # Order
  ///
  /// 1. **Stack tick** — `iface.poll` drains the device and services TCP/UDP.
  /// 2. **Engine pump** — the engine runs every protocol phase over a
  ///    [`SmoltcpGossip`] + [`SmoltcpStream`] view of the just-ticked sockets:
  ///    reap closing sockets, accept inbound and replenish the listener,
  ///    rebalance deferred dials, drain UDP gossip ingress through the codec,
  ///    pump reliable ingress, drain join seeds, fire machine timers, then drain
  ///    stream actions and TCP/UDP egress. See
  ///    [`Engine::pump`](memberlist_embedded::Engine::pump) for the full ordered
  ///    phase list.
  /// 3. **Deadline** — fold the smoltcp stack's next scheduled event (`poll_at`)
  ///    into the engine's returned `min(machine, closing)` wakeup, so a caller
  ///    sleeping to the result wakes in time for the stack's retransmit /
  ///    delayed-ACK timers as well as every engine-owned deadline.
  pub fn poll(&mut self, now: Instant, device: &mut D) -> Option<Instant> {
    let s_now = to_smoltcp_instant(now);

    // 1. Stack tick.
    // `PollResult` is ignored — the engine drives its own state over the views
    // below; we do not gate machine work on stack idle/active.
    self.iface.poll(s_now, device, &mut self.sockets);

    // 2. Engine pump over a view of the just-ticked sockets. smoltcp keeps the
    // gossip UDP socket and the reliable-plane TCP pool in ONE `SocketSet`, so the
    // gossip and stream views share mutable access to it through a `RefCell` held
    // for the pump's duration; each view's methods take a brief `borrow_mut` and
    // never hold a socket borrow across a call into the other, so the borrows never
    // overlap. The stream view also borrows the interface (a separate field) for
    // `connect`'s context. The engine's `ReliablePlane` owns the connection pool and
    // lifecycle, reaching the sockets only through these two trait views; the driver
    // no longer holds any of that logic.
    let next = {
      let sockets = RefCell::new(&mut self.sockets);
      let mut gossip = SmoltcpGossip::new(&sockets, self.udp);
      let mut stream = SmoltcpStream::new(&mut self.iface, &sockets);
      self.engine.pump(now, &mut gossip, &mut stream)
    };

    // 3. Fold the smoltcp stack's next scheduled event into the engine's returned
    // deadline. The engine returns only `min(machine, closing)` — the SWIM timers
    // and the soonest gracefully-closing socket's force-abort instant — because it
    // owns no link layer. The driver adds the stack's `poll_at` (retransmit /
    // delayed-ACK / etc.) so a caller sleeping to the result wakes in time for
    // both. `poll_at` is re-read AFTER the pump so it reflects any socket the pump
    // just opened/closed (e.g. a dial whose SYN is queued reports ~now).
    let stack = self
      .iface
      .poll_at(s_now, &self.sockets)
      .map(from_smoltcp_instant);
    min_opt(stack, next)
  }
}

/// Returns the earlier of two optional deadlines. If only one is `Some`, that
/// deadline wins; if both are `None` the result is `None`.
fn min_opt(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
  match (a, b) {
    (Some(x), Some(y)) => Some(core::cmp::min(x, y)),
    (x, y) => x.or(y),
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
