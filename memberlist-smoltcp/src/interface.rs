//! Public configuration for the smoltcp [`Interface`](smoltcp::iface::Interface)
//! the driver binds to.
//!
//! The caller assembles an [`InterfaceConfig`] from smoltcp's own wire and
//! routing types — re-exported here so a downstream crate need not depend on
//! smoltcp directly — and the driver applies it verbatim at construction.

// `Vec` resolves to `std::vec::Vec`, which under `no_std + alloc` is the
// `alloc` crate aliased to `std` in `lib.rs`.
use std::vec::Vec;

pub use smoltcp::{
  iface::Route,
  phy::Medium,
  wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address, Ipv6Address},
};

/// How to configure the smoltcp [`Interface`](smoltcp::iface::Interface) the
/// driver binds to.
///
/// The hardware address implies the medium and is validated against the bound
/// device at construction (a mismatch is a typed
/// [`InitError::MediumMismatch`](crate::InitError::MediumMismatch), not a
/// panic). The IP addresses and routes are applied verbatim. The random seed
/// defaults to system entropy — recommended, so smoltcp's TCP initial sequence
/// number and ephemeral port selection cannot repeat across reboots — and may
/// be pinned to a fixed value for deterministic tests.
///
/// Build it with [`new`](Self::new) plus the repeatable `with_*` setters:
///
/// ```
/// use memberlist_smoltcp::{HardwareAddress, InterfaceConfig, IpAddress, IpCidr};
///
/// let cfg = InterfaceConfig::new(HardwareAddress::Ip)
///   .with_ip_addr(IpCidr::new(IpAddress::v4(10, 0, 0, 1), 24));
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct InterfaceConfig {
  /// The interface's hardware address. Its variant selects the medium:
  /// [`HardwareAddress::Ip`] is `Medium::Ip`, [`HardwareAddress::Ethernet`] is
  /// `Medium::Ethernet`. An Ethernet address must be unicast (smoltcp rejects a
  /// multicast/broadcast source MAC).
  pub hardware_addr: HardwareAddress,
  /// The unicast IP addresses (with prefix lengths) the interface accepts
  /// packets for. Must be non-empty — an interface with no address would
  /// silently drop everything. Bounded by smoltcp's `IFACE_MAX_ADDR_COUNT`.
  pub ip_addrs: Vec<IpCidr>,
  /// The interface's routing table (e.g. a default gateway for off-link peers).
  /// Empty is fine for an all-on-link cluster. Bounded by smoltcp's
  /// `IFACE_MAX_ROUTE_COUNT`.
  pub routes: Vec<Route>,
  /// The seed for smoltcp's interface RNG (TCP ISN and ephemeral port
  /// selection). `None` draws a fresh seed from system entropy at construction;
  /// `Some(seed)` pins it for deterministic tests.
  pub random_seed: Option<u64>,
}

impl InterfaceConfig {
  /// Begin a configuration for the given hardware address, with no IP
  /// addresses, no routes, and an entropy-drawn random seed.
  ///
  /// At least one IP address must be added via [`with_ip_addr`](Self::with_ip_addr)
  /// before the interface can be constructed.
  pub fn new(hardware_addr: HardwareAddress) -> Self {
    Self {
      hardware_addr,
      ip_addrs: Vec::new(),
      routes: Vec::new(),
      random_seed: None,
    }
  }

  /// Add one unicast IP address (with prefix length) the interface accepts
  /// packets for. Repeatable; addresses accumulate in order.
  pub fn with_ip_addr(mut self, cidr: IpCidr) -> Self {
    self.ip_addrs.push(cidr);
    self
  }

  /// Add one route to the interface's routing table. Repeatable; routes
  /// accumulate in order.
  pub fn with_route(mut self, route: Route) -> Self {
    self.routes.push(route);
    self
  }

  /// Pin the interface RNG seed to a fixed value (for deterministic tests),
  /// instead of drawing it from system entropy.
  pub fn with_random_seed(mut self, seed: u64) -> Self {
    self.random_seed = Some(seed);
    self
  }
}
