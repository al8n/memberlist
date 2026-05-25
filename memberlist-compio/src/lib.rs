//! Completion-based async driver for the Sans-I/O memberlist machine,
//! powered by compio.

mod address;
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
mod bridge;
mod command;
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
mod driver;
mod driver_options;
mod error;
mod events;
mod memberlist;
mod resolver;
mod snapshot;

#[cfg(feature = "tcp")]
mod tcp;

#[cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]
mod tls;

pub use address::Address;
pub use driver_options::{
  DEFAULT_BRIDGE_INBOUND_CAP, DEFAULT_BRIDGE_RECV_BUF_LEN, DEFAULT_CMD_FAIRNESS_BUDGET,
  DEFAULT_DIAL_TIMEOUT, DEFAULT_EVENT_QUEUE_CAP, DEFAULT_IDLE_WAKE_INTERVAL,
  DEFAULT_ITER_DRAIN_CAP, DEFAULT_JOIN_DEADLINE, DEFAULT_PEEK_BUDGET, DriverOptions,
};
pub use error::{JoinAllFailed, MemberlistError, Result};
pub use events::EventStream;
pub use memberlist::Memberlist;
pub use resolver::{OsResolver, Resolver, SocketAddrResolver};
pub use snapshot::MemberlistSnapshot;

#[cfg(feature = "tcp")]
pub use tcp::TcpMemberlist;

#[cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]
pub use tls::TlsMemberlist;

#[cfg(feature = "dns")]
pub use resolver::{DEFAULT_DNS_TIMEOUT, DnsResolver};
