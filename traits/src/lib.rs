#![forbid(unsafe_code)]

#[cfg(all(feature = "sync", feature = "async"))]
compile_error!("feature `sync` and `async` cannot be enabled at the same time");

#[cfg(feature = "async")]
pub use async_trait;

mod transport;
pub use transport::*;

#[cfg(feature = "async")]
pub use trust_dns_proto::{
  tcp::{Connect, DnsTcpStream},
  udp::{DnsUdpSocket, UdpSocket},
  Time,
};
