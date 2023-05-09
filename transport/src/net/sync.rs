use std::{
  collections::VecDeque,
  net::SocketAddr,
  os::fd::{AsRawFd, FromRawFd},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use either::Either;
use showbiz_types::{Address, Packet};

use super::{NetTransportOptions, UDP_PACKET_BUF_SIZE, UDP_RECV_BUF_SIZE};
use crate::sealed::{
  sleep, spawn, unbounded, IOError, TcpListener, TcpStream, UdpSocket, UnboundedReceiver,
  UnboundedSender, WaitGroup,
};

error!();
set_udp_recv_buf!();
transport!(TcpStream,,);

impl showbiz_traits::NodeAwareTransport for NetTransport {
  fn write_to_address(&self, b: &[u8], addr: Address) -> Result<std::time::Instant, Self::Error> {
    // We made sure there's at least one UDP listener, so just use the
    // packet sending interface on the first one. Take the time after the
    // write call comes back, which will underestimate the time a little,
    // but help account for any delays before the write occurs.
    self
      .udp_listener
      .send_to(b, addr.addr())
      .map(|_| Instant::now())
      .map_err(From::from)
  }

  fn dial_address_timeout(
    &self,
    addr: Address,
    timeout: std::time::Duration,
  ) -> Result<Self::Connection, Self::Error> {
    TcpStream::connect_timeout(&addr.addr(), timeout).map_err(From::from)
  }
}

tcp_processor!(TcpStream,, , ||);
udp_processor!(, , ||);
