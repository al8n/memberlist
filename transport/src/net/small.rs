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
use showbiz_traits::{async_trait, SmolConnection};
use showbiz_types::{Address, Packet};

use super::{NetTransportOptions, UDP_PACKET_BUF_SIZE, UDP_RECV_BUF_SIZE};
use crate::smol_sealed::{
  sleep, spawn, unbounded, IOError, TcpListener, TcpStream, UdpSocket, UnboundedReceiver,
  UnboundedSender, WaitGroup,
};

error!();
set_udp_recv_buf!();
transport!(SmolConnection, await, async);

#[async_trait::async_trait]
impl showbiz_traits::NodeAwareTransport for NetTransport {
  async fn write_to_address(
    &self,
    b: &[u8],
    addr: Address,
  ) -> Result<std::time::Instant, Self::Error> {
    // We made sure there's at least one UDP listener, so just use the
    // packet sending interface on the first one. Take the time after the
    // write call comes back, which will underestimate the time a little,
    // but help account for any delays before the write occurs.
    self
      .udp_listener
      .send_to(b, addr.addr())
      .await
      .map(|_| Instant::now())
      .map_err(From::from)
  }

  async fn dial_address_timeout(
    &self,
    addr: Address,
    timeout: std::time::Duration,
  ) -> Result<Self::Connection, Self::Error> {
    // match smol::Timer::after(timeout, TcpStream::connect_timeout(addr.addr())).await {
    //   Ok(rst) => rst.map_err(From::from),
    //   Err(_) => Err(Error::IO(IOError::new(smol::io::ErrorKind::TimedOut, "timeout"))),
    // }
    todo!()
  }
}

tcp_processor!(SmolConnection, await, async,);

udp_processor!(await, async,);
