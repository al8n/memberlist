use std::{
  collections::VecDeque,
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use either::Either;
use showbiz_types::{Address, Packet};

use super::{set_udp_recv_buf, Error, NetTransportOptions, UDP_PACKET_BUF_SIZE};
use crate::{
  sleep, spawn, unbounded, IOError, TcpListener, TcpStream, UdpSocket, UnboundedReceiver,
  UnboundedSender, WaitGroup,
};

transport!(await, async);

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
  ) -> Result<Self::Conn, Self::Error> {
    todo!()
  }
}

tcp_processor!(await, async,);

udp_processor!(await, async,);
