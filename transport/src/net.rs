use std::net::SocketAddr;

/// Used to buffer incoming packets during read
/// operations.
const UDP_PACKET_BUF_SIZE: usize = 65536;

/// A large buffer size that we attempt to set UDP
/// sockets to in order to handle a large volume of messages.
const UDP_RECV_BUF_SIZE: usize = 2 * 1024 * 1024;

macro_rules! error {
  () => {
    #[derive(Debug, thiserror::Error)]
    pub enum Error {
      #[error("showbiz: net transport: {0}")]
      IO(#[from] IOError),
      #[error("showbiz: net transport: no private IP address found, and explicit IP not provided")]
      NoPrivateIP,
      #[error("showbiz: net transport: failed to get interface addresses {0}")]
      NoInterfaceAddresses(#[from] local_ip_address::Error),
      #[error("showbiz: net transport: at least one bind address is required")]
      EmptyBindAddrs,
      #[error("showbiz: net transport: failed to resize UDP buffer {0}")]
      FailToResizeUdpBuffer(IOError),
    }
  };
}

/// Used to configure a net transport.
#[viewit::viewit(vis_all = "")]
pub struct NetTransportOptions {
  /// A list of addresses to bind to for both TCP and UDP
  /// communications.
  #[viewit(getter(
    style = "ref",
    result(type = "&[SocketAddr]", converter(fn = "Vec::as_slice"))
  ))]
  bind_addrs: Vec<SocketAddr>,
}

macro_rules! set_udp_recv_buf {
  () => {
    #[inline]
    fn set_udp_recv_buf(socket: &UdpSocket) -> Result<(), Error> {
      use socket2::Socket;
      // Safety: the fd we created from the socket is just created, so it is a valid and open file descriptor
      let socket = unsafe { Socket::from_raw_fd(socket.as_raw_fd()) };
      let mut size = UDP_RECV_BUF_SIZE;
      let mut err = None;

      while size > 0 {
        match socket.set_recv_buffer_size(size) {
          Ok(()) => return Ok(()),
          Err(e) => {
            err = Some(e);
            size /= 2;
          }
        }
      }

      // This is required to prevent double-closing the file descriptor.
      drop(socket);

      match err {
        Some(err) => Err(Error::FailToResizeUdpBuffer(err)),
        None => Ok(()),
      }
    }
  };
}

macro_rules! transport {
  ($conn: ident, $($suffix: ident)?, $($async:ident)?) => {
    pub struct NetTransport {
      opts: Arc<NetTransportOptions>,
      packet_rx: UnboundedReceiver<Packet>,
      stream_rx: UnboundedReceiver<$conn>,

      tcp_addr: SocketAddr,
      udp_listener: Arc<UdpSocket>,
      wg: WaitGroup,
      shutdown: Arc<AtomicBool>,
    }

    impl NetTransport {
      /// Returns the bind port that was automatically given by the
      /// kernel, if a bind port of 0 was given.
      #[inline]
      pub const fn get_auto_bind_port(&self) -> u16 {
        self.tcp_addr.port()
      }
    }

    #[cfg_attr(any(
      feature = "tokio",
      feature = "async",
      feature = "smol"
    ), async_trait::async_trait)]
    impl showbiz_traits::Transport for NetTransport {
      type Error = Error;

      type Connection = $conn;

      type Options = Arc<NetTransportOptions>;

      $($async)? fn new(opts: Self::Options) -> Result<Self, Self::Error>
      where
        Self: Sized,
      {
        // If we reject the empty list outright we can assume that there's at
        // least one listener of each type later during operation.
        if opts.bind_addrs.is_empty() {
          return Err(Error::EmptyBindAddrs);
        }

        let (stream_tx, stream_rx) = unbounded();
        let (packet_tx, packet_rx) = unbounded();

        let mut tcp_listeners = VecDeque::with_capacity(opts.bind_addrs.len());
        let mut udp_listeners = VecDeque::with_capacity(opts.bind_addrs.len());
        let tcp_addr = opts.bind_addrs[0];
        for addr in &opts.bind_addrs {
          tcp_listeners.push_back(TcpListener::bind(addr) $(. $suffix )? ?);

          let udp_ln = UdpSocket::bind(addr) $(. $suffix )? ?;
          set_udp_recv_buf(&udp_ln)?;
          udp_listeners.push_back(udp_ln);
        }

        let wg = WaitGroup::new();
        let shutdown = Arc::new(AtomicBool::new(false));

        // Fire them up now that we've been able to create them all.
        // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
        // udp and tcp listener can
        for _ in 0..opts.bind_addrs.len() - 1 {
          wg.add(2);
          let tcp_ln = tcp_listeners.pop_back().unwrap();
          let udp_ln = udp_listeners.pop_back().unwrap();

          TcpProcessor {
            wg: wg.clone(),
            stream_tx: stream_tx.clone(),
            ln: tcp_ln,
            shutdown: shutdown.clone(),
          }
          .run();

          UdpProcessor {
            wg: wg.clone(),
            packet_tx: packet_tx.clone(),
            ln: Either::Right(udp_ln),
            shutdown: shutdown.clone(),
          }
          .run();
        }

        wg.add(2);
        let tcp_ln = tcp_listeners.pop_back().unwrap();
        let udp_ln = Arc::new(udp_listeners.pop_back().unwrap());
        TcpProcessor {
          wg: wg.clone(),
          stream_tx,
          ln: tcp_ln,
          shutdown: shutdown.clone(),
        }
        .run();

        UdpProcessor {
          wg: wg.clone(),
          packet_tx,
          ln: Either::Left(udp_ln.clone()),
          shutdown: shutdown.clone(),
        }
        .run();

        Ok(Self {
          opts,
          packet_rx,
          stream_rx,
          wg,
          shutdown,
          tcp_addr,
          udp_listener: udp_ln,
        })
      }

      $($async)? fn write_to(&self, b: &[u8], addr: SocketAddr) -> Result<Instant, Self::Error> {
        <Self as showbiz_traits::NodeAwareTransport>::write_to_address(self, b, Address::from(addr)) $(. $suffix)?
      }

      $($async)? fn dial_timeout(
        &self,
        addr: SocketAddr,
        timeout: std::time::Duration,
      ) -> Result<Self::Connection, Self::Error> {
        <Self as showbiz_traits::NodeAwareTransport>::dial_address_timeout(&self, addr.into(), timeout) $(. $suffix)?
      }

      $($async)? fn shutdown(self) -> Result<(), Self::Error> {
        // This will avoid log spam about errors when we shut down.
        self.shutdown.store(true, Ordering::SeqCst);

        self.wg.wait() $(. $suffix)? ;

        Ok(())
      }

      fn final_advertise_addr(
        &self,
        addr: Option<SocketAddr>,
      ) -> Result<SocketAddr, Self::Error> {
        match addr {
          Some(addr) => Ok(addr),
          None => {
            if self.opts.bind_addrs[0].ip().is_unspecified() {
              local_ip_address::local_ip()
                .map_err(|e| match e {
                  local_ip_address::Error::LocalIpAddressNotFound => Error::NoPrivateIP,
                  e => Error::NoInterfaceAddresses(e),
                })
                .map(|ip| SocketAddr::new(ip, self.get_auto_bind_port()))
            } else {
              // Use the IP that we're bound to, based on the first
              // TCP listener, which we already ensure is there.
              let mut addr = self.tcp_addr;
              addr.set_port(self.get_auto_bind_port());
              Ok(addr)
            }
          }
        }
      }

      fn packet_rx(&self) -> &UnboundedReceiver<Packet> {
        &self.packet_rx
      }

      fn stream_rx(&self) -> &UnboundedReceiver<Self::Connection> {
        &self.stream_rx
      }
    }
  };
}

macro_rules! udp_processor {
  ($($suffix: ident)?, $($async:ident)?, $($closure: tt)?) => {
    struct UdpProcessor {
      wg: WaitGroup,
      packet_tx: UnboundedSender<Packet>,
      ln: Either<Arc<UdpSocket>, UdpSocket>,
      shutdown: Arc<AtomicBool>,
    }

    impl UdpProcessor {
      pub(super) fn run(self) {
        let Self {
          wg,
          packet_tx,
          ln,
          shutdown,
        } = self;

        spawn($($async)? move $($closure)? {
          scopeguard::defer!(wg.done());
          loop {
            // Do a blocking read into a fresh buffer. Grab a time stamp as
            // close as possible to the I/O.
            let mut buf = vec![0; UDP_PACKET_BUF_SIZE];
            let ln = match &ln {
              Either::Left(ln) => ln,
              Either::Right(ln) => ln,
            };
            match ln.recv_from(&mut buf) $(. $suffix)? {
              Ok((n, addr)) => {
                // Check the length - it needs to have at least one byte to be a
                // proper message.
                if n < 1 {
                  tracing::error!(target = "showbiz", err = "UDP packet too short (0 bytes)");
                  continue;
                }

                buf.truncate(n);
                if let Err(e) = packet_tx.send(Packet::new(buf.into(), addr, Instant::now())) $(. $suffix)? {
                  tracing::error!(target = "showbiz", err = %e, "failed to send packet");
                }
                // TODO: record metrics
              }
              Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                  break;
                }

                tracing::error!(target = "showbiz", err = %e, "Error reading UDP packet");
                continue;
              }
            };
          }
        });
      }
    }
  };
}

macro_rules! tcp_processor {
  ($conn: ident, $($suffix: ident)?, $($async:ident)?, $($closure: tt)?) => {
    struct TcpProcessor {
      wg: WaitGroup,
      stream_tx: UnboundedSender<$conn>,
      ln: TcpListener,
      shutdown: Arc<AtomicBool>,
    }

    impl TcpProcessor {
      pub(super) fn run(self) {
        let Self {
          wg,
          stream_tx,
          ln,
          shutdown,
        } = self;

        /// The initial delay after an AcceptTCP() error before attempting again
        const BASE_DELAY: Duration = Duration::from_millis(5);

        /// the maximum delay after an AcceptTCP() error before attempting again.
        /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
        /// Therefore, changes to maxDelay may have an effect on the latency of shutdown.
        const MAX_DELAY: Duration = Duration::from_secs(1);

        spawn($($async)? move $($closure)? {
          scopeguard::defer!(wg.done());
          let mut loop_delay = Duration::ZERO;

          loop {
            match ln.accept() $(. $suffix)? {
              Ok((conn, _)) => {
                // No error, reset loop delay
                loop_delay = Duration::ZERO;

                if let Err(e) = stream_tx.send(conn.into()) $(. $suffix)? {
                  tracing::error!(target = "showbiz", err = %e, "failed to send tcp connection");
                }
              }
              Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                  break;
                }

                if loop_delay == Duration::ZERO {
                  loop_delay = BASE_DELAY;
                } else {
                  loop_delay *= 2;
                }

                if loop_delay > MAX_DELAY {
                  loop_delay = MAX_DELAY;
                }

                tracing::error!(target = "showbiz", err = %e, "Error accepting TCP connection");
                sleep(loop_delay) $(. $suffix)? ;
                continue;
              }
            }
          }
        });
      }
    }
  };
}

#[cfg(feature = "sync")]
mod sync;
#[cfg(feature = "sync")]
pub use sync::*;

#[cfg(feature = "tokio")]
mod tokyo;
#[cfg(feature = "tokio")]
pub use tokyo::*;

#[cfg(feature = "async-std")]
mod async_;
#[cfg(feature = "async-std")]
pub use async_::*;

#[cfg(feature = "smol")]
mod small;
#[cfg(feature = "smol")]
pub use small::*;
