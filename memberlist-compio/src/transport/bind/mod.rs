//! Claiming the one address a stream transport serves on both protocols.

use std::{io, net::SocketAddr};

use compio::net::{TcpListener, UdpSocket};

use crate::error::{MemberlistError, Result};

/// Attempts spent looking for a port both protocols accept, when the advertise
/// address asks for an ephemeral one.
///
/// Attempts alternate which protocol leads, so this is eight chances for each
/// strategy — far more than the scattered `TIME_WAIT` collisions the TCP-led
/// pass can hit, and the UDP-led pass needs only one.
const EPHEMERAL_BIND_ATTEMPTS: usize = 16;

/// The backlog compio's own `TcpListener::bind` passes to `listen`.
///
/// The Windows listener is built here rather than by compio, and it must differ
/// from an ordinary compio listener in nothing except the address-reuse option
/// it declines to set.
#[cfg(windows)]
const TCP_LISTEN_BACKLOG: i32 = 128;

/// Bind the UDP gossip socket and the TCP reliable listener a stream transport
/// serves, on ONE address.
///
/// memberlist reaches a node at a single advertised address, so both sockets
/// must hold the same port. Neither protocol's bind can ask for "a free port
/// the other one can have as well", so the pair is claimed in two steps and the
/// attempt is retried when the second step is refused. Each ordering has its
/// own failure mode, and the retry ALTERNATES which protocol leads so that
/// neither mode can trap the loop:
///
/// * TCP first — the OS picks a free TCP port, then UDP takes it. This avoids
///   the reverse race, where an ephemeral UDP port lands on a TCP port still in
///   `TIME_WAIT` (the two port spaces are independent) and the TCP bind fails
///   with `AddrInUse`. Its own failure is Windows': the platform reserves
///   contiguous BLOCKS of UDP ports, whose bind returns `WSAEACCES`, and the
///   TCP ephemeral allocator hands out ports in ascending runs — so once it
///   walks into such a block, every further TCP-led attempt lands in the same
///   block and a whole retry budget can be spent inside one reservation.
/// * UDP first — the OS picks a free UDP port, which by construction is not in
///   a UDP-reserved block, then TCP takes it. That is the escape from the
///   block, and its own failure (a `TIME_WAIT` TCP port) is transient and
///   scattered rather than contiguous.
///
/// Alternating bounds both: an attempt that failed for one protocol's reason is
/// followed by one that cannot fail for it. A fixed (nonzero) port has no other
/// port to walk to, so it is a single TCP-led attempt and a genuine conflict
/// surfaces to the caller instead of looping.
///
/// # Taking a port another socket owns
///
/// A claim must never land on a port some other socket already holds: two
/// listeners on one TCP port split the connections arriving for it, and two
/// sockets on one UDP port take each other's datagrams in an order the OS does
/// not define — gossip that simply disappears. Both halves are therefore bound
/// so that an occupied port is REFUSED.
///
/// On Unix that is what a bind already does. `SO_REUSEADDR` there only lets a
/// bind reuse a port whose previous connections are lingering in `TIME_WAIT`;
/// it cannot take a port from a socket that is still using it. So compio's own
/// `bind` is used unchanged.
///
/// Windows is the platform where it matters. There `SO_REUSEADDR` on the SECOND
/// bind hands it the port an active socket holds — Microsoft documents the
/// outcome as "the second socket has overtaken the port", with which socket
/// receives packets left undefined — and compio's `TcpListener::bind` sets that
/// option. A claim built on it can succeed against a port an unrelated listener
/// owns. So on Windows both halves are constructed here with no address-reuse
/// option at all, and Windows then refuses an occupied port with
/// `WSAEADDRINUSE`, the same answer Unix gives. The gossip socket goes through
/// the same path even though compio's UDP bind happens not to set the option
/// today: the property belongs to this module, not to an implementation detail
/// of a pinned dependency.
///
/// `SO_EXCLUSIVEADDRUSE` would go one step further and stop ANOTHER process's
/// `SO_REUSEADDR` bind from taking a port this node holds. It is deliberately
/// not set. Windows keeps the port of a listener bound with it reserved after
/// close until every connection that listener accepted is fully finished, so a
/// node that died without a graceful shutdown could not rebind its configured
/// port — which trades a defence against a hostile co-located process for a
/// failure in the crash-and-rejoin path this protocol exists to survive.
///
/// Returns the listener, the concrete bound address (the OS-assigned port for
/// an ephemeral advertise), and the gossip socket.
pub(crate) async fn bind_stream_pair(
  advertise: SocketAddr,
) -> Result<(TcpListener, SocketAddr, UdpSocket)> {
  if advertise.port() != 0 {
    return bind_tcp_first(advertise).await.map_err(MemberlistError::Io);
  }

  let mut attempt = 0usize;
  loop {
    let outcome = if attempt.is_multiple_of(2) {
      bind_tcp_first(advertise).await
    } else {
      bind_udp_first(advertise).await
    };
    match outcome {
      Ok(pair) => return Ok(pair),
      Err(e) => {
        attempt += 1;
        if attempt >= EPHEMERAL_BIND_ATTEMPTS || !is_port_conflict(&e) {
          return Err(MemberlistError::Io(e));
        }
      }
    }
  }
}

/// One TCP-led attempt: claim a free TCP port, then take the same port for UDP.
async fn bind_tcp_first(advertise: SocketAddr) -> io::Result<(TcpListener, SocketAddr, UdpSocket)> {
  let listener = bind_tcp_exclusive(advertise).await?;
  let bound = listener.local_addr()?;
  match bind_udp_exclusive(bound).await {
    Ok(gossip) => Ok((listener, bound, gossip)),
    Err(e) => {
      // Release the claimed TCP port before the next attempt walks on. A plain
      // drop closes the handle asynchronously on Windows, so the port would
      // linger and a run of attempts — several nodes constructing at once —
      // could exhaust the ephemeral pool before finding a bindable pair.
      // Ignoring Err: this attempt's listener is discarded either way, and a
      // close error says nothing the bind error below does not.
      let _ = listener.close().await;
      Err(e)
    }
  }
}

/// One UDP-led attempt: claim a free UDP port, then take the same port for TCP.
async fn bind_udp_first(advertise: SocketAddr) -> io::Result<(TcpListener, SocketAddr, UdpSocket)> {
  let gossip = bind_udp_exclusive(advertise).await?;
  let bound = gossip.local_addr()?;
  match bind_tcp_exclusive(bound).await {
    Ok(listener) => Ok((listener, bound, gossip)),
    Err(e) => {
      // Ignoring Err: as above — the gossip socket is discarded and its close
      // outcome adds nothing to the TCP bind error being returned.
      let _ = gossip.close().await;
      Err(e)
    }
  }
}

/// Bind the reliable listener half of a claim, refusing a port another socket
/// already owns.
///
/// compio's `bind` sets `SO_REUSEADDR`, which on Windows is the option that
/// takes a port from its owner, so there the listener is built here without it.
/// The socket is otherwise the one compio would have made: on Windows compio's
/// own constructor is this same `socket2` call, and the backlog matches.
#[cfg(windows)]
async fn bind_tcp_exclusive(addr: SocketAddr) -> io::Result<TcpListener> {
  use socket2::{Protocol, SockAddr, Socket, Type};

  let addr = SockAddr::from(addr);
  let socket = Socket::new(addr.domain(), Type::STREAM, Some(Protocol::TCP))?;
  socket.bind(&addr)?;
  socket.listen(TCP_LISTEN_BACKLOG)?;
  TcpListener::from_std(socket.into())
}

/// Bind the reliable listener half of a claim, refusing a port another socket
/// already owns.
///
/// `SO_REUSEADDR`, which compio's `bind` sets, cannot take a port from an
/// active socket on this platform — it only reuses one whose old connections
/// are in `TIME_WAIT` — so compio's bind is already exclusive enough.
#[cfg(not(windows))]
async fn bind_tcp_exclusive(addr: SocketAddr) -> io::Result<TcpListener> {
  TcpListener::bind(addr).await
}

/// Bind the gossip half of a claim, refusing a port another socket already
/// owns.
///
/// Built here for the same reason as the listener: on Windows an address-reuse
/// option is what lets a bind share a port, and a shared UDP port delivers
/// datagrams to one of the two sockets unpredictably.
#[cfg(windows)]
async fn bind_udp_exclusive(addr: SocketAddr) -> io::Result<UdpSocket> {
  use socket2::{Protocol, SockAddr, Socket, Type};

  let addr = SockAddr::from(addr);
  let socket = Socket::new(addr.domain(), Type::DGRAM, Some(Protocol::UDP))?;
  socket.bind(&addr)?;
  UdpSocket::from_std(socket.into())
}

/// Bind the gossip half of a claim, refusing a port another socket already
/// owns.
#[cfg(not(windows))]
async fn bind_udp_exclusive(addr: SocketAddr) -> io::Result<UdpSocket> {
  UdpSocket::bind(addr).await
}

/// Whether a refused bind is the kind of port conflict a fresh attempt on a
/// different port can get past.
///
/// `AddrInUse` is the ordinary collision, including a TCP port still in
/// `TIME_WAIT`. `PermissionDenied` is Windows' `WSAEACCES`, which bind returns
/// for a port inside a platform-reserved block. Anything else — an address that
/// is not local, an exhausted descriptor table — fails identically on every
/// port, so it surfaces to the caller rather than burning the budget.
fn is_port_conflict(e: &io::Error) -> bool {
  matches!(
    e.kind(),
    io::ErrorKind::AddrInUse | io::ErrorKind::PermissionDenied
  )
}

#[cfg(test)]
mod tests;
