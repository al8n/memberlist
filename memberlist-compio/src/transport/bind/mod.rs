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
  let listener = TcpListener::bind(advertise).await?;
  let bound = listener.local_addr()?;
  match UdpSocket::bind(bound).await {
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
  let gossip = UdpSocket::bind(advertise).await?;
  let bound = gossip.local_addr()?;
  match TcpListener::bind(bound).await {
    Ok(listener) => Ok((listener, bound, gossip)),
    Err(e) => {
      // Ignoring Err: as above — the gossip socket is discarded and its close
      // outcome adds nothing to the TCP bind error being returned.
      let _ = gossip.close().await;
      Err(e)
    }
  }
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
