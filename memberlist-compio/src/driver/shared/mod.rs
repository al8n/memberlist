//! Driver helpers shared by every transport backend's driver loop (the
//! stream driver in [`crate::driver`] and the QUIC driver in
//! [`crate::quic::driver`]).
//!
//! These are the parts of the observation / event hand-off path that are
//! independent of the reliable plane: the cluster-wide [`ExchangeId`] token,
//! the [`Delegate`] hook dispatcher, the cooperative yield used to drain a
//! bounded observation channel, and the observation byte-backstop accounting.
//! They live here — outside the feature-gated [`crate::driver`] /
//! [`crate::bridge`] stream-transport modules — so the QUIC driver can reuse
//! them without depending on the byte-stream plane (which is only compiled with
//! a `tcp` / `tls-*` feature).

use std::{cell::Cell, future::Future, pin::Pin, time::Duration};

use compio::{buf::BufResult, net::UdpSocket};

use memberlist_proto::event::Event;

use crate::{delegate::Delegate, transport::runtime::CidrFilter};
use core::task::Poll;
use smallvec::SmallVec;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// Coordinator-allocated handle for one in-flight reliable exchange.
///
/// The driver and the per-bridge task agree on the same opaque id without
/// the rest of the crate having to name the machine's `streams` module.
/// Sourced from the ungated [`memberlist_proto::event`] module so the QUIC
/// driver — whose reliable plane is `QuicEndpoint`, not the byte-stream
/// `streams` plane — shares the identical id type as the stream backends.
pub(crate) type ExchangeId = memberlist_proto::event::ExchangeId;

/// Fire the matching [`Delegate`] hook for one drained [`Event`].
///
/// The event-shaped hooks (`notify_join` / `notify_leave` / `notify_update`
/// / `notify_ping_complete`) run on the driver thread BEFORE the event is
/// forwarded to subscribers, so a delegate observes the transition before
/// any [`EventStream`](crate::EventStream) consumer does. The membership
/// FSM already carries the resolved `Arc<NodeState>` inside each variant,
/// so the hook borrows it (cheap `Arc` bump) with no re-projection.
///
/// Admission (`notify_alive` / `notify_merge`) is NOT fired here — those
/// are the machine's `AliveDelegate` / `MergeDelegate` predicates, supplied
/// via [`Options`](crate::Options) and run inline inside the FSM ahead of
/// the alive/merge transition. The observation [`Delegate`] is a distinct
/// concern: its hooks observe transitions the FSM has already applied.
pub(crate) async fn dispatch_event_delegate<I, A, D>(delegate: &D, ev: &Event<I, A>)
where
  D: Delegate<Id = I, Address = A>,
{
  match ev {
    Event::NodeJoined(node) => delegate.notify_join(node.clone()).await,
    Event::NodeLeft(node) => delegate.notify_leave(node.clone()).await,
    Event::NodeUpdated(node) => delegate.notify_update(node.clone()).await,
    Event::PingCompleted(payload) => {
      let node = payload.node_ref();
      delegate
        .notify_ping_complete(
          node.id_ref(),
          node.address_ref(),
          payload.rtt(),
          payload.payload_ref().clone(),
        )
        .await;
    }
    Event::NodeConflict(c) => {
      delegate
        .notify_conflict(c.existing_ref().clone(), c.other_ref().clone())
        .await;
    }
    Event::UserPacket(pkt) => {
      delegate
        .notify_user_msg(std::borrow::Cow::Borrowed(pkt.data_ref().as_ref()))
        .await;
    }
    Event::RemoteStateReceived(rs) => {
      delegate
        .merge_remote_state(rs.user_data_ref().as_ref(), rs.join())
        .await;
    }
    _ => {}
  }
}

/// Yield to the runtime exactly once.
///
/// The event drain is synchronous — no `.await` fires for membership
/// events — so on a single-threaded runtime the observation task is not
/// scheduled mid-drain. A bounded `obs_tx` would therefore overflow on a
/// single large-but-valid burst (e.g. a join push/pull carrying many members)
/// before the task drains a single event. Yielding hands the scheduler to the
/// already-woken observation task so it can drain `obs_rx` before the drain
/// continues. Runtime-agnostic (no dependency on a specific `yield_now`):
/// re-arms the waker and returns `Pending` once, so the executor runs other
/// ready tasks before re-polling this one.
pub(crate) async fn yield_once() {
  let mut yielded = false;
  core::future::poll_fn(move |cx| {
    if yielded {
      Poll::Ready(())
    } else {
      yielded = true;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  })
  .await
}

pub(crate) use memberlist_driver::observation_payload_bytes;

/// Add a just-enqueued event's payload weight (if any) to the byte-backstop
/// counter. Paired with the subtract in each driver's `observation_task` on
/// dequeue.
pub(crate) fn add_obs_payload(counter: &Cell<u64>, bytes: Option<u64>) {
  if let Some(b) = bytes {
    counter.set(counter.get() + b);
  }
}

/// Build a fully-resolved join's reply from its reached set and requested count.
/// A non-empty set resolves `Ok(set)`; an empty set is the all-failed case,
/// surfacing `JoinFailed { requested, contacted: 0 }` with an empty
/// reached-so-far set (the partial-success slot mirrored from the serf driver).
pub(crate) fn join_reply(
  contacted: SmallVec<[SocketAddr; 1]>,
  requested: usize,
) -> crate::command::JoinReply {
  if contacted.is_empty() {
    Err((
      SmallVec::new(),
      crate::error::MemberlistError::JoinFailed(crate::error::JoinFailed::new(requested, 0)),
    ))
  } else {
    Ok(contacted)
  }
}

/// CIDR transport filter shared by the stream and QUIC driver loops: whether the
/// policy blocks `ip` (always `false` without the `cidr` feature). Applied at recv
/// (the gossip / QUIC datagram source) and at accept (the reliable stream peer).
#[cfg(feature = "cidr")]
pub(crate) fn cidr_blocks(filter: &CidrFilter, ip: IpAddr) -> bool {
  filter.as_ref().is_some_and(|p| p.is_blocked(&ip))
}
#[cfg(not(feature = "cidr"))]
pub(crate) fn cidr_blocks(_filter: &CidrFilter, _ip: IpAddr) -> bool {
  false
}

/// One in-flight `recv_from` on a driver's gossip UDP socket, owning its
/// buffer until the operation resolves. Boxed so [`PendingRecv`] can hold it
/// across loop iterations at a stable address.
pub(crate) type RecvFut<'a> =
  Pin<Box<dyn Future<Output = BufResult<(usize, SocketAddr), Vec<u8>>> + 'a>>;

/// A driver loop's single in-flight datagram receive.
///
/// Exactly ONE `recv_from` operation stays armed for the whole life of the
/// loop, and it is re-armed only after it has resolved. The alternative —
/// building a fresh `recv_from` every iteration and abandoning it whenever a
/// non-recv select arm wins — is unsound on a completion-based backend
/// (io_uring on Linux, IOCP on Windows), where the buffer is handed to the
/// kernel at submit time rather than filled at poll time:
///
/// * A datagram the kernel has already delivered into the abandoned buffer is
///   discarded, so inbound gossip is silently lost whenever the loop happens
///   to service a command or a timer instead.
/// * Each abandoned operation keeps a reference to the socket's shared
///   descriptor until its cancellation is reaped. io_uring's cancellation is
///   itself a submission-queue entry that is dropped when that queue is full,
///   and a loop that resolves a ready arm without ever yielding to the runtime
///   never lets the queue drain, so those operations are stranded permanently.
///   `UdpSocket::close` waits for the descriptor's last reference, so a
///   stranded operation makes the post-loop close — and with it the caller's
///   `shutdown` — hang forever.
///
/// A readiness-based backend (epoll, kqueue) consumes nothing until the future
/// is polled, which is why the defect is invisible outside io_uring/IOCP. This
/// is the same discipline the stream driver's accept future already follows.
///
/// The borrowed socket must outlive the holder. A driver whose socket lives
/// inside a struct it also borrows mutably each iteration passes a reference to
/// a CLONE; one that owns the socket as a local passes it directly. Either way
/// the pending future — and any clone it borrows — must be dropped before the
/// post-loop close, which awaits the sole remaining reference to the descriptor.
pub(crate) struct PendingRecv<'a> {
  socket: &'a UdpSocket,
  buf_len: usize,
  fut: RecvFut<'a>,
}

impl<'a> PendingRecv<'a> {
  /// Arm the first receive on `socket`.
  pub(crate) fn new(socket: &'a UdpSocket, buf_len: usize) -> Self {
    let fut = Box::pin(socket.recv_from(vec![0u8; buf_len]));
    Self {
      socket,
      buf_len,
      fut,
    }
  }

  /// The pending operation, for polling inside a `select`.
  pub(crate) fn fut(&mut self) -> &mut RecvFut<'a> {
    &mut self.fut
  }

  /// Arm a fresh receive with a fresh buffer.
  ///
  /// Call this ONLY after the pending future has resolved: the resolved future
  /// has released its buffer and its descriptor reference, so re-arming
  /// strands nothing. Calling it on an unresolved future would reintroduce the
  /// abandonment this type exists to prevent.
  pub(crate) fn rearm(&mut self) {
    self.fut = Box::pin(self.socket.recv_from(vec![0u8; self.buf_len]));
  }
}

/// Payload of the teardown marker datagram.
///
/// The bytes are never interpreted: the marker is sent only after a driver
/// loop has exited, and the receive it completes is discarded rather than fed
/// to the endpoint. The pattern is fixed and private purely so a datagram seen
/// while debugging is recognisable.
const TEARDOWN_MARKER: [u8; 8] = [0xff, 0x00, 0x6d, 0x6c, 0x74, 0x64, 0x00, 0xff];

/// Marker sends attempted before falling back to the drop-based path.
const TEARDOWN_MARKER_ATTEMPTS: usize = 3;

/// How long to wait for the pending receive after each marker send.
const TEARDOWN_RECV_WAIT: Duration = Duration::from_secs(1);

/// Ceiling on a driver's post-loop socket close.
///
/// The close is expected to be immediate once the last receive has been
/// completed. The bound exists so that a teardown which could not complete it
/// — a send failure, or a platform where the self-addressed datagram does not
/// come back — degrades to a slow shutdown instead of a hung one.
pub(crate) const TEARDOWN_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// The socket's own address, as a destination it can send to.
fn self_addressed_dest(socket: &UdpSocket) -> Option<SocketAddr> {
  self_addressed(socket.local_addr().ok()?)
}

/// A bound local address, as a destination it can be reached on.
///
/// A wildcard bind (`0.0.0.0` / `::`) is not a valid destination, so the
/// matching loopback address is substituted; the port is the bound one either
/// way. Returns `None` for an unbound address, whose port would be zero.
fn self_addressed(local: SocketAddr) -> Option<SocketAddr> {
  if local.port() == 0 {
    return None;
  }
  let ip = match local.ip() {
    IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
    IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
    ip => ip,
  };
  Some(SocketAddr::new(ip, local.port()))
}

/// Complete a driver's last pending receive so the socket can be closed.
///
/// Dropping an in-flight receive does NOT reliably end it on io_uring. The
/// drop issues ONE best-effort `AsyncCancel` submission-queue entry, and
/// unlike an ordinary operation push — which drains the queue and retries when
/// it is full — that push is discarded with a warning if there is no room. The
/// receive then stays in flight forever, holding a reference to the socket's
/// shared descriptor, and the `UdpSocket::close` that follows waits on that
/// last reference and never returns.
///
/// Sending the socket a datagram addressed to itself resolves the receive
/// instead of cancelling it, which needs no free queue slot at drop time: the
/// send is awaited, so it drives the runtime to submit and reap, and awaiting
/// the receive afterwards reaps its completion. A resolved future cancels
/// nothing when it is dropped, so the close that follows has no reference left
/// to wait for.
///
/// The received bytes are discarded — the loop has already exited and nothing
/// further is fed to the endpoint — so the marker is never interpreted, and a
/// real datagram that happens to arrive first serves equally well.
///
/// Returns whether the receive was completed. `false` means the caller is back
/// on the drop-based path and must bound its close with
/// [`TEARDOWN_CLOSE_TIMEOUT`].
pub(crate) async fn complete_recv_before_close(mut recv: PendingRecv<'_>) -> bool {
  let Some(dest) = self_addressed_dest(recv.socket) else {
    return false;
  };
  for _ in 0..TEARDOWN_MARKER_ATTEMPTS {
    let BufResult(res, _) = recv.socket.send_to(TEARDOWN_MARKER.to_vec(), dest).await;
    if res.is_err() {
      // A connected socket rejects a send to another address, and a send can
      // fail outright on a torn-down interface. Neither is recoverable here.
      return false;
    }
    // Awaiting the receive is what drives the runtime to submit and reap the
    // completion. `timeout` borrows the pending future, so an elapsed attempt
    // drops only the borrow and leaves the operation in flight to be retried.
    if compio::time::timeout(TEARDOWN_RECV_WAIT, recv.fut().as_mut())
      .await
      .is_ok()
    {
      return true;
    }
  }
  false
}

/// Complete a stream driver's pending accept so the listener can be closed.
///
/// The listener carries the same hazard as the gossip socket: a pending accept
/// that is merely DROPPED relies on one best-effort `AsyncCancel` push, which
/// io_uring discards when its submission queue is full. The orphaned accept
/// keeps a reference to the listener's shared descriptor, so the port stays
/// bound — a plain `drop` of the listener then does not release it at all, and
/// an awaited `close` would wait on that reference forever. A driver that
/// reports a successful shutdown while still holding the port makes an
/// immediate rebind fail (`WSAEACCES` on Windows, `AddrInUse` elsewhere).
///
/// Connecting to the listener's own address resolves the accept instead of
/// cancelling it, needing no free queue slot: both the connect and the accept
/// are awaited, which drives the runtime to submit and reap. The accepted
/// connection and the connecting side are dropped immediately — the driver
/// loop has exited and nothing will be served on them.
///
/// A terminated future has already resolved and holds no operation, so there
/// is nothing to complete and the listener can be closed directly.
///
/// Returns whether the listener has no operation left in flight. `false` means
/// the caller is on the drop-based path and must bound its close with
/// [`TEARDOWN_CLOSE_TIMEOUT`].
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
pub(crate) async fn complete_accept_before_close<F>(
  mut accept: core::pin::Pin<&mut F>,
  listener: &compio::net::TcpListener,
) -> bool
where
  F: futures_util::future::FusedFuture + ?Sized,
{
  if accept.is_terminated() {
    return true;
  }
  let Ok(local) = listener.local_addr() else {
    return false;
  };
  let Some(dest) = self_addressed(local) else {
    return false;
  };
  for _ in 0..TEARDOWN_MARKER_ATTEMPTS {
    let Ok(stream) = compio::net::TcpStream::connect(dest).await else {
      // A listener whose address cannot be dialled (a torn-down interface, a
      // refused loopback connect) leaves nothing to complete the accept with.
      return false;
    };
    // Awaiting the accept is what drives the runtime to reap its completion.
    // `timeout` borrows the pending future, so an elapsed attempt drops only
    // the borrow and leaves the operation in flight for the next attempt.
    let done = compio::time::timeout(TEARDOWN_RECV_WAIT, accept.as_mut())
      .await
      .is_ok();
    // Drop the connecting side and anything it was accepted into: the loop has
    // exited, so neither is served.
    drop(stream);
    if done {
      return true;
    }
  }
  false
}

#[cfg(test)]
mod tests;
