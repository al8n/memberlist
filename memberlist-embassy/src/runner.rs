//! The single-task run loop: drive the engine `pump` plus the N per-slot workers
//! as sibling futures.
//!
//! One embassy task owns the [`Runner`] and calls [`Runner::run`]. Inside, two
//! kinds of future run concurrently under one `join`:
//!
//! - the **pump loop** — re-pump the engine over a fresh
//!   [`EmbassyGossip`](crate::EmbassyGossip) + [`EmbassyStream`](crate::EmbassyStream)
//!   view, drain the machine's events to resolve parked handle ops, then sleep on
//!   whichever of {UDP recv-ready, a worker/handle pump-wake, the folded deadline
//!   timer} fires first;
//! - the **N workers** — each [`run_slot`](crate::worker::run_slot) owns one
//!   `TcpSocket` and its `RefCell<Mailbox>`, looping internally forever.
//!
//! Only the pump loop re-pumps; the workers loop on their own. Because
//! [`Engine::pump`](memberlist_embedded::Engine::pump) is synchronous, the pump's
//! borrows of the engine and the mailboxes complete before its `.await`, and each
//! worker only borrows its own mailbox briefly around its socket awaits — so the
//! pump and the workers never hold overlapping `RefCell` borrows even though they
//! are siblings in one task.
//!
//! # Two wake channels
//!
//! - Per-slot **command wakes** (`cmd_wakes[i]`) carry engine → worker directives:
//!   the [`EmbassyStream`] command posters pulse slot `i`'s wake so its worker
//!   promptly acts on a new `Listen` / `Dial` / `Close` / `Abort`. One producer
//!   (the engine view), one consumer (worker `i`).
//! - The shared **pump wake** (`Shared::pump_wake`) carries worker → pump and
//!   handle → pump progress: a worker pulses it after advancing its mailbox, and a
//!   handle op pulses it after enqueuing work. Many producers, one consumer (the
//!   pump loop), which is sound because the pump drains every mailbox each tick.

use core::cell::RefCell;

use alloc::{rc::Rc, vec::Vec};

use embassy_futures::{
  join::{join, join_array},
  select::{select, select3},
};
use embassy_net::{tcp::TcpSocket, udp::UdpSocket};
use embassy_time::Timer;
use memberlist_proto::Rng;

use crate::{
  gossip_io::EmbassyGossip,
  mailbox::Mailbox,
  shared::Shared,
  stream_io::{EmbassyStream, SlotId, SlotWake},
  time,
  worker::run_slot,
};

/// The owned run-loop state for a node: the shared engine, the gossip UDP socket,
/// the `N` reliable-plane TCP sockets and their per-slot mailboxes + command
/// wakes, and the driver-side free-list.
///
/// `I` is the node id type; `N` is the TCP socket pool size; `R` is the gossip
/// RNG (defaulting to [`SmallRng`](memberlist_proto::SmallRng)). Built by
/// [`Memberlist::new`](crate::Memberlist::new), which hands back the paired
/// [`Memberlist`](crate::Memberlist) handle.
pub struct Runner<'a, I, const N: usize, R = memberlist_proto::SmallRng> {
  pub(crate) shared: Rc<Shared<I, R>>,
  pub(crate) udp: UdpSocket<'a>,
  pub(crate) tcp: [TcpSocket<'a>; N],
  pub(crate) mailboxes: [RefCell<Mailbox>; N],
  pub(crate) cmd_wakes: [SlotWake; N],
  /// Per-socket inactivity timeout (already in the embassy-time tick domain) each
  /// worker applies to its `TcpSocket` so a blocking connect/write/flush/read to an
  /// unresponsive peer cannot wedge it.
  pub(crate) socket_timeout: embassy_time::Duration,
  pub(crate) free: Vec<SlotId>,
}

impl<I, const N: usize, R> Runner<'_, I, N, R>
where
  I: memberlist_proto::Id,
  // `run` drives `pump_loop`, whose `Engine::pump` draws from the gossip RNG.
  R: Rng,
{
  /// Drive the node forever: pump the engine and run the `N` workers concurrently.
  ///
  /// Never returns under normal operation; spawn it as an embassy task (or drive
  /// it with `select` against an operation in a test). On the rare path where the
  /// pump loop itself ends, every parked handle op is failed so nothing hangs.
  pub async fn run(self) -> ! {
    let Runner {
      shared,
      udp,
      mut tcp,
      mailboxes,
      cmd_wakes,
      socket_timeout,
      mut free,
    } = self;

    // Build the N worker futures, each owning a distinct `&mut TcpSocket` (via
    // `each_mut`, which yields `N` non-aliasing mutable refs) paired with its
    // mailbox and command wake by index, plus the shared pump wake. `from_fn`
    // calls the closure for i = 0..N in order and `into_iter().next()` yields the
    // sockets in the same order, so socket[i] is matched with mailbox[i] /
    // cmd_wakes[i].
    let mut socket_iter = tcp.each_mut().into_iter();
    let workers = core::array::from_fn::<_, N, _>(|i| {
      let sock = socket_iter
        .next()
        .expect("from_fn yields indices 0..N and the socket array has exactly N elements");
      run_slot(
        sock,
        &mailboxes[i],
        &cmd_wakes[i],
        &shared.pump_wake,
        socket_timeout,
      )
    });

    // The pump loop and the workers run as siblings under one join. The pump loop
    // diverges (loops forever); `join_array` of the diverging workers likewise
    // never completes, so `join` never resolves — matching the `-> !` contract.
    join(
      pump_loop(&shared, &udp, &mailboxes, &cmd_wakes, &mut free),
      join_array(workers),
    )
    .await;

    // Unreachable: both arms diverge. Kept so the type is `-> !` and, defensively,
    // so any future change that lets the loop end does not silently leave parked
    // handle ops hanging.
    shared.fail_all_waiters();
    core::unreachable!("the run loop and workers never complete")
  }
}

/// The engine-pump half of [`Runner::run`]: re-pump on each wake and resolve
/// parked handle ops from the drained events.
async fn pump_loop<I, R>(
  shared: &Shared<I, R>,
  udp: &UdpSocket<'_>,
  mailboxes: &[RefCell<Mailbox>],
  cmd_wakes: &[SlotWake],
  free: &mut Vec<SlotId>,
) -> !
where
  I: memberlist_proto::Id,
  // `Engine::pump` drives the gossip scheduler, which draws from the RNG.
  R: Rng,
{
  loop {
    let now = time::now();

    // Pump the engine over a fresh view of the gossip socket and the slot
    // mailboxes. The pump is synchronous: its borrows of the engine and the
    // mailboxes complete here, before the `.await` below.
    let next = {
      let mut gossip = EmbassyGossip::new(udp);
      let mut stream = EmbassyStream::new(mailboxes, cmd_wakes, free);
      shared
        .engine
        .borrow_mut()
        .pump(now, &mut gossip, &mut stream)
    };

    // Resolve any handle ops whose terminal event the pump just emitted, and
    // pulse `join_wake` so a parked `join` re-checks membership.
    shared.drain_events();

    // Wait for the next thing worth re-pumping for: an inbound gossip datagram, a
    // worker/handle pump-wake, or the folded deadline. A worker pulses
    // `pump_wake` whenever it advances its mailbox (read bytes in, drained
    // outbound, saw a FIN/reset / a handshake settled), and a handle op pulses it
    // after enqueuing work, so a single consumer here re-runs the whole pump.
    match next {
      Some(deadline) => {
        let raw = time::machine_to_raw(deadline);
        // Ignoring the `Either3`: which arm woke is irrelevant — any wake re-runs
        // the whole pump, which re-derives all work and the next deadline.
        let _ = select3(
          udp.wait_recv_ready(),
          shared.pump_wake.wait(),
          Timer::at(raw),
        )
        .await;
      }
      // No scheduled machine work: wake only on I/O or a pump-wake.
      None => {
        // Ignoring the `Either`: as above, any wake simply re-runs the pump.
        let _ = select(udp.wait_recv_ready(), shared.pump_wake.wait()).await;
      }
    }
  }
}
