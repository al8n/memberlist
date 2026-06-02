//! The per-slot async worker future.
//!
//! Each pool slot has one [`run_slot`] future owning one embassy-net
//! [`TcpSocket`] and sharing a `RefCell<Mailbox>` with the engine. The worker is
//! the ONLY code that touches the socket; it translates the engine's mailbox
//! [`Command`]s into socket operations and mirrors the socket's inbound bytes,
//! send-queue depth, and peer-FIN/reset state back into the mailbox for the
//! engine's synchronous [`StreamIo`](memberlist_embedded::StreamIo) view to read.
//!
//! # Borrow discipline
//!
//! The worker NEVER holds a `Mailbox` borrow across an `.await`. Every step
//! copies what it needs out of the mailbox (dropping the borrow), awaits the
//! socket op, then re-borrows to record the result. Combined with the engine's
//! synchronous pump (whose borrows complete before any worker runs), this keeps
//! the pump and the N sibling workers free of overlapping `RefCell` borrows.

use core::cell::RefCell;

use embassy_futures::select::{Either, select};
use embassy_net::tcp::TcpSocket;

use crate::{
  mailbox::{Command, Mailbox},
  stream_io::SlotWake,
};

/// Scratch buffer for one `read()` per readable wake. A modest size keeps the
/// worker's stack frame small; the engine's TCP socket RX ring (not this buffer)
/// bounds how much the peer can have in flight, and a larger payload simply takes
/// more read wakes to drain.
const READ_CHUNK: usize = 1024;

/// Drive one pool slot's socket forever, mediated by its mailbox.
///
/// Loops over the slot's lifecycle: wait for a [`Command::Listen`] /
/// [`Command::Dial`], complete the handshake, run the established read/write loop
/// until close/abort/peer-reset, reset the socket for reuse, and repeat.
///
/// An [`Command::Idle`] slot PARKS on its command wake rather than listening.
/// This diverges deliberately from a "every idle slot is a candidate acceptor"
/// design: the shared engine ([`memberlist_embedded::Engine`]) tracks exactly ONE
/// listener slot and only inspects that slot's `accepted_peer`. If a non-listener
/// idle slot also listened on the same port, the stack could hand an inbound SYN
/// to it instead of the engine's listener slot, and the engine would never map
/// that connection — wedging the inbound exchange and stranding the slot. So a
/// worker listens ONLY when the engine commands it via `stream.listen` (the
/// construction-time listener slot and each replenished listener), keeping exactly
/// one acceptor live at a time. The construction-time listener is seeded by
/// posting `Command::Listen(port)` to that slot's mailbox before the workers run
/// (see [`Memberlist::new`](crate::Memberlist::new)).
///
/// `cmd_wake` is the slot's command signal (engine → worker): the engine-view's
/// command posters pulse it so the worker promptly acts on a new directive.
/// `pump_wake` is the shared pump wake (worker → pump): the worker pulses it after
/// every mailbox change the pump must observe (inbound bytes ready, outbound
/// drained, a handshake settled, a FIN/reset), so the synchronous pump re-runs and
/// hands those bytes/state transitions to the machine.
pub(crate) async fn run_slot(
  socket: &mut TcpSocket<'_>,
  mb: &RefCell<Mailbox>,
  cmd_wake: &SlotWake,
  pump_wake: &SlotWake,
  socket_timeout: embassy_time::Duration,
) -> ! {
  // Bound EVERY blocking socket await (connect / write / flush / read) so a peer
  // that stops responding cannot wedge this worker: embassy-net aborts the socket
  // after `socket_timeout` of inactivity, turning the pending op into an error the
  // worker's failure arms convert to a teardown + `reset_socket` (so `reset_done`
  // becomes true and the slot is reusable again). Without this, a flush awaiting an
  // ACK from a black-holed peer never returns, the slot never resets, and the
  // round-1 reuse gate would withhold it forever. Set once; it persists across the
  // reuse loop below (the socket object is reused, not recreated). The timeout was
  // converted into the embassy-time tick domain and validated against the engine
  // deadlines at construction (see `Memberlist::new`), so it installs directly.
  socket.set_timeout(Some(socket_timeout));
  loop {
    // 1. Read the pending command. An idle slot parks on its wake (see the
    //    single-listener rationale above) until the engine posts Listen/Dial.
    let command = {
      let m = mb.borrow();
      m.command
    };

    match command {
      Command::Listen(port) => {
        // The slot is now in use (its socket leaves the clean Closed state), so it
        // is no longer reuse-ready until the next `reset_socket`. Clearing this
        // BEFORE the first await is what closes the abort/reuse race: once the
        // engine has handed this slot a `Listen`, `reuse_ready` reports `false`, so
        // a later same-pump `give` + re-take cannot re-`listen`/`connect` it (and
        // clobber a pending teardown) until the worker has actually reset it.
        mb.borrow_mut().reset_done = false;
        // `accept(port)` puts the socket in Listen and resolves once the
        // three-way handshake completes (the socket leaves Listen/SynSent/
        // SynReceived). Race it against the command wake so a command change (e.g.
        // the engine re-purposing this slot for a dial) can pre-empt a pending
        // accept. Consume the wake that delivered THIS command first: it is latched
        // (the command was read from the mailbox, not the signal), so without this
        // the race fires `Either::Second` immediately on the already-consumed signal.
        cmd_wake.reset();
        match select(socket.accept(port), cmd_wake.wait()).await {
          Either::First(Ok(())) => {
            let peer = socket.remote_endpoint().map(Into::into);
            {
              let mut m = mb.borrow_mut();
              // Accept gate: surface the peer ONLY with a known remote endpoint,
              // matching smoltcp's `may_send()`-gated `accepted_peer`.
              m.accepted_peer = peer;
              m.established = peer.is_some();
              m.open = peer.is_some();
              m.command = Command::Idle;
              m.sock_send_queue = socket.send_queue();
            }
            // The pump must observe the new accept so `check_listener` maps it.
            pump_wake.signal(());
            if peer.is_some() {
              run_established(socket, mb, cmd_wake, pump_wake).await;
            }
            reset_socket(socket, mb).await;
            pump_wake.signal(());
          }
          Either::First(Err(_)) => {
            // Listen failed (e.g. the socket was not in a clean state). Reset and
            // retry on the next loop.
            reset_socket(socket, mb).await;
          }
          Either::Second(()) => {
            // A new command arrived mid-accept; abandon the listen and re-read it.
            // `abort()` returns the socket to Closed so the next command's
            // accept/connect starts cleanly.
            socket.abort();
            // Ignoring Err: a best-effort RST flush on a socket with no peer is a
            // no-op; the reset below re-clears the mailbox regardless.
            let _ = socket.flush().await;
          }
        }
      }
      Command::Dial(remote) => {
        // The slot is now in use; it is no longer reuse-ready until `reset_socket`
        // runs (see the `Listen` arm). Clear it before the connect await so the
        // engine cannot reuse this slot mid-teardown.
        mb.borrow_mut().reset_done = false;
        // `connect(remote)` spans the handshake; on success the slot is established
        // and open. Race it against the command wake so a `Close`/`Abort` (the
        // engine retiring this dial at the exchange deadline) pre-empts a connect to
        // a black-holed peer; the socket inactivity timeout bounds the connect
        // itself as a backstop. On failure the engine reaps the slot via
        // `is_open() == false`. Consume the wake that delivered THIS `Dial` before
        // racing: it is latched (the command was read from the mailbox, not the
        // signal), so without this the race fires `Either::Second` immediately on
        // the already-consumed signal and abandons every dial.
        cmd_wake.reset();
        match select(socket.connect(remote), cmd_wake.wait()).await {
          Either::First(Ok(())) => {
            let peer = socket.remote_endpoint().map(Into::into);
            {
              let mut m = mb.borrow_mut();
              m.accepted_peer = peer;
              m.established = true;
              m.open = true;
              m.command = Command::Idle;
              m.sock_send_queue = socket.send_queue();
            }
            // The pump must observe the established slot to promote its exchange.
            pump_wake.signal(());
            run_established(socket, mb, cmd_wake, pump_wake).await;
            reset_socket(socket, mb).await;
            pump_wake.signal(());
          }
          Either::First(Err(_)) => {
            // Dial failed (refused, unreachable, or the socket timed out): mark the
            // slot closed so the engine reaps it, then reset the socket for reuse.
            {
              let mut m = mb.borrow_mut();
              m.established = false;
              m.open = false;
              m.command = Command::Idle;
            }
            // The pump must observe `open == false` to reap the failed dial.
            pump_wake.signal(());
            reset_socket(socket, mb).await;
          }
          Either::Second(()) => {
            // A command arrived mid-connect (the engine posted `Abort`, retiring this
            // dial at the exchange deadline): abandon the dial, reset the socket so
            // the next command starts cleanly, and wake the pump so it observes the
            // slot is reuse-ready (`reset_done`) and can dispatch any deferred dial —
            // without this the dial-churn would stall (freed slots never re-observed).
            reset_socket(socket, mb).await;
            pump_wake.signal(());
          }
        }
      }
      // A Close/Abort posted to an un-established slot has nothing to tear down;
      // clear it and park again.
      Command::Close | Command::Abort => {
        reset_socket(socket, mb).await;
      }
      // Idle: park until the engine posts a command. Re-read after the wake.
      Command::Idle => {
        cmd_wake.wait().await;
      }
    }
  }
}

/// The established read/write loop: shuttle bytes between the socket and the
/// mailbox until the peer closes/resets, or the engine commands a close/abort.
///
/// Returns once the connection is finished (so the caller resets the socket for
/// reuse). Mirrors `sock_send_queue` after every write and on every read wake so
/// the engine's `send_queue` drain-before-close gate stays accurate.
async fn run_established(
  socket: &mut TcpSocket<'_>,
  mb: &RefCell<Mailbox>,
  cmd_wake: &SlotWake,
  pump_wake: &SlotWake,
) {
  // Half-close state machine. A push/pull exchange is a full-duplex request/reply
  // over ONE connection that BOTH sides half-close: the requester writes its
  // request, FINs, then reads the reply + the peer's FIN; the responder reads the
  // request + the peer's FIN, then writes its reply and FINs. So:
  //
  // - `we_finned`   — the engine commanded `Close`, we sent our write-half FIN.
  //   We keep READING afterward (the peer's reply + FIN still arrive).
  // - `peer_finned` — we read the peer's FIN (`read` returned `Ok(0)`). We keep
  //   WRITING afterward (the responder still owes its reply).
  //
  // The connection is fully done — slot reaped — once BOTH halves are shut and the
  // engine has drained every inbound byte: `we_finned && peer_finned`, the
  // outbound ring is empty, and the inbound ring is empty. `abort` / a transport
  // error tears down immediately.
  let mut we_finned = false;
  let mut peer_finned = false;

  loop {
    // Snapshot the command and whether there is outbound work, releasing the
    // borrow before any await.
    let (command, has_outbound) = {
      let m = mb.borrow();
      (m.command, !m.outbound.is_empty())
    };

    match command {
      Command::Close => {
        // Graceful half-close: FIN our write half (the last queued segment carries
        // the FIN). The engine issues Close only after its own outbound bytes have
        // drained (its `send_queue` gate), so nothing is left to write. CRUCIAL: do
        // NOT return — the read half stays open so the peer's reply + FIN still
        // pump inbound. Consume the command and fall through to the I/O select.
        if !we_finned {
          socket.close();
          we_finned = true;
          {
            let mut m = mb.borrow_mut();
            m.command = Command::Idle;
            m.sock_send_queue = socket.send_queue();
          }
          pump_wake.signal(());
        }
      }
      Command::Abort => {
        socket.abort();
        // Ignoring Err: abort flush waits for the RST to be sent; failure means
        // it is already gone. The slot is reset next regardless.
        let _ = socket.flush().await;
        {
          let mut m = mb.borrow_mut();
          m.open = false;
          m.sock_send_queue = 0;
        }
        pump_wake.signal(());
        return;
      }
      // Idle / Listen / Dial while established: no transition to act on; the
      // command was already consumed at handshake time. Fall through to the I/O
      // select.
      Command::Idle | Command::Listen(_) | Command::Dial(_) => {}
    }

    // Full-close completion: both halves FIN'd and every byte handed off. Flush so
    // OUR FIN is actually sent and ACKed by the peer BEFORE the caller resets the
    // socket — `reset_socket`'s `abort()` would otherwise RST a still-pending FIN,
    // and the peer (mid graceful close) could see a reset instead of our clean FIN
    // and fail to deliver its inbound message. `flush` resolves once the FIN is
    // ACKed (the peer has received it), so the graceful handshake completes first.
    if we_finned && peer_finned {
      let drained = {
        let m = mb.borrow();
        m.inbound.is_empty() && m.outbound.is_empty()
      };
      if drained {
        // Ignoring Err: a reset during the final flush just means the peer already
        // tore down; our FIN is moot then and the slot is reset regardless.
        let _ = socket.flush().await;
        {
          let mut m = mb.borrow_mut();
          m.open = false;
          m.sock_send_queue = 0;
        }
        pump_wake.signal(());
        return;
      }
    }

    // Write-before-read, serialized. A push/pull exchange is request→reply (never
    // interleaved on one side), so the worker can fully drain its outbound, then
    // wait to read — which avoids racing `read` (needs `&mut socket`) against a
    // write in one `select`. CRUCIAL for correctness: reads use `socket.read()`
    // directly, NOT `wait_read_ready`. embassy-net's `poll_read_ready` is ready
    // only when `can_recv()` is true, which is FALSE for a pure peer-FIN (EOF with
    // no data) — so `wait_read_ready` never fires on a graceful close and the EOF
    // would be missed. `read()` correctly resolves `Ok(0)` on the FIN and parks on
    // the recv waker otherwise (which smoltcp wakes on the FIN).

    // 1. Drain outbound (only while our write half is open). Write the whole ring,
    //    then flush so the bytes are ACKed and `sock_send_queue` reaches 0 — the
    //    engine's `send_queue` drain-before-close gate depends on this (an ACK
    //    alone does not wake a readiness future, so without the flush the gate
    //    would never see the queue drain and the FIN would be deferred forever).
    if has_outbound && !we_finned {
      let chunk: alloc::vec::Vec<u8> = {
        let m = mb.borrow();
        m.outbound.iter().copied().collect()
      };
      match socket.write(&chunk).await {
        Ok(written) => {
          {
            let mut m = mb.borrow_mut();
            for _ in 0..written {
              m.outbound.pop_front();
            }
            m.sock_send_queue = socket.send_queue();
          }
          // Ignoring Err: a reset mid-flush means the connection is gone; the read
          // below observes it and tears the slot down. Re-sync the send queue from
          // the socket regardless.
          let _ = socket.flush().await;
          {
            let mut m = mb.borrow_mut();
            m.sock_send_queue = socket.send_queue();
          }
          pump_wake.signal(());
        }
        Err(_) => {
          {
            let mut m = mb.borrow_mut();
            m.open = false;
            m.sock_send_queue = 0;
          }
          pump_wake.signal(());
          return;
        }
      }
      // Loop to re-read the command (the engine may now Close) and re-drain.
      continue;
    }

    // 2. No outbound to write. If the peer has FIN'd there is nothing left to read
    //    either; park on the command wake (the engine drives the remaining
    //    teardown: it processes the EOF, finishes the exchange, and commands
    //    Close, which wakes us here).
    if peer_finned {
      cmd_wake.wait().await;
      continue;
    }

    // 3. Read the next inbound chunk, bounded by the ring's remaining capacity so a
    //    slow-to-drain engine cannot grow the ring past `inbound_cap`. Race the
    //    read against the command wake so a Close/Abort (or freshly-queued
    //    outbound) pre-empts a blocked read.
    let room = {
      let m = mb.borrow();
      m.inbound_cap.saturating_sub(m.inbound.len())
    };
    if room == 0 {
      // Ring full: park until the engine's `recv` drains it (it pulses this slot's
      // command wake on a drain) or a close/abort arrives.
      cmd_wake.wait().await;
      continue;
    }
    let cap = room.min(READ_CHUNK);
    let mut tmp = [0u8; READ_CHUNK];
    match select(socket.read(&mut tmp[..cap]), cmd_wake.wait()).await {
      // `Ok(0)` is the peer's FIN (embassy-net maps `RecvError::Finished` to
      // `Ok(0)`): record the one-shot EOF and stop reading. Do NOT tear down — the
      // responder still owes its reply (its engine queues it AFTER seeing this
      // EOF), and the engine still needs to drain any buffered inbound and observe
      // the EOF via `recv_finished`. `peer_finned` routes subsequent loops to the
      // write/park paths above.
      Either::First(Ok(0)) => {
        peer_finned = true;
        {
          let mut m = mb.borrow_mut();
          m.peer_fin = true;
          m.sock_send_queue = socket.send_queue();
        }
        pump_wake.signal(());
      }
      Either::First(Ok(n)) => {
        {
          let mut m = mb.borrow_mut();
          m.inbound.extend(tmp[..n].iter().copied());
          m.sock_send_queue = socket.send_queue();
        }
        // The pump must observe the new inbound bytes to feed the machine.
        pump_wake.signal(());
      }
      Either::First(Err(_)) => {
        // A reset / invalid-state read: the connection FAILED (a peer RST, not a
        // graceful close). Mark the slot closed (`open = false`) but DO NOT set
        // `peer_fin`: a reset is NOT an orderly end-of-stream. Latching `peer_fin`
        // here would make `EmbassyStream::recv_finished` (`peer_fin &&
        // inbound.is_empty()`) report a clean EOF for a reset once the buffered
        // inbound drained, and the machine maps a `UserMessage` transport EOF to a
        // SUCCESSFUL completion (see `StreamEndpoint::handle_dial_failed`'s contrast
        // note) — so a mid-send RST would falsely resolve `send_reliable` as Ok.
        // Surfacing only `open = false` keeps the reset a failure: the exchange's
        // bridge elapses at its `stream_timeout` deadline and the machine emits
        // `StreamAction::Abort`, exactly as the smoltcp driver treats a RST-`Closed`
        // socket (its `recv_finished` likewise excludes the reset state). Any
        // already-buffered inbound still drains via `recv` before then; it simply is
        // not followed by a spurious EOF anchor.
        {
          let mut m = mb.borrow_mut();
          m.open = false;
          m.sock_send_queue = 0;
        }
        pump_wake.signal(());
        return;
      }
      // Command wake fired (Close/Abort or new outbound): loop to handle it. The
      // in-flight `read` future is dropped; smoltcp keeps the received bytes
      // buffered, so the next read still sees them — no data is lost.
      Either::Second(()) => {}
    }
  }
}

/// Return the socket to a clean Closed state and reset the mailbox for the slot's
/// next reuse.
///
/// `abort()` is idempotent on an already-closed socket and guarantees the socket
/// leaves any half-open/closing state so the next `accept`/`connect` starts
/// fresh; the subsequent `flush` lets a pending RST go out. The mailbox reset
/// clears every per-connection field (preserving ring capacities).
async fn reset_socket(socket: &mut TcpSocket<'_>, mb: &RefCell<Mailbox>) {
  socket.abort();
  // Ignoring Err: flushing the RST on an already-dead socket is a no-op; the
  // socket is returned to Closed either way.
  let _ = socket.flush().await;
  mb.borrow_mut().reset();
}
