use super::{Command, EmbassyStream, Mailbox, SlotId, SlotWake};
use alloc::vec::Vec;
use core::{cell::RefCell, net::SocketAddr};
use memberlist_embedded::{StreamIo, StreamIoError};

/// Build `n` fresh mailboxes (each `cap` bytes per ring) and `n` wakes for a
/// view. Returns them so the test frame owns them for the view's lifetime.
fn slots(n: usize, cap: usize) -> (Vec<RefCell<Mailbox>>, Vec<SlotWake>) {
  let mb = (0..n)
    .map(|_| RefCell::new(Mailbox::new(cap, cap)))
    .collect();
  let wakes = (0..n).map(|_| SlotWake::new()).collect();
  (mb, wakes)
}

fn sa(last: u8) -> SocketAddr {
  SocketAddr::from(([169, 254, 0, last], 7946))
}

#[test]
fn slot_id_exposes_its_raw_index() {
  assert_eq!(SlotId(3).index(), 3);
}

/// The driver-owned free-list is a faithful LIFO: `give` then `take_free`
/// returns the same slot, and `free_count` tracks the length.
#[test]
fn free_list_take_give_and_count() {
  let (mb, wakes) = slots(2, 64);
  let mut free = Vec::new();
  let mut view = EmbassyStream::new(&mb, &wakes, &mut free);

  assert_eq!(view.free_count(), 0);
  assert!(view.take_free().is_none(), "an empty free-list yields None");

  view.give(SlotId(0));
  view.give(SlotId(1));
  assert_eq!(view.free_count(), 2);
  assert_eq!(
    view.take_free(),
    Some(SlotId(1)),
    "LIFO: last given comes first"
  );
  assert_eq!(view.take_free(), Some(SlotId(0)));
  assert_eq!(view.free_count(), 0);
}

/// `reuse_ready` reads the slot's `reset_done`: true for a fresh slot, false
/// once the worker has begun using it (cleared `reset_done`).
#[test]
fn reuse_ready_tracks_reset_done() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let view = EmbassyStream::new(&mb, &wakes, &mut free);

  assert!(view.reuse_ready(SlotId(0)), "a fresh slot is reuse-ready");
  mb[0].borrow_mut().reset_done = false;
  assert!(
    !view.reuse_ready(SlotId(0)),
    "a slot whose worker is mid-use (reset_done cleared) is not reuse-ready"
  );
}

/// `listen` posts a `Listen` command and wakes the worker for a valid port, and
/// rejects port 0 with `Unaddressable` without posting.
#[test]
fn listen_posts_for_a_valid_port_and_rejects_port_zero() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let mut view = EmbassyStream::new(&mb, &wakes, &mut free);

  assert!(matches!(
    view.listen(SlotId(0), 0),
    Err(StreamIoError::Unaddressable)
  ));
  assert_eq!(
    mb[0].borrow().command,
    Command::Idle,
    "a rejected listen does not post a command"
  );

  view
    .listen(SlotId(0), 7946)
    .expect("a non-zero port is accepted");
  assert_eq!(mb[0].borrow().command, Command::Listen(7946));
  assert!(
    wakes[0].signaled(),
    "the worker is woken for the new directive"
  );
}

/// `connect` posts a `Dial` to the slot regardless of the engine's requested
/// local port (embassy-net owns ephemeral-port selection).
#[test]
fn connect_posts_a_dial() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let mut view = EmbassyStream::new(&mb, &wakes, &mut free);

  view.connect(SlotId(0), sa(2), 1234).expect("connect posts");
  assert_eq!(mb[0].borrow().command, Command::Dial(sa(2)));
  assert!(wakes[0].signaled());
}

/// The established-state predicates read straight from the mailbox: an
/// established, open slot with a known peer reports each accordingly.
#[test]
fn lifecycle_predicates_reflect_the_mailbox() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let view = EmbassyStream::new(&mb, &wakes, &mut free);

  // A fresh slot is neither open nor established and has no peer.
  assert!(!view.is_open(SlotId(0)));
  assert!(!view.is_established(SlotId(0)));
  assert!(view.accepted_peer(SlotId(0)).is_none());

  {
    let mut m = mb[0].borrow_mut();
    m.established = true;
    m.open = true;
    m.accepted_peer = Some(sa(2));
  }
  assert!(view.is_open(SlotId(0)));
  assert!(view.is_established(SlotId(0)));
  assert_eq!(view.accepted_peer(SlotId(0)), Some(sa(2)));
  // Established with room ⇒ may_send; established and not (peer-FIN'd + drained)
  // ⇒ may_recv.
  assert!(view.may_send(SlotId(0)));
  assert!(view.may_recv(SlotId(0)));
}

/// `send` fills the outbound ring up to capacity (partial accept on a full
/// ring) and `may_send` reports false once the ring is full.
#[test]
fn send_respects_outbound_capacity() {
  let (mb, wakes) = slots(1, 4);
  let mut free = Vec::new();
  let mut view = EmbassyStream::new(&mb, &wakes, &mut free);
  mb[0].borrow_mut().established = true;

  assert_eq!(view.send(SlotId(0), b"abcdef"), 4, "only 4 of 6 bytes fit");
  assert_eq!(mb[0].borrow().outbound.len(), 4);
  assert!(
    !view.may_send(SlotId(0)),
    "a full outbound ring cannot accept more"
  );
  assert_eq!(view.send(SlotId(0), b"z"), 0, "a full ring accepts nothing");
}

/// `recv` drains buffered inbound up to the caller's buffer length and returns
/// `None` (not end-of-stream) when the ring is empty.
#[test]
fn recv_drains_inbound_and_returns_none_when_empty() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let mut view = EmbassyStream::new(&mb, &wakes, &mut free);

  assert!(
    view.recv(SlotId(0), &mut [0u8; 4]).is_none(),
    "empty ⇒ None"
  );

  mb[0].borrow_mut().inbound.extend(b"hello".iter().copied());
  let mut buf = [0u8; 3];
  assert_eq!(view.recv(SlotId(0), &mut buf), Some(3));
  assert_eq!(&buf, b"hel");
  let mut rest = [0u8; 8];
  assert_eq!(view.recv(SlotId(0), &mut rest), Some(2));
  assert_eq!(&rest[..2], b"lo");
  assert!(view.recv(SlotId(0), &mut rest).is_none());
}

/// `send_queue` sums the un-written outbound bytes and the worker's mirror of
/// the socket's un-ACKed send queue, reaching zero only when both drain.
#[test]
fn send_queue_sums_unwritten_and_unacked() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let view = EmbassyStream::new(&mb, &wakes, &mut free);

  assert_eq!(view.send_queue(SlotId(0)), 0);
  {
    let mut m = mb[0].borrow_mut();
    m.outbound.extend(b"ab".iter().copied());
    m.sock_send_queue = 3;
  }
  assert_eq!(view.send_queue(SlotId(0)), 5, "2 unwritten + 3 un-ACKed");
}

/// `recv_finished` is the one-shot EOF: true only with a peer FIN AND a drained
/// inbound ring, so the FIN is delivered after the data, exactly once.
#[test]
fn recv_finished_is_the_drained_peer_fin() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let view = EmbassyStream::new(&mb, &wakes, &mut free);

  assert!(!view.recv_finished(SlotId(0)), "no FIN yet ⇒ not finished");

  {
    let mut m = mb[0].borrow_mut();
    m.peer_fin = true;
    m.inbound.extend(b"tail".iter().copied());
  }
  assert!(
    !view.recv_finished(SlotId(0)),
    "a FIN with buffered bytes still pending is not yet finished"
  );
  // `may_recv` stays true while buffered bytes remain even after the FIN.
  assert!(view.may_recv(SlotId(0)));

  mb[0].borrow_mut().inbound.clear();
  assert!(
    view.recv_finished(SlotId(0)),
    "FIN + drained inbound ⇒ end-of-stream"
  );
}

/// `close` and `abort` post their respective commands and wake the worker.
#[test]
fn close_and_abort_post_commands() {
  let (mb, wakes) = slots(1, 64);
  let mut free = Vec::new();
  let mut view = EmbassyStream::new(&mb, &wakes, &mut free);

  view.close(SlotId(0));
  assert_eq!(mb[0].borrow().command, Command::Close);
  assert!(wakes[0].signaled());

  wakes[0].reset();
  view.abort(SlotId(0));
  assert_eq!(mb[0].borrow().command, Command::Abort);
  assert!(wakes[0].signaled());
}
