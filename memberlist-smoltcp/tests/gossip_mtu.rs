//! Gossip ingress must honour a configured gossip MTU larger than 2 KiB.
//!
//! `EndpointConfig::gossip_mtu` is configurable above 2 KiB, so the driver's
//! UDP receive scratch is sized from it. This matters because smoltcp's
//! `udp::Socket::recv_slice` POPS the datagram before checking the caller's
//! slice length and returns `RecvError::Truncated` for an oversized one: a
//! datagram larger than the scratch is consumed and lost, never delivered. A
//! scratch sized from the configured MTU therefore receives any in-budget
//! datagram intact, and a truncation (an over-budget peer datagram) is skipped
//! without abandoning the rest of the rx queue.

mod harness;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use bytes::Bytes;
use memberlist_proto::{EndpointConfig, Event};
use memberlist_smoltcp::{Config, Memberlist, TransformOptions};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// A gossiped user-data payload larger than 2 KiB (but within a 4 KiB gossip
/// MTU) is delivered to the peer as `Event::UserPacket`.
///
/// The on-wire datagram for a ~3 KiB `UserData` frame exceeds a 2 KiB receive
/// buffer. A receiver whose scratch is fixed at 2 KiB pops and discards the
/// datagram (smoltcp returns `Truncated` after dequeuing it), so the
/// `UserPacket` never surfaces and this test would time out. Sizing the receive
/// scratch from the configured gossip MTU lets the full datagram be read and
/// the payload arrive intact.
#[test]
fn oversized_gossip_datagram_is_received() {
  // The join/gossip settles in a few zero-latency ticks; this bounds a wedged
  // run so the test fails loudly instead of hanging.
  const BUDGET: u32 = 600;
  // 4 KiB gossip MTU — well above a 2 KiB receive buffer, the size at which an
  // oversized datagram would be truncated and lost.
  const GOSSIP_MTU: usize = 4096;
  // Payload size: above 2 KiB so its datagram overflows the old buffer, and
  // comfortably within the 4 KiB MTU budget (frame = payload + a few header
  // bytes) so the machine accepts and gossips it.
  const PAYLOAD: usize = 3000;

  // Device MTU large enough to carry the whole ~3 KiB UDP datagram in ONE link
  // frame: this smoltcp build does not enable IPv4 fragmentation, so an
  // over-MTU datagram could not traverse a 1500-MTU link at all. The point
  // under test is the driver's RECEIVE-buffer sizing, not IP fragmentation, so
  // the link is widened to isolate it.
  let (mut da, mut db) = harness::link(4096);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // Gossip frequently so the broadcast rides out quickly; the large gossip MTU
  // is the knob under test.
  let mk = |id: &str, ip: u8, seed: u64| {
    EndpointConfig::new(SmolStr::new(id), addr(ip, 7946))
      .with_rng_seed(seed)
      .with_gossip_mtu(GOSSIP_MTU)
      .with_gossip_interval(Duration::from_millis(20))
  };

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1, 1),
    &mut da,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    mk("b", 2, 2),
    &mut db,
    now,
  );

  a.start(now);
  b.start(now);

  // Establish membership without the TCP join path so gossip flows immediately
  // (this isolates the UDP ingress under test).
  a.inject_alive(SmolStr::new("b"), addr(2, 7946), clk.now());
  b.inject_alive(SmolStr::new("a"), addr(1, 7946), clk.now());

  // A queues a >2 KiB user payload for piggyback gossip. A distinctive byte
  // pattern lets the receiver assert the payload arrived intact (not just
  // truncated to the old 2 KiB).
  let payload = Bytes::from(vec![0xABu8; PAYLOAD]);
  a.queue_user_broadcast(payload.clone())
    .expect("a 3 KiB payload fits a 4 KiB gossip MTU");

  // Drive both nodes until B surfaces the user packet.
  let mut got: Option<Bytes> = None;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    while let Some(ev) = b.poll_event() {
      if let Event::UserPacket(p) = ev {
        got = Some(p.data_ref().clone());
      }
    }
    if got.is_some() {
      break;
    }
    clk.advance_ms(10);
  }

  let received = got.expect(
    "B never received the >2 KiB gossip user payload: the UDP ingress dropped \
     the oversized datagram instead of reading it into an MTU-sized buffer",
  );
  assert_eq!(
    received.len(),
    PAYLOAD,
    "received payload was truncated: got {} bytes, expected {}",
    received.len(),
    PAYLOAD
  );
  assert_eq!(received, payload, "received payload bytes differ from sent");
}

/// A gossip datagram larger than the DEFAULT UDP tx payload arena is still SENT
/// (not silently dropped) because the driver scales the arena up from the
/// configured gossip MTU at construction.
///
/// The machine emits gossip whose on-wire datagram can reach
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`. smoltcp's `udp::send_slice`
/// enqueues into a fixed payload arena and returns `BufferFull` — which the
/// driver drops silently, gossip being best-effort — for any datagram larger
/// than that arena's capacity. The default arena is `8 * 1500 = 12 000` bytes,
/// so a ~14 KiB datagram (well within a 16 KiB gossip MTU) would be rejected and
/// the payload would never leave the node. Sizing each arena to at least
/// `udp_*_packets * (gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD)` lets the in-budget
/// datagram enqueue and reach the peer. End-to-end receipt of the distinctive
/// payload proves the SEND side was not dropped.
///
/// With the un-scaled default arena the `send_slice` for this datagram fails
/// silently and the peer never receives the `UserPacket`; this test would then
/// hang to its budget and fail.
#[test]
fn oversized_gossip_datagram_is_sent() {
  const BUDGET: u32 = 600;
  // 16 KiB gossip MTU: the scaled tx arena becomes 8 * (16384 + overhead) ≫
  // the default 12 000-byte arena.
  const GOSSIP_MTU: usize = 16 * 1024;
  // Payload comfortably inside the MTU budget (frame = payload + a few header
  // bytes) yet whose on-wire datagram (~14 KiB) far exceeds the default
  // 12 000-byte tx arena, so an un-scaled arena would reject the send.
  const PAYLOAD: usize = 14 * 1024;

  // Link wide enough to carry the whole ~14 KiB datagram in one frame: this
  // build has no IPv4 fragmentation, so an over-MTU datagram could not traverse.
  // The point under test is the SEND-buffer sizing, not IP fragmentation.
  let (mut da, mut db) = harness::link(65535);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  let mk = |id: &str, ip: u8, seed: u64| {
    EndpointConfig::new(SmolStr::new(id), addr(ip, 7946))
      .with_rng_seed(seed)
      .with_gossip_mtu(GOSSIP_MTU)
      .with_gossip_interval(Duration::from_millis(20))
  };

  // Both nodes use the DEFAULT Config (default UDP arenas) so the scaling floor,
  // not an explicitly enlarged arena, is what makes the send fit.
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1, 1),
    &mut da,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    mk("b", 2, 2),
    &mut db,
    now,
  );

  a.start(now);
  b.start(now);

  a.inject_alive(SmolStr::new("b"), addr(2, 7946), clk.now());
  b.inject_alive(SmolStr::new("a"), addr(1, 7946), clk.now());

  // A queues the large payload for gossip. Its on-wire datagram exceeds the
  // default tx arena; only the MTU-scaled arena can enqueue it.
  let payload = Bytes::from(vec![0xCDu8; PAYLOAD]);
  a.queue_user_broadcast(payload.clone())
    .expect("a 14 KiB payload fits a 16 KiB gossip MTU");

  let mut got: Option<Bytes> = None;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    while let Some(ev) = b.poll_event() {
      if let Event::UserPacket(p) = ev {
        got = Some(p.data_ref().clone());
      }
    }
    if got.is_some() {
      break;
    }
    clk.advance_ms(10);
  }

  let received = got.expect(
    "B never received the large gossip user payload: A's UDP send dropped the \
     datagram because the tx payload arena was not scaled to the configured \
     gossip MTU",
  );
  assert_eq!(
    received.len(),
    PAYLOAD,
    "received payload was truncated: got {} bytes, expected {}",
    received.len(),
    PAYLOAD
  );
  assert_eq!(received, payload, "received payload bytes differ from sent");
}
