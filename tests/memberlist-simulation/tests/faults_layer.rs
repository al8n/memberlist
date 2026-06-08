//! The probabilistic fault layer drops/duplicates/jitters and stays deterministic.
use memberlist_simulation::Cluster;
use smol_str::format_smolstr;
use std::{net::SocketAddr, time::Duration};

fn addr(p: u16) -> SocketAddr {
  format!("127.0.0.1:{p}").parse().unwrap()
}

fn converged_count(drop_per_mille: u32, seed: u64) -> usize {
  let n = 5u16;
  let addrs: Vec<SocketAddr> = (0..n).map(|i| addr(31000 + i)).collect();
  let ids: Vec<_> = (0..n).map(|i| format_smolstr!("n{i}")).collect();
  let mut c = Cluster::new();
  c.seed_faults(seed);
  c.set_drop_per_mille(drop_per_mille);
  for (id, &a) in ids.iter().zip(&addrs) {
    c.add_node(id.clone(), a);
  }
  for i in 1..n as usize {
    c.inject_alive(addrs[0], ids[i].clone(), addrs[i], 1);
  }
  for _ in 0..500 {
    c.step();
  }
  let mut known = 0;
  for &h in &addrs {
    for id in &ids {
      if c.member(h, id).is_some() {
        known += 1;
      }
    }
  }
  known
}

#[test]
fn heavy_drop_slows_propagation_but_is_deterministic() {
  // Same seed => identical outcome.
  assert_eq!(converged_count(800, 42), converged_count(800, 42));
  // Heavy drop in a short run converges less than (or equal to) no drop.
  assert!(converged_count(800, 42) <= converged_count(0, 42));
}

/// A stream dial must not consume the one-shot datagram-drop token.
///
/// `drop_next` is a datagram-only fault; a reliable-stream dial is cut only by
/// a partition. If a dial incorrectly consumed the token, the next real UDP
/// datagram from that node would pass through even though the fault was armed
/// for it.
#[test]
fn stream_dial_does_not_consume_drop_next_token() {
  let a0 = addr(31900);
  let a1 = addr(31901);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node("n1".into(), a1);
  c.inject_alive(a0, "n1".into(), a1, 1);
  c.inject_alive(a1, "n0".into(), a0, 1);
  for _ in 0..200 {
    c.step();
  }

  // Arm the one-shot drop for datagrams FROM a0.
  c.drop_next_datagram_from(a0);
  assert!(
    c.drop_next_armed(a0),
    "precondition: drop_next must be armed"
  );

  // Trigger a stream dial from a0 to a1 and step to process it. The dial path
  // must NOT consume the one-shot token.
  c.trigger_push_pull(a0, a1);
  c.step();

  assert!(
    c.drop_next_armed(a0),
    "stream dial must not consume the one-shot datagram-drop token"
  );

  // Confirm the token IS consumed by the next real datagram from a0.
  // Use a latency window so we can observe the queue count.
  c.set_latency(Duration::from_millis(50));
  c.send(a0, a1, bytes::Bytes::from_static(b"probe"));
  // Token consumed at enqueue — datagram is dropped, not queued.
  assert_eq!(
    c.queued_to(a1),
    0,
    "the datagram must have been dropped by the one-shot token"
  );
  assert!(
    !c.drop_next_armed(a0),
    "token must be consumed by the datagram, not still armed"
  );
}
