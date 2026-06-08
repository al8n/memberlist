//! Same seed produces an identical run: the harness is a pure function of its inputs.
use memberlist_simulation::Cluster;
use smol_str::{SmolStr, format_smolstr};
use std::{net::SocketAddr, time::Duration};

fn addr(p: u16) -> SocketAddr {
  format!("127.0.0.1:{p}").parse().unwrap()
}

fn run() -> Vec<(SocketAddr, SmolStr, Option<u32>)> {
  let n = 5u16;
  let addrs: Vec<SocketAddr> = (0..n).map(|i| addr(30000 + i)).collect();
  let ids: Vec<SmolStr> = (0..n).map(|i| format_smolstr!("node{i}")).collect();
  let mut c = Cluster::new();
  c.set_latency(Duration::from_millis(40));
  for (id, &a) in ids.iter().zip(&addrs) {
    c.add_node(id.clone(), a);
  }
  for i in 1..n as usize {
    c.inject_alive(addrs[0], ids[i].clone(), addrs[i], 1);
  }
  for _ in 0..2000 {
    c.step();
  }
  let mut out = Vec::new();
  for &h in &addrs {
    for id in &ids {
      out.push((h, id.clone(), c.get_node_incarnation(h, id)));
    }
  }
  out
}

#[test]
fn same_seed_is_reproducible() {
  assert_eq!(run(), run(), "two runs of the same deterministic cluster diverged");
}

#[test]
fn initial_incarnation_supersedes() {
  let a = addr(30100);
  let b = addr(30101);
  let mut c = Cluster::new();
  c.add_node("a".into(), a);
  // Node b starts with initial incarnation 7 via the builder.
  c.add_node_with("b".into(), b, |opts| opts.with_initial_incarnation(7));
  c.inject_alive(a, "b".into(), b, 1); // stale alive at incarnation 1 must NOT win
  for _ in 0..2000 {
    c.step();
  }
  assert!(
    c.get_node_incarnation(a, &"b".into()).unwrap() >= 7,
    "a should hold b at its real incarnation >= 7, not the stale 1"
  );
}
