use std::net::SocketAddr;

use bytes::Bytes;
use smol_str::SmolStr;

use super::PushPullSnapshot;
use crate::typed::{PushNodeState, State};

fn addr(port: u16) -> SocketAddr {
  SocketAddr::from(([127, 0, 0, 1], port))
}

#[test]
fn push_pull_snapshot_join_round_trips() {
  let states = std::vec![
    PushNodeState::new(1, SmolStr::new("a"), addr(1), State::Alive),
    PushNodeState::new(2, SmolStr::new("b"), addr(2), State::Dead),
  ];
  let snap = PushPullSnapshot::new(true, states, Bytes::from_static(b"ud"));
  assert!(snap.is_join());
  assert_eq!(snap.states_slice().len(), 2);
  assert_eq!(snap.user_data(), b"ud");
  assert_eq!(snap.user_data_bytes().as_ref(), b"ud");

  let (join, decoded_states, ud) = snap.into_parts();
  assert!(join);
  assert_eq!(decoded_states.len(), 2);
  assert_eq!(ud.as_ref(), b"ud");
}

#[test]
fn push_pull_snapshot_refresh_can_be_empty() {
  let snap = PushPullSnapshot::<SmolStr, SocketAddr>::new(false, std::vec![], Bytes::new());
  assert!(!snap.is_join());
  assert!(snap.states_slice().is_empty());
  assert!(snap.user_data().is_empty());
}
