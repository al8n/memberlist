use super::EndpointOptions;

#[test]
fn initial_incarnation_default_is_one() {
  let opts = EndpointOptions::<(), ()>::new((), ());
  assert_eq!(opts.initial_incarnation(), 1);
}

#[test]
fn with_initial_incarnation_accepts_lower_half() {
  // A small seed (the realistic case) and the exact upper boundary of the
  // accepted lower half both round-trip.
  let small = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(7);
  assert_eq!(small.initial_incarnation(), 7);
  let boundary = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX / 2);
  assert_eq!(boundary.initial_incarnation(), u32::MAX / 2);
}

#[test]
#[should_panic(expected = "lower half")]
fn with_initial_incarnation_rejects_upper_half() {
  // Just past the midpoint is rejected — the smallest value outside the
  // reserved headroom.
  let _ = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX / 2 + 1);
}

#[test]
#[should_panic(expected = "lower half")]
fn with_initial_incarnation_rejects_near_max() {
  // `u32::MAX - 1` is only two bumps from wrapping; the previous MAX-only
  // guard wrongly accepted it.
  let _ = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX - 1);
}

#[test]
#[should_panic(expected = "lower half")]
fn with_initial_incarnation_rejects_u32_max() {
  let _ = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX);
}

#[test]
fn every_builder_round_trips_through_its_accessor() {
  use core::time::Duration;
  use std::net::SocketAddr;

  use bytes::Bytes;
  use smol_str::SmolStr;

  use crate::typed::{DelegateVersion, Meta, ProtocolVersion};

  let local = SmolStr::new("local");
  let advertise = SocketAddr::from(([127, 0, 0, 1], 7946));
  let meta = Meta::try_from(Bytes::from_static(b"meta")).expect("valid meta");

  let opts = EndpointOptions::<SmolStr, SocketAddr>::new(local.clone(), advertise)
    .with_initial_meta(meta.clone())
    .with_initial_local_state(Bytes::from_static(b"state"))
    .with_suspicion_mult(5)
    .with_suspicion_max_timeout_mult(7)
    .with_probe_interval(Duration::from_millis(100))
    .with_gossip_to_the_dead_time(Duration::from_secs(30))
    .with_dead_node_reclaim_time(Duration::from_secs(60))
    .with_awareness_max_multiplier(9)
    .with_indirect_checks(4)
    .with_probe_timeout(Duration::from_millis(250))
    .with_stream_timeout(Duration::from_secs(15))
    .with_max_stream_frame_size(70_000)
    .with_gossip_mtu(1300)
    .with_gossip_interval(Duration::from_millis(200))
    .with_gossip_nodes(3)
    .with_meta_max_size(400)
    .with_accept_handshake_deadline(Duration::from_secs(11))
    .with_push_pull_interval(Duration::from_secs(45))
    .with_retransmit_mult(6)
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1)
    .with_rng_seed(0xABCD);

  assert_eq!(opts.local_id_ref(), &local);
  assert_eq!(opts.advertise_addr_ref(), &advertise);
  assert_eq!(opts.initial_meta_ref(), &meta);
  assert_eq!(opts.initial_local_state(), b"state");
  assert_eq!(opts.initial_local_state_bytes().as_ref(), b"state");
  assert_eq!(opts.suspicion_mult(), 5);
  assert_eq!(opts.suspicion_max_timeout_mult(), 7);
  assert_eq!(opts.probe_interval(), Duration::from_millis(100));
  assert_eq!(opts.gossip_to_the_dead_time(), Duration::from_secs(30));
  assert_eq!(opts.dead_node_reclaim_time(), Duration::from_secs(60));
  assert_eq!(opts.awareness_max_multiplier(), 9);
  assert_eq!(opts.indirect_checks(), 4);
  assert_eq!(opts.probe_timeout(), Duration::from_millis(250));
  assert_eq!(opts.stream_timeout(), Duration::from_secs(15));
  assert_eq!(opts.max_stream_frame_size(), 70_000);
  assert_eq!(opts.gossip_mtu(), 1300);
  assert_eq!(opts.gossip_interval(), Duration::from_millis(200));
  assert_eq!(opts.gossip_nodes(), 3);
  assert_eq!(opts.meta_max_size(), 400);
  assert_eq!(opts.accept_handshake_deadline(), Duration::from_secs(11));
  assert_eq!(opts.push_pull_interval(), Duration::from_secs(45));
  assert_eq!(opts.retransmit_mult(), 6);
  assert_eq!(opts.protocol_version(), ProtocolVersion::V1);
  assert_eq!(opts.delegate_version(), DelegateVersion::V1);
  assert_eq!(opts.rng_seed(), Some(0xABCD));
}

#[test]
fn rng_seed_setter_family_round_trips() {
  let mut opts = EndpointOptions::<(), ()>::new((), ());
  assert_eq!(opts.rng_seed(), None);
  opts.set_rng_seed(11);
  assert_eq!(opts.rng_seed(), Some(11));
  opts.update_rng_seed(Some(22));
  assert_eq!(opts.rng_seed(), Some(22));
  opts.update_rng_seed(None);
  assert_eq!(opts.rng_seed(), None);
  opts.set_rng_seed(33);
  opts.clear_rng_seed();
  assert_eq!(opts.rng_seed(), None);

  assert_eq!(
    EndpointOptions::<(), ()>::new((), ())
      .maybe_rng_seed(Some(44))
      .rng_seed(),
    Some(44)
  );
  assert_eq!(
    EndpointOptions::<(), ()>::new((), ())
      .maybe_rng_seed(None)
      .rng_seed(),
    None
  );
  assert_eq!(
    EndpointOptions::<(), ()>::new((), ())
      .with_rng_seed(55)
      .rng_seed(),
    Some(55)
  );
}
