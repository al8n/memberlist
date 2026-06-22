use super::*;

#[test]
fn meta_too_large_accessors() {
  let m = MetaTooLarge::new(600, 512);
  assert_eq!(m.meta_len(), 600);
  assert_eq!(m.max(), 512);
}

#[test]
fn meta_too_large_init_error_display_and_from() {
  let payload = MetaTooLarge::new(600, 512);
  // `derive_more::From` (no `#[from(skip)]` on this variant) builds the error.
  let err: EndpointInitError = payload.into();
  assert!(matches!(err, EndpointInitError::MetaTooLarge(_)));
  // The Display string reads the payload through `meta_len()` / `max()`.
  assert_eq!(
    err.to_string(),
    "initial_meta (600 bytes) exceeds meta_max_size (512 bytes)"
  );
}

#[test]
fn gossip_mtu_bound_accessors() {
  let g = GossipMtuBound::new(10, 32);
  assert_eq!(g.configured(), 10);
  assert_eq!(g.bound(), 32);
}

#[test]
fn gossip_mtu_bound_init_error_displays_read_accessors() {
  // Both variants format `configured()` and `bound()`; `#[from(skip)]` means
  // they must be constructed explicitly rather than via `From`.
  let too_small = EndpointInitError::GossipMtuTooSmall(GossipMtuBound::new(10, 64));
  assert_eq!(
    too_small.to_string(),
    "gossip_mtu (10 bytes) is below the minimum (64 bytes) for the mandatory control packets"
  );
  let too_large = EndpointInitError::GossipMtuTooLarge(GossipMtuBound::new(70000, 65535));
  assert_eq!(
    too_large.to_string(),
    "gossip_mtu (70000 bytes) exceeds the maximum UDP datagram payload (65535 bytes)"
  );
}

#[test]
fn size_exceeded_accessors_and_error_displays() {
  let se = SizeExceeded::new(900, 800);
  assert_eq!(se.size(), 900);
  assert_eq!(se.limit(), 800);
  // The size-limit `Error` variants format `_0.0` / `_0.1` (size then limit).
  assert_eq!(
    Error::MetaExceedsCap(se).to_string(),
    "meta size 900 exceeds per-endpoint cap 800"
  );
  assert_eq!(
    Error::AckPayloadExceedsMtu(se).to_string(),
    "encoded ack (900 bytes) exceeds the gossip packet budget (800 bytes)"
  );
}
