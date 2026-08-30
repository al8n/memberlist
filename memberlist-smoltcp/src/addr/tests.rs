use super::*;
use core::net::{IpAddr, Ipv4Addr, SocketAddr};

#[test]
fn socket_addr_round_trips_through_ip_endpoint() {
  let sa = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 7946);
  let ep = to_endpoint(sa);
  assert_eq!(from_endpoint(ep), sa);
}

#[test]
fn ipv6_socket_addr_round_trips() {
  use core::net::Ipv6Addr;
  let sa = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 7946);
  assert_eq!(from_endpoint(to_endpoint(sa)), sa);
}

#[test]
fn machine_instant_round_trips_through_smoltcp_instant() {
  let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_millis(123_456));
  let s = to_smoltcp_instant(now);
  assert_eq!(from_smoltcp_instant(s), now);
}

#[test]
fn extreme_instant_saturates_without_wrapping() {
  use smoltcp::time::Instant as SmoltcpInstant;

  // `since_origin`'s milliseconds exceed `i64::MAX / 1000` -- the real bound,
  // since smoltcp's `Instant::from_millis` scales millis -> micros by 1000
  // before storing them in an `i64`. `i64::MAX` millis alone would overflow
  // that multiply and wrap negative (the same-class bug as the duration
  // conversion below, guarded before this fix only by a release-silent
  // `debug_assert` with the wrong bound).
  let extreme = memberlist_proto::Instant::from_origin(core::time::Duration::from_millis(u64::MAX));
  let converted = to_smoltcp_instant(extreme);
  assert!(
    converted.total_micros() > 0,
    "an extreme instant must saturate to a positive smoltcp instant, got {converted:?}"
  );
  assert_eq!(converted, SmoltcpInstant::from_millis(i64::MAX / 1000));
}

#[test]
fn duration_converts_to_smoltcp_micros() {
  use smoltcp::time::Duration as SmoltcpDuration;
  // The default close_timeout (10 s), the value the driver installs as the socket
  // inactivity timeout, converts exactly.
  assert_eq!(
    to_smoltcp_duration(core::time::Duration::from_secs(10)),
    SmoltcpDuration::from_secs(10)
  );
  // Sub-second precision is preserved at microsecond granularity.
  assert_eq!(
    to_smoltcp_duration(core::time::Duration::from_micros(1_500_250)),
    SmoltcpDuration::from_micros(1_500_250)
  );
  // A non-zero timeout never converts to smoltcp's zero (which it treats as an
  // immediate abort of every socket).
  assert_ne!(
    to_smoltcp_duration(core::time::Duration::from_millis(1)),
    SmoltcpDuration::from_micros(0)
  );
}

#[test]
fn non_millisecond_duration_preserves_microseconds() {
  use smoltcp::time::Duration as SmoltcpDuration;
  // 1500 µs is not a whole number of milliseconds; the conversion must keep
  // microsecond granularity rather than truncating to `from_millis(1)`.
  assert_eq!(
    to_smoltcp_duration(core::time::Duration::from_micros(1_500)),
    SmoltcpDuration::from_micros(1_500)
  );
}

#[test]
fn sub_microsecond_duration_is_at_least_one_tick() {
  use smoltcp::time::Duration as SmoltcpDuration;
  // 1 ns truncates to 0 whole microseconds; the conversion must clamp up to
  // one tick rather than yielding smoltcp's abort-on-any-inactivity zero.
  let converted = to_smoltcp_duration(core::time::Duration::from_nanos(1));
  assert!(
    converted >= SmoltcpDuration::from_micros(1),
    "a sub-microsecond duration must clamp up to at least one tick, got {converted:?}"
  );
}

#[test]
fn out_of_range_duration_saturates_without_panicking() {
  use smoltcp::time::Duration as SmoltcpDuration;
  // A duration whose microseconds exceed u64 clamps to the ~100y cap instead
  // of overflowing smoltcp's own `From`, which multiplies whole seconds by
  // 1_000_000 -- and, unlike the pre-fix `u64::MAX` saturation, the cap stays
  // far enough below smoltcp's i64 microsecond range that adding it to an
  // `Instant` can never cast negative (see
  // `extreme_duration_does_not_install_a_past_deadline` below).
  assert_eq!(
    to_smoltcp_duration(core::time::Duration::MAX),
    SmoltcpDuration::from_micros(MAX_TIMEOUT_MICROS)
  );
}

#[test]
fn extreme_duration_does_not_install_a_past_deadline() {
  use smoltcp::time::{Duration as SmoltcpDuration, Instant as SmoltcpInstant};

  let base = SmoltcpInstant::from_millis(1_000_000i64);

  // Mutation anchor: the pre-fix conversion saturated straight to `u64::MAX`
  // microseconds. smoltcp's `Instant + Duration` computes `self.micros +
  // rhs.total_micros() as i64`, and `u64::MAX as i64 == -1`, so the old value
  // installed a deadline ONE MICROSECOND BEFORE `base` -- a past deadline that
  // aborts the socket immediately, the opposite of a "far future" timeout.
  // Confirm the unpatched primitive still reproduces that bug (guarding
  // against smoltcp changing this representation out from under the fix),
  // then assert the real conversion flips it.
  let old_buggy = SmoltcpDuration::from_micros(u64::MAX);
  assert!(
    base + old_buggy < base,
    "sanity check: the pre-fix saturation must reproduce a past deadline"
  );

  for extreme in [
    core::time::Duration::MAX,
    core::time::Duration::new(u64::MAX, 0),
  ] {
    let converted = to_smoltcp_duration(extreme);
    assert!(
      (converted.total_micros() as i64) > 0,
      "converted micros must stay representable as a positive i64, got {converted:?}"
    );
    let deadline = base + converted;
    assert!(
      deadline > base,
      "an extreme duration must install a FUTURE deadline, got {deadline:?} <= base {base:?}"
    );
  }
}
