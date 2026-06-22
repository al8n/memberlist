use super::*;
use crate::{FirstAddrResolver, MaybeResolved, OsResolver, SocketAddrResolver};
use std::{io::ErrorKind, net::SocketAddr};

fn test_tcp_opts() -> TcpTransportOptions {
  let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
  TcpTransportOptions::new()
    .with_local_id(smol_str::SmolStr::new("test-node"))
    .with_advertise_addr(MaybeResolved::Resolved(bind))
}

#[compio::test]
async fn new_with_resolved_advertise_skips_resolver() {
  let opts = test_tcp_opts();
  let t: TcpTransport = TcpTransport::new(opts, &OsResolver, &FirstAddrResolver)
    .await
    .expect("construct TcpTransport");
  assert_eq!(t.local_id().as_str(), "test-node");
  assert!(t.local_address().is_resolved());
  let _: &SocketAddr = t.advertise_address();
}

/// `new` rejects a missing `local_id` with `InvalidInput` BEFORE any
/// resolution or socket bind — the field is required.
#[compio::test]
async fn new_without_local_id_errors() {
  let opts = TcpTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
    .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()));
  let res = TcpTransport::<smol_str::SmolStr, SocketAddr>::new(
    opts,
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("local_id"));
    }
    Err(other) => panic!("expected InvalidInput(local_id), got {other:?}"),
    Ok(_) => panic!("a missing local_id must be rejected, but construction succeeded"),
  }
}

/// `new` rejects a missing `advertise_addr` with `InvalidInput`.
#[compio::test]
async fn new_without_advertise_addr_errors() {
  let opts = TcpTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
    .with_local_id(smol_str::SmolStr::new("no-adv"));
  let res = TcpTransport::<smol_str::SmolStr, SocketAddr>::new(
    opts,
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("advertise_addr"));
    }
    Err(other) => panic!("expected InvalidInput(advertise_addr), got {other:?}"),
    Ok(_) => panic!("a missing advertise_addr must be rejected, but construction succeeded"),
  }
}

/// The `TcpTransportOptions` getters reflect what the builders set, and the
/// pre-build state (before `with_local_id` / `with_advertise_addr`) is `None`
/// — the `None` arms are the ones `new()`'s `ok_or_else` checks rely on.
#[test]
fn options_accessors_reflect_builders() {
  let empty = TcpTransportOptions::<SmolStr, SocketAddr>::new();
  assert!(empty.local_id().is_none());
  assert!(empty.advertise_addr().is_none());
  // The default stream knobs are present and valid out of the box.
  assert!(empty.stream().validate().is_ok());

  let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap();
  let custom = StreamTransportOptions::new();
  let opts = TcpTransportOptions::<SmolStr, SocketAddr>::new()
    .with_local_id(SmolStr::new("acc-node"))
    .with_advertise_addr(MaybeResolved::Resolved(addr))
    .with_stream(custom);
  assert_eq!(opts.local_id().map(|s| s.as_str()), Some("acc-node"));
  match opts.advertise_addr() {
    Some(MaybeResolved::Resolved(s)) => assert_eq!(*s, addr),
    other => panic!("expected a resolved advertise addr, got {other:?}"),
  }
  assert!(opts.stream().validate().is_ok());
}

/// `Default` is the `new()` all-`None` state — the same un-built options
/// `new()` rejects for a missing `local_id`.
#[test]
fn default_matches_new() {
  let d = TcpTransportOptions::<SmolStr, SocketAddr>::default();
  assert!(d.local_id().is_none());
  assert!(d.advertise_addr().is_none());
}

/// Labels larger than 253 bytes or non-UTF-8 must be rejected at the
/// MemberlistOptions setter — not at construction and not via a panic.
#[test]
fn with_label_rejects_invalid_at_setter() {
  use crate::{MemberlistError, MemberlistOptions};

  let too_long = vec![b'x'; 254];
  let result = MemberlistOptions::new().with_label(Some(too_long));
  assert!(
    matches!(result, Err(MemberlistError::InvalidLabel(_))),
    "a label exceeding 253 bytes must be rejected at the setter"
  );

  let non_utf8 = vec![0xff, 0xfe];
  let result = MemberlistOptions::new().with_label(Some(non_utf8));
  assert!(
    matches!(result, Err(MemberlistError::InvalidLabel(_))),
    "a non-UTF-8 label must be rejected at the setter"
  );
}
