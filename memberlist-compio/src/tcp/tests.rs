use super::*;
use crate::{FirstAddrResolver, MaybeResolved, OsResolver, SocketAddrResolver};
use std::net::SocketAddr;

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
      assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
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
      assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
      assert!(e.to_string().contains("advertise_addr"));
    }
    Err(other) => panic!("expected InvalidInput(advertise_addr), got {other:?}"),
    Ok(_) => panic!("a missing advertise_addr must be rejected, but construction succeeded"),
  }
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
