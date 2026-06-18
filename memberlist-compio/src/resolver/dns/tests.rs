use super::*;
use crate::Address;
use hickory_proto::rr::Name;
use hostaddr::HostAddr;
use std::{
  fs,
  net::{Ipv4Addr, Ipv6Addr},
  path::PathBuf,
  sync::atomic::{AtomicU64, Ordering},
};

/// A nameserver that is guaranteed never to answer: `240.0.0.1` is in the
/// reserved 240.0.0.0/4 block (RFC 1112 §4) and is unroutable. Pairing it
/// with a fast-path test proves the IP-literal branch never opens a socket
/// — a real connect would hang well past any test budget.
fn dead_server() -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1)), 53)
}

/// Build a resolver pointed at the unroutable nameserver with a sub-second
/// timeout, so any test that *did* hit the network would fail loudly
/// rather than silently fall through.
fn resolver_with_dead_server() -> DnsResolver {
  DnsResolver::from_servers(vec![dead_server()]).with_timeout(Duration::from_millis(50))
}

/// Allocate a unique temp path for a resolv.conf fixture. Avoids a
/// `tempfile` dev-dependency; the counter + PID keep parallel test
/// threads from colliding.
fn temp_resolv_conf_path(tag: &str) -> PathBuf {
  static COUNTER: AtomicU64 = AtomicU64::new(0);
  let n = COUNTER.fetch_add(1, Ordering::Relaxed);
  let mut p = std::env::temp_dir();
  p.push(format!(
    "memberlist-compio-dns-{tag}-{}-{n}.conf",
    std::process::id()
  ));
  p
}

#[test]
fn from_servers_uses_default_timeout() {
  let r = DnsResolver::from_servers(vec![dead_server()]);
  assert_eq!(r.timeout(), DEFAULT_DNS_TIMEOUT);
  assert_eq!(DEFAULT_DNS_TIMEOUT, Duration::from_secs(5));
}

#[test]
fn from_empty_servers_uses_default_timeout() {
  let r = DnsResolver::from_servers(Vec::new());
  assert_eq!(r.timeout(), DEFAULT_DNS_TIMEOUT);
}

#[test]
fn with_timeout_overrides_default() {
  let custom = Duration::from_millis(250);
  let r = DnsResolver::from_servers(vec![dead_server()]).with_timeout(custom);
  assert_eq!(r.timeout(), custom);

  // The builder is chainable and last-write-wins.
  let r = r.with_timeout(Duration::from_secs(30));
  assert_eq!(r.timeout(), Duration::from_secs(30));
}

#[test]
fn from_resolv_conf_parses_valid_file() {
  let path = temp_resolv_conf_path("valid");
  fs::write(
    &path,
    "# a comment\nnameserver 8.8.8.8\nnameserver 1.1.1.1\nsearch example.com\n",
  )
  .expect("write resolv.conf fixture");

  let r = DnsResolver::from_resolv_conf(&path).expect("parse valid resolv.conf");
  // No accessor exposes the parsed server list, but a successful parse
  // must still leave the default per-query timeout in place.
  assert_eq!(r.timeout(), DEFAULT_DNS_TIMEOUT);

  // Ignoring Err: best-effort cleanup of the temp fixture; a leaked file
  // in the OS temp dir is harmless and must not fail the test.
  let _ = fs::remove_file(&path);
}

#[test]
fn from_resolv_conf_parses_empty_file() {
  let path = temp_resolv_conf_path("empty");
  fs::write(&path, "").expect("write empty resolv.conf fixture");

  // An empty file is a valid resolv.conf with zero nameservers.
  let r = DnsResolver::from_resolv_conf(&path).expect("parse empty resolv.conf");
  assert_eq!(r.timeout(), DEFAULT_DNS_TIMEOUT);

  // Ignoring Err: best-effort temp-fixture cleanup.
  let _ = fs::remove_file(&path);
}

#[test]
fn from_resolv_conf_rejects_invalid_nameserver() {
  let path = temp_resolv_conf_path("invalid");
  // A `nameserver` line whose argument is not an IP makes
  // `resolv_conf::Config::parse` surface an `InvalidIp` error, which the
  // constructor maps to `io::Error::other`.
  fs::write(&path, "nameserver not-an-ip-address\n").expect("write bad resolv.conf fixture");

  // `DnsResolver` is not `Debug`, so map the `Ok` side to `()` before
  // `expect_err` can report it.
  let err = DnsResolver::from_resolv_conf(&path)
    .map(|_| ())
    .expect_err("malformed resolv.conf must fail");
  assert_eq!(err.kind(), io::ErrorKind::Other);
  assert!(
    err.to_string().contains("resolv.conf parse"),
    "error should carry the parse context, got: {err}"
  );

  // Ignoring Err: best-effort temp-fixture cleanup.
  let _ = fs::remove_file(&path);
}

#[test]
fn from_resolv_conf_missing_file_is_io_error() {
  let path = temp_resolv_conf_path("does-not-exist");
  // Guard against an accidentally pre-existing path.
  // Ignoring Err: removal is precautionary; absence is the desired state.
  let _ = fs::remove_file(&path);

  let err = DnsResolver::from_resolv_conf(&path)
    .map(|_| ())
    .expect_err("missing file must fail");
  assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[compio::test]
async fn resolve_ipv4_literal_skips_dns() {
  // An IPv4 literal must short-circuit before any socket is opened, even
  // though the resolver is pointed at an unroutable nameserver.
  let r = resolver_with_dead_server();
  let addr: Address = HostAddr::from_sock_addr("127.0.0.1:7946".parse().unwrap());
  let resolved = r.resolve(&addr).await.expect("IP literal resolves");
  assert_eq!(
    resolved,
    vec![SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
      7946
    )]
  );
}

#[compio::test]
async fn resolve_ipv6_literal_skips_dns() {
  let r = resolver_with_dead_server();
  let addr: Address = HostAddr::from_ip_addr(IpAddr::V6(Ipv6Addr::LOCALHOST)).with_port(443);
  let resolved = r.resolve(&addr).await.expect("IPv6 literal resolves");
  assert_eq!(
    resolved,
    vec![SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 443)]
  );
}

#[compio::test]
async fn resolve_ip_literal_without_port_uses_zero() {
  // `Address::port()` is `None` for a bare IP; the resolver substitutes 0.
  let r = resolver_with_dead_server();
  let addr: Address = HostAddr::from_ip_addr(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)));
  assert_eq!(addr.port(), None);
  let resolved = r
    .resolve(&addr)
    .await
    .expect("portless IP literal resolves");
  assert_eq!(
    resolved,
    vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 0)]
  );
}

#[compio::test]
async fn resolve_ip_literal_works_with_empty_server_list() {
  // The fast path does not consult `self.servers`, so an empty list is
  // irrelevant for IP literals.
  let r = DnsResolver::from_servers(Vec::new());
  let addr: Address = HostAddr::from_sock_addr("192.0.2.1:8080".parse().unwrap());
  let resolved = r.resolve(&addr).await.expect("IP literal resolves");
  assert_eq!(
    resolved,
    vec![SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
      8080
    )]
  );
}

#[compio::test]
async fn resolve_short_name_bypasses_tcp_and_uses_os_fallback() {
  // `localhost` has no `.`, so the TCP-first branch is skipped entirely
  // and resolution goes straight to the OS resolver. `localhost` maps to
  // loopback via the hosts file, so this stays off the external network
  // even though the configured nameserver is unroutable.
  let r = resolver_with_dead_server();
  let addr: Address = "localhost:9000".parse().expect("parse localhost:9000");
  // Confirm the test input is actually a short domain, not an IP literal.
  assert!(matches!(addr.host(), Host::Domain(_)));

  let resolved = r.resolve(&addr).await.expect("localhost resolves via OS");
  assert!(
    !resolved.is_empty(),
    "localhost must resolve to at least one address"
  );
  for sa in &resolved {
    assert_eq!(sa.port(), 9000);
    assert!(
      sa.ip().is_loopback(),
      "localhost must only map to loopback, got {sa}"
    );
  }
}

#[test]
fn dns_error_display_and_debug() {
  let io_err = DnsError::Io(io::Error::new(io::ErrorKind::TimedOut, "boom"));
  assert!(io_err.to_string().starts_with("I/O error:"));
  assert!(io_err.to_string().contains("boom"));
  assert!(!format!("{io_err:?}").is_empty());

  // A 64-octet single label exceeds the 63-octet DNS limit, yielding a real
  // hickory `ProtoError`.
  let proto_err = Name::from_ascii("x".repeat(64)).unwrap_err();
  let host_err = DnsError::Hostname(proto_err);
  assert!(host_err.to_string().starts_with("hostname parse error: "));
  assert!(format!("{host_err:?}").contains("Hostname"));
}

#[test]
fn dns_error_from_io_error() {
  // `#[from]` wires `io::Error` into the `Io` variant.
  let converted: DnsError = io::Error::new(io::ErrorKind::ConnectionRefused, "nope").into();
  assert!(matches!(converted, DnsError::Io(_)));
  assert_eq!(converted.to_string(), "I/O error: nope");
}

#[test]
fn dns_error_into_io_error_roundtrips() {
  // `From<DnsError> for io::Error` wraps via `io::Error::other`, so the
  // kind is `Other` and the source string is preserved.
  let original = DnsError::Hostname(Name::from_ascii("x".repeat(64)).unwrap_err());
  let io_err: io::Error = original.into();
  assert_eq!(io_err.kind(), io::ErrorKind::Other);
  assert!(io_err.to_string().starts_with("hostname parse error: "));
}

#[test]
fn dns_error_decode_variant_displays() {
  // Exercise the `Decode` variant's `Display` prefix by forcing a decode
  // failure from a too-short DNS message buffer.
  let decode_err = Message::from_vec(&[0x00]).expect_err("truncated DNS message must fail");
  let err = DnsError::from(decode_err);
  assert!(
    err.to_string().starts_with("DNS decode error:"),
    "got: {err}"
  );
  assert!(matches!(err, DnsError::Decode(_)));
}

/// A loopback TCP server that speaks just enough of the TCP-DNS wire protocol
/// (RFC 1035 §4.2.2) to answer ONE query: read the 2-byte length prefix +
/// query body, then write back a framed DNS response carrying the supplied
/// A and AAAA answers. Returns the bound address so the resolver can target
/// it. Stays entirely on loopback — no external DNS is contacted.
///
/// The response echoes no relationship to the query name; the resolver's
/// `tcp_query_inner` decodes the message and harvests A/AAAA answers without
/// validating the question, so a fixed answer set exercises the full
/// frame → connect → write → read-length → read-body → decode → collect path.
async fn spawn_tcp_dns_server(
  answers: Vec<RData>,
) -> (SocketAddr, compio::runtime::JoinHandle<()>) {
  use compio::{
    buf::BufResult,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
  };
  use hickory_proto::{
    op::{Message, OpCode},
    rr::{Name, Record},
  };

  let listener = TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind loopback DNS server");
  let addr = listener.local_addr().expect("server local_addr");

  let handle = compio::runtime::spawn(async move {
    let Ok((mut stream, _)) = listener.accept().await else {
      return;
    };

    // Read the 2-byte big-endian query length, then the query body.
    let len_buf = vec![0u8; 2];
    let BufResult(r, len_buf) = stream.read_exact(len_buf).await;
    if r.is_err() {
      return;
    }
    let qlen = u16::from_be_bytes([len_buf[0], len_buf[1]]) as usize;
    let qbuf = vec![0u8; qlen];
    let BufResult(r, _qbuf) = stream.read_exact(qbuf).await;
    if r.is_err() {
      return;
    }

    // Build a response message carrying the fixed answers. The id is
    // irrelevant to the resolver (it does not match the query id).
    let mut resp = Message::response(0, OpCode::Query);
    let name = Name::from_ascii("seed.cluster.test.").expect("answer name");
    for rdata in answers {
      resp.add_answer(Record::from_rdata(name.clone(), 60, rdata));
    }
    let body = resp.to_vec().expect("encode response");
    let mut framed = Vec::with_capacity(2 + body.len());
    framed.extend_from_slice(&(body.len() as u16).to_be_bytes());
    framed.extend_from_slice(&body);

    // Ignoring Err: best-effort single write into a test fixture; if the
    // client hung up the resolver test will surface the failure itself.
    let BufResult(_w, _b) = stream.write_all(framed).await;
  });

  (addr, handle)
}

// The TCP-first success path: a FQDN (contains a `.`) with a configured
// nameserver drives `tcp_query` → `tcp_query_inner` end to end against a
// loopback DNS server, and the harvested A + AAAA answers (carrying the
// query port) are returned without consulting the OS fallback.
#[compio::test]
async fn tcp_query_collects_a_and_aaaa_from_loopback_server() {
  use hickory_proto::rr::{
    RData,
    rdata::{A, AAAA},
  };

  let answers = vec![
    RData::A(A(Ipv4Addr::new(203, 0, 113, 7))),
    RData::AAAA(AAAA(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x1))),
    // A CNAME-like non-address record is ignored by the collector; model it
    // with a TXT to prove the `_ => {}` arm is taken.
    RData::TXT(hickory_proto::rr::rdata::TXT::new(vec![
      "ignored".to_string(),
    ])),
  ];
  let (server_addr, handle) = spawn_tcp_dns_server(answers).await;

  let r = DnsResolver::from_servers(vec![server_addr]).with_timeout(Duration::from_secs(5));
  // A fully-qualified name (has a `.`) so the TCP-first branch is taken.
  let addr: Address = "seed.cluster.test:8300".parse().expect("parse FQDN:port");
  assert!(matches!(addr.host(), Host::Domain(_)));

  let resolved = r.resolve(&addr).await.expect("TCP-DNS resolve");
  // Both address answers surface, each carrying the requested port; the TXT
  // is dropped.
  assert_eq!(
    resolved.len(),
    2,
    "only A + AAAA are collected: {resolved:?}"
  );
  assert!(resolved.contains(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
    8300
  )));
  assert!(resolved.contains(&SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x1)),
    8300
  )));

  handle.await.expect("DNS server task");
}

// When the TCP-DNS server returns an EMPTY answer set, the resolver treats
// the TCP attempt as unproductive and falls through to the OS fallback. With
// a short loopback name target the fallback resolves loopback, proving the
// empty-TCP-answer → fallback path (the `!addrs.is_empty()` guard going
// false) rather than surfacing the empty TCP result.
#[compio::test]
async fn tcp_query_empty_answers_falls_through_to_os_fallback() {
  // Server answers with zero address records.
  let (server_addr, handle) = spawn_tcp_dns_server(Vec::new()).await;

  let r = DnsResolver::from_servers(vec![server_addr]).with_timeout(Duration::from_secs(5));
  // `localhost.` is fully-qualified (trailing dot ⇒ contains `.`), so the TCP
  // branch is attempted; the empty answer makes it fall through to the OS
  // resolver, which maps localhost to loopback.
  let addr: Address = "localhost.:9100".parse().expect("parse localhost.:9100");
  assert!(matches!(addr.host(), Host::Domain(_)));

  let resolved = r
    .resolve(&addr)
    .await
    .expect("empty TCP answer falls back to OS resolver");
  assert!(
    !resolved.is_empty(),
    "OS fallback must resolve localhost to loopback"
  );
  for sa in &resolved {
    assert_eq!(sa.port(), 9100);
    assert!(
      sa.ip().is_loopback(),
      "localhost maps to loopback, got {sa}"
    );
  }

  handle.await.expect("DNS server task");
}
