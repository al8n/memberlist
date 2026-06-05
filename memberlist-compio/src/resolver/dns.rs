//! TCP-first DNS resolver — hickory-proto codec + compio TcpStream
//! transport. Mirrors Go memberlist's `tcpLookupIP` algorithm
//! (`hashicorp/memberlist/memberlist.go:308-417`).
//!
//! Why TCP-first: UDP DNS responses are capped at 512 bytes (without EDNS),
//! which can truncate the answer list for cluster-discovery hostnames
//! resolving to many A/AAAA records. TCP-DNS has no such cap, so it gives
//! the largest possible join set on a single query.

#![cfg(feature = "dns")]

use crate::{
  Address,
  resolver::{OsResolver, Resolver},
};
use compio::{
  buf::BufResult,
  io::{AsyncReadExt, AsyncWriteExt},
  net::TcpStream,
};
use futures_util::{FutureExt, pin_mut, select_biased};
use hickory_proto::{
  ProtoError,
  op::{Message, Query},
  rr::{Name, RData, RecordType},
  serialize::binary::{BinEncodable, BinEncoder, DecodeError},
};
use hostaddr::Host;
use std::{
  io::{self, Read},
  net::{IpAddr, SocketAddr},
  path::Path,
  time::Duration,
};

/// Default wall-clock upper bound on a single TCP-DNS query (connect +
/// write + read length-prefix + read response). Matches the default
/// DNS query timeout used by most stub resolvers (Go's `net.Resolver`
/// uses 5s, glibc's resolver uses 5s per attempt). Configured
/// per-resolver via [`DnsResolver::with_timeout`]; without a bound
/// the query inherits the kernel's TCP timeouts (~3 minutes connect,
/// infinite read), which would let a slow or hostile nameserver hang
/// the caller's `join_with` future indefinitely.
pub const DEFAULT_DNS_TIMEOUT: Duration = Duration::from_secs(5);

/// TCP-first DNS resolver — queries configured nameservers over TCP and
/// falls back to the OS resolver (which is UDP-first with TCP retry on
/// truncation) if TCP returns nothing.
///
/// Constructed from a resolv.conf-format file. For hostnames that lack a
/// `.` (short names, likely resolved via the host's search-domain list)
/// the TCP path is skipped entirely and the OS resolver is used directly,
/// matching the upstream behavior.
pub struct DnsResolver {
  servers: Vec<SocketAddr>,
  fallback: OsResolver,
  timeout: Duration,
}

/// Errors returned by [`DnsResolver`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DnsError {
  /// I/O error from the TCP transport or the OS-resolver fallback.
  #[error("I/O error: {0}")]
  Io(#[from] io::Error),

  /// hickory-proto encoding error (malformed query construction).
  #[error("DNS encode error: {0}")]
  Encode(#[from] ProtoError),

  /// hickory-proto decoding error (malformed response from the server).
  #[error("DNS decode error: {0}")]
  Decode(#[from] DecodeError),

  /// The hostname could not be parsed into a wire-format DNS name.
  #[error("hostname parse error: {0}")]
  Hostname(String),
}

impl From<DnsError> for io::Error {
  fn from(e: DnsError) -> Self {
    Self::other(e)
  }
}

impl DnsResolver {
  /// Construct from a resolv.conf-format file path. Reads the file, parses
  /// the nameserver list (each pinned to port 53), and stores the OS
  /// resolver as the fallback.
  pub fn from_resolv_conf(path: impl AsRef<Path>) -> Result<Self, io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    let cfg = resolv_conf::Config::parse(&buf)
      .map_err(|e| io::Error::other(format!("resolv.conf parse: {e}")))?;

    let servers: Vec<SocketAddr> = cfg
      .nameservers
      .iter()
      .map(|ns| SocketAddr::new(IpAddr::from(ns), 53))
      .collect();

    Ok(Self {
      servers,
      fallback: OsResolver,
      timeout: DEFAULT_DNS_TIMEOUT,
    })
  }

  /// Construct from an explicit nameserver list. The OS resolver is used
  /// as the fallback path.
  pub fn from_servers(servers: Vec<SocketAddr>) -> Self {
    Self {
      servers,
      fallback: OsResolver,
      timeout: DEFAULT_DNS_TIMEOUT,
    }
  }

  /// Builder: override the per-query timeout. Defaults to
  /// [`DEFAULT_DNS_TIMEOUT`].
  #[must_use]
  #[inline]
  pub const fn with_timeout(mut self, d: Duration) -> Self {
    self.timeout = d;
    self
  }

  /// The configured per-query timeout.
  #[inline]
  pub const fn timeout(&self) -> Duration {
    self.timeout
  }

  /// Send a single TCP-DNS query for TYPE ANY against the first
  /// configured nameserver and return the collected A + AAAA records.
  /// Returns an empty vec when no servers are configured.
  ///
  /// Bounded by `self.timeout` (default [`DEFAULT_DNS_TIMEOUT`]): a
  /// slow or hostile nameserver cannot hang the caller's `join_with`
  /// future beyond this wall-clock budget. On timeout returns
  /// [`DnsError::Io`]`(io::ErrorKind::TimedOut)`; the resolver's
  /// caller falls back to the OS resolver via the standard error
  /// path (see [`Resolver::resolve`] below).
  async fn tcp_query(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DnsError> {
    let query = self.tcp_query_inner(host, port).fuse();
    let timeout = compio::time::sleep(self.timeout).fuse();
    pin_mut!(query, timeout);
    select_biased! {
      res = query => res,
      _ = timeout => Err(DnsError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "TCP-DNS query exceeded the configured timeout",
      ))),
    }
  }

  /// Inner unbounded TCP-DNS query — invoked by [`Self::tcp_query`]
  /// inside the deadline select. Kept separate so the deadline wrapper
  /// owns the timer arm without complicating the protocol logic.
  async fn tcp_query_inner(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DnsError> {
    let Some(&server) = self.servers.first() else {
      return Ok(Vec::new());
    };

    // Build a TYPE ANY query message. `Message::query()` initializes a
    // fresh ID with the standard query flags; we add the question.
    let name = Name::from_ascii(host).map_err(|e| DnsError::Hostname(e.to_string()))?;
    let mut msg = Message::query();
    msg.add_query(Query::query(name, RecordType::ANY));

    // Encode to bytes via BinEncoder over an owned Vec.
    let mut payload: Vec<u8> = Vec::with_capacity(512);
    {
      let mut encoder = BinEncoder::new(&mut payload);
      msg.emit(&mut encoder)?;
    }

    // TCP-DNS (RFC 1035 §4.2.2) prepends a 2-byte big-endian length.
    let payload_len = u16::try_from(payload.len())
      .map_err(|_| DnsError::Io(io::Error::other("DNS query exceeds 65535 bytes")))?;
    let mut framed = Vec::with_capacity(2 + payload.len());
    framed.extend_from_slice(&payload_len.to_be_bytes());
    framed.extend_from_slice(&payload);

    // Connect and send the framed query. compio's IO is buffer-owning, so
    // we destructure BufResult and discard the returned buffer.
    let mut stream = TcpStream::connect(server).await?;
    let BufResult(write_res, _) = stream.write_all(framed).await;
    write_res?;

    // Read the 2-byte length prefix, then the body of exactly that length.
    let len_buf = vec![0u8; 2];
    let BufResult(read_res, len_buf) = stream.read_exact(len_buf).await;
    read_res?;
    let response_len = u16::from_be_bytes([len_buf[0], len_buf[1]]) as usize;

    let resp_buf = vec![0u8; response_len];
    let BufResult(read_res, resp_buf) = stream.read_exact(resp_buf).await;
    read_res?;

    // Decode and collect A + AAAA answers. CNAME and other RR types are
    // ignored to match the upstream behavior (see Go reference above).
    let response = Message::from_vec(&resp_buf)?;
    let mut addrs = Vec::new();
    for record in &response.answers {
      // `Record` exposes the rdata as a public field `data`; the
      // same-named accessor is shadowed when the field is the same name.
      match &record.data {
        RData::A(ipv4) => addrs.push(SocketAddr::new(IpAddr::V4(ipv4.0), port)),
        RData::AAAA(ipv6) => addrs.push(SocketAddr::new(IpAddr::V6(ipv6.0), port)),
        _ => {}
      }
    }
    Ok(addrs)
  }
}

impl Resolver for DnsResolver {
  type Address = Address;
  type Error = DnsError;

  async fn resolve(&self, addr: &Self::Address) -> Result<Vec<SocketAddr>, Self::Error> {
    let port = addr.port().unwrap_or(0);

    // IP literal: short-circuit, no DNS at all.
    if let Host::Ip(ip) = addr.host() {
      return Ok(vec![SocketAddr::new(*ip, port)]);
    }

    let host_str: &str = match addr.host() {
      Host::Domain(d) => d.as_ref(),
      Host::Ip(_) => unreachable!("handled above"),
    };

    // TCP-first only for names that look fully qualified (contain a `.`)
    // and only when we have at least one nameserver configured. Short
    // names will be resolved through the OS resolver's search-domain list.
    if host_str.contains('.') && !self.servers.is_empty() {
      // Ignoring Err: TCP-first is best-effort per the upstream spec
      // ("If this fails it's not fatal since this isn't a standard way to
      // query DNS, and we have a fallback below.", memberlist.go:404).
      // We unconditionally fall through to the OS resolver on any error
      // or empty answer.
      if let Ok(addrs) = self.tcp_query(host_str, port).await
        && !addrs.is_empty()
      {
        return Ok(addrs);
      }
    }

    self.fallback.resolve(addr).await.map_err(DnsError::Io)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Address;
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

    let host_err = DnsError::Hostname("bad name".to_string());
    assert_eq!(host_err.to_string(), "hostname parse error: bad name");
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
    let original = DnsError::Hostname("xyz".to_string());
    let io_err: io::Error = original.into();
    assert_eq!(io_err.kind(), io::ErrorKind::Other);
    assert!(io_err.to_string().contains("hostname parse error: xyz"));
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
}
