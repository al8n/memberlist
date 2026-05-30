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
