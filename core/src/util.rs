use bytes::BufMut;
use prost::Message;

use super::CompressionAlgo;

pub(crate) fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
  let node_scale = ((n + 1) as f64).log10().ceil() as usize;
  retransmit_mult * node_scale
}

const LZW_LIT_WIDTH: u8 = 8;

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
  #[error("{0}")]
  LZW(#[from] weezl::LzwError),
}

#[inline]
pub(crate) fn decompress_buffer(
  cmp: CompressionAlgo,
  data: &[u8],
) -> Result<Vec<u8>, CompressionError> {
  match cmp {
    CompressionAlgo::LZW => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .decode(data)
      .map_err(CompressionError::LZW),
    CompressionAlgo::None => unreachable!(),
  }
}

#[inline]
pub(crate) fn compress_payload(
  cmp: CompressionAlgo,
  inp: &[u8],
) -> Result<Vec<u8>, CompressionError> {
  match cmp {
    CompressionAlgo::LZW => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .encode(inp)
      .map_err(Into::into),
    CompressionAlgo::None => unreachable!(),
  }
}

#[inline]
pub(crate) fn decompress_payload(
  cmp: CompressionAlgo,
  inp: &[u8],
) -> Result<Vec<u8>, CompressionError> {
  match cmp {
    CompressionAlgo::LZW => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .decode(inp)
      .map_err(Into::into),
    CompressionAlgo::None => unreachable!(),
  }
}

#[inline]
pub(crate) fn encode<B: BufMut, M: Message>(
  buf: &mut B,
  msg: &M,
) -> Result<(), prost::EncodeError> {
  msg.encode(buf)
}

#[inline]
pub(crate) fn decode<M: Message + Default>(buf: &[u8]) -> Result<M, prost::DecodeError> {
  M::decode(buf)
}

/// Returns the host:port form of an address, for use with a
/// transport.
#[inline]
fn join_host_port(host: &str, port: u16) -> String {
  // We assume that host is a literal IPv6 address if host has
  // colons.
  if host.find(':').is_some() {
    return format!("[{}]:{}", host, port);
  }
  format!("{}:{}", host, port)
}

/// Given a string of the form "host", "host:port", "ipv6::address",
/// or "\[ipv6::address\]:port", and returns true if the string includes a port.
#[inline]
pub(crate) fn has_port(s: &str) -> bool {
  // IPv6 address in brackets.
  if s.starts_with('[') {
    s.rfind(':') > s.rfind(']')
  } else {
    // Otherwise the presence of a single colon determines if there's a port
    // since IPv6 addresses outside of brackets (count > 1) can't have a
    // port.
    s.matches(':').count() == 1
  }
}

/// Makes sure the given string has a port number on it, otherwise it
/// appends the given port as a default.
#[inline]
pub(crate) fn ensure_port(s: &str, port: u16) -> String {
  if has_port(s) {
    s.to_string()
  } else {
    let s = s.trim_matches(|c| c == '[' || c == ']');
    join_host_port(s, port)
  }
}

#[derive(Debug)]
pub struct InvalidAddress {
  pub err: &'static str,
  pub addr: String,
}

impl core::fmt::Display for InvalidAddress {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}: {}", self.addr, self.err)
  }
}

impl std::error::Error for InvalidAddress {}

#[inline]
fn addr_err(addr: String, why: &'static str) -> Result<(String, u16), InvalidAddress> {
  Err(InvalidAddress { err: why, addr })
}

pub(crate) fn split_host_port(hostport: String) -> Result<(String, u16), InvalidAddress> {
  let missing_port = "missing port in address";
  let too_many_colons = "too many colons in address";
  let (mut j, mut k) = (0, 0);
  let mut host = String::new();

  let last_colon_index = match hostport.rfind(':') {
    Some(index) => index,
    None => return addr_err(hostport, missing_port),
  };

  if hostport.starts_with('[') {
    let end = match hostport.find(']') {
      Some(index) => index,
      None => return addr_err(hostport, "missing ']' in address"),
    };
    if end + 1 == hostport.len() {
      return addr_err(hostport, missing_port);
    }
    if end + 1 != last_colon_index {
      return if hostport.chars().nth(end + 1).unwrap() == ':' {
        addr_err(hostport, too_many_colons)
      } else {
        addr_err(hostport, missing_port)
      };
    }
    host = hostport[1..end].to_string();
    j = 1;
    k = end + 1;
  } else {
    host = hostport[..last_colon_index].to_string();
    if host.contains(':') {
      return addr_err(hostport, too_many_colons);
    }
  }

  if hostport[j..].contains('[') {
    return addr_err(hostport, "unexpected '[' in address");
  }

  if hostport[k..].contains(']') {
    return addr_err(hostport, "unexpected ']' in address");
  }

  match u16::from_str_radix(&hostport[last_colon_index + 1..], 10) {
    Ok(port) => Ok((host, port)),
    Err(_e) => Err(InvalidAddress {
      err: "invalid port",
      addr: hostport,
    }),
  }
}
