use std::{
  net::{Ipv4Addr, SocketAddr, SocketAddrV4},
  sync::Arc,
  time::Duration,
};

use bytes::Bytes;
use futures_util::{future::BoxFuture, FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use showbiz_traits::Transport;
use showbiz_types::{MessageType, NodeStateType, SmolStr};

use crate::showbiz::Showbiz;

#[cfg(feature = "async")]
mod r#async;

#[cfg(feature = "sync")]
mod sync;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Assumed header overhead
const COMPOUND_HEADER_OVERHEAD: usize = 2;

/// Assumed overhead per entry in compound header
const COMPOUND_OVERHEAD: usize = 2;

const USER_MSG_OVERHEAD: usize = 1;

/// Warn if a UDP packet takes this long to process
const BLOCKING_WARNING: Duration = Duration::from_millis(10);

const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;
/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionAlgo {
  #[default]
  LZW = 0,
  None = 1,
}

impl CompressionAlgo {
  pub fn is_none(&self) -> bool {
    matches!(self, Self::None)
  }
}

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Ping {
  seq_no: u32,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect again an agent
  /// restart with a new name.
  node: SmolStr,

  /// Source address, used for a direct reply
  source_addr: SocketAddr,
  /// Source name, used for a direct reply
  source_node: SmolStr,
}

impl Ping {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.seq_no)
      .and_then(|_| rmp::encode::write_str(wr, &self.node))
      .and_then(|_| match self.source_addr {
        SocketAddr::V4(v) => {
          let ip = v.ip().octets();
          rmp::encode::write_bin(wr, &ip)
        }
        SocketAddr::V6(v) => {
          let ip = v.ip().octets();
          rmp::encode::write_bin(wr, &ip)
        }
      })
      .and_then(|_| rmp::encode::write_str(wr, &self.source_node))
  }
}

#[test]
fn test_ping_encode_decode() {
  let ping = Ping {
    seq_no: 42,
    node: "test".into(),
    source_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 12345)),
    source_node: "other".into(),
  };
  let mut vec = vec![];
  ping.encode(&mut vec).unwrap();
  let v1 = rmp_serde::encode::to_vec(&ping).unwrap();
  eprintln!("len: {} {:?}", vec.len(), vec);
  eprintln!("len: {} {:?}", v1.len(), v1);
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct IndirectPingRequest {
  seq_no: u32,
  target: SocketAddr,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect against an agent
  /// restart with a new name.
  node: SmolStr,

  /// true if we'd like a nack back
  nack: bool,

  /// Source address, used for a direct reply
  source_addr: SocketAddr,
  /// Source name, used for a direct reply
  source_node: SmolStr,
}

/// Ack response is sent for a ping
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct AckResponse {
  seq_no: u32,
  payload: bytes::Bytes,
}

impl AckResponse {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.seq_no).and_then(|_| rmp::encode::write_bin(wr, &self.payload))
  }
}

/// nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub(crate) struct NackResponse {
  seq_no: u32,
}

impl NackResponse {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.seq_no)
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub(crate) struct ErrorResponse {
  err: SmolStr,
}

impl ErrorResponse {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_str(wr, &self.err)
  }
}

/// suspect is broadcast when we suspect a node is dead
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Suspect {
  incarnation: u32,
  node: SmolStr,
  from: SmolStr,
}

impl Suspect {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.incarnation)
      .and_then(|_| rmp::encode::write_str(wr, &self.node))
      .and_then(|_| rmp::encode::write_str(wr, &self.from))
  }
}

/// Alive is broadcast when we know a node is alive.
/// Overloaded for nodes joining
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Alive {
  incarnation: u32,
  node: SmolStr,
  addr: SocketAddr,
  meta: Bytes,
  // The versions of the protocol/delegate that are being spoken, order:
  // pmin, pmax, pcur, dmin, dmax, dcur
  vsn: [u8; 6],
}

impl Alive {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.incarnation)
      .and_then(|_| rmp::encode::write_str(wr, &self.node))
      .and_then(|_| match self.addr {
        SocketAddr::V4(v) => {
          let ip = v.ip().octets();
          rmp::encode::write_bin(wr, &ip)
        }
        SocketAddr::V6(v) => {
          let ip = v.ip().octets();
          rmp::encode::write_bin(wr, &ip)
        }
      })
      .and_then(|_| rmp::encode::write_bin(wr, &self.meta))
      .and_then(|_| rmp::encode::write_bin(wr, &self.vsn))
  }
}

/// Dead is broadcast when we confirm a node is dead
/// Overloaded for nodes leaving
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Dead {
  incarnation: u32,
  node: SmolStr,
  from: SmolStr, // Include who is suspecting
}

impl Dead {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.incarnation)
      .and_then(|_| rmp::encode::write_str(wr, &self.node))
      .and_then(|_| rmp::encode::write_str(wr, &self.from))
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct PushPullHeader {
  nodes: usize,
  user_state_len: usize, // Encodes the byte lengh of user state
  join: bool,            // Is this a join request or a anti-entropy run
}

impl PushPullHeader {
  pub fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.nodes as u32)
      .and_then(|_| rmp::encode::write_u32(wr, self.user_state_len as u32))
      .and_then(|_| {
        rmp::encode::write_bool(wr, self.join)
          .map_err(rmp::encode::ValueWriteError::InvalidMarkerWrite)
      })
  }

  // pub fn decode<R: rmp::decode::>
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct UserMsgHeader {
  user_msg_len: usize, // Encodes the byte lengh of user state
}

impl UserMsgHeader {
  pub(crate) fn encode<W: rmp::encode::RmpWrite>(
    &self,
    wr: &mut W,
  ) -> Result<(), rmp::encode::ValueWriteError<W::Error>> {
    rmp::encode::write_u32(wr, self.user_msg_len as u32)
  }

  pub(crate) fn decode<R: rmp::decode::RmpRead>(
    rd: &mut R,
  ) -> Result<Self, rmp::decode::ValueReadError<R::Error>> {
    let user_msg_len = rmp::decode::read_u32(rd)? as usize;
    Ok(Self { user_msg_len })
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct PushNodeState {
  name: SmolStr,
  addr: SocketAddr,
  meta: Bytes,
  incarnation: u32,
  state: NodeStateType,
  vsn: [u8; 6],
}

#[viewit::viewit]
pub(crate) struct NodeState {
  join: bool,
  push_state: PushNodeState,
  user_state: Bytes,
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Compress {
  algo: CompressionAlgo,
  buf: Bytes,
}

// impl Showbiz {
//   fn stream_listen(&self) {}
// }
